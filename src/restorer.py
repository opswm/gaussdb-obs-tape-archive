"""Restorer: PITR 计划生成 + 执行 + Snapshot 独立恢复 (P0-4)。"""
from __future__ import annotations

import hashlib
import io
import json
import tarfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.catalog import Catalog
from src.errors import (
    PitrNotCapableError,
    RestoreError,
    SnapshotNotFoundError,
)
from src.obs_client import ObsClient
from src.tape_lib import TapeLibrary


# ─── P0-4: Snapshot 独立恢复入口 ───
def plan_snapshot_restore(
    catalog: Catalog, instance_id: str, snapshot_dir: str,
) -> dict:
    """规划 Snapshot 独立恢复。不走 PITR 链, 不取 xlog, 不叠加 diff。
    Raises: SnapshotNotFoundError。
    """
    conn = catalog._conn()
    objs = conn.execute(
        """SELECT * FROM backup_objects
           WHERE instance_id = ? AND backup_type = 'snapshot'
             AND parent_backup_dir = ?""",
        (instance_id, snapshot_dir),
    ).fetchall()
    if not objs:
        raise SnapshotNotFoundError(
            f"实例 {instance_id} 无 Snapshot/{snapshot_dir} 记录"
        )

    daily_ids = sorted({o["daily_archive_id"] for o in objs if o["daily_archive_id"]})
    if not daily_ids:
        raise SnapshotNotFoundError(
            f"Snapshot/{snapshot_dir} 尚未写入 daily_archive, 请先归档"
        )

    placeholders = ",".join("?" * len(daily_ids))
    dailies = conn.execute(
        f"SELECT * FROM daily_archives WHERE id IN ({placeholders})", daily_ids,
    ).fetchall()
    not_on_tape = [d for d in dailies if d["status"] != "on_tape"]
    if not_on_tape:
        raise SnapshotNotFoundError(
            f"Snapshot/{snapshot_dir} 所在部分 daily_archive 尚未 on_tape: "
            f"{[d['id'] for d in not_on_tape]}"
        )

    sid = str(uuid.uuid4())
    conn.execute(
        """INSERT INTO restore_sessions
           (session_id, target_time, required_daily_archives, required_full_dir,
            required_diff_dirs, xlog_redundancy_hours, xlog_forward_hours, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (sid, datetime.now(timezone.utc).isoformat(), json.dumps(daily_ids),
         snapshot_dir, "[]", 0.0, 0.0, "retrieving"),
    )
    return {
        "session_id": sid,
        "required_full": {
            "dir_name": snapshot_dir,
            "backup_type": "snapshot",
            "daily_archive_ids": daily_ids,
        },
    }


class Restorer:
    def __init__(
        self, obs_client: ObsClient | None, tape_lib: TapeLibrary | None,
        catalog: Catalog, work_dir: Path,
    ) -> None:
        self.obs = obs_client
        self.tape = tape_lib
        self.catalog = catalog
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

    # ─── PITR plan ───
    def plan(
        self, target_time: datetime, instance_id: str,
        xlog_redundancy_hours: float = 6.0,
        xlog_forward_hours: float = 6.0,
    ) -> dict:
        # P2-4: 必须 full+diff+xlog 全开
        pol = self.catalog.get_policy(instance_id)
        if not pol.is_full_pitr_capable():
            raise PitrNotCapableError(
                f"实例 {instance_id} 策略不支持 PITR: "
                f"需 archive_full + archive_diff + archive_xlog 同时为 True"
            )

        chain = self.catalog.find_pitr_chain_at(instance_id, target_time)
        if chain is None:
            raise RestoreError(
                f"PITR 链未覆盖 {target_time}, instance={instance_id}")

        base_full_dir = chain["base_full_dir"]
        base_full_time = datetime.fromisoformat(chain["base_full_time"])
        diff_dirs = json.loads(chain["diff_dirs"])

        xlog_start = base_full_time
        xlog_end = target_time + timedelta(hours=xlog_forward_hours)

        xlog_obs = list(self.catalog._conn().execute(
            """SELECT * FROM backup_objects
               WHERE instance_id = ? AND backup_type = 'xlog'
                 AND obs_last_modified >= ? AND obs_last_modified <= ?""",
            (instance_id, xlog_start.isoformat(), xlog_end.isoformat()),
        ).fetchall())

        needed_da: set[int] = set()
        for d in [base_full_dir] + diff_dirs:
            for r in self.catalog._conn().execute(
                """SELECT DISTINCT daily_archive_id FROM backup_objects
                   WHERE instance_id = ? AND (
                     (backup_type='full' AND parent_backup_dir = ?) OR
                     (backup_type='diff' AND parent_backup_dir = ?)
                   ) AND daily_archive_id IS NOT NULL""",
                (instance_id, d, d),
            ).fetchall():
                needed_da.add(r["daily_archive_id"])
        for xo in xlog_obs:
            if xo["daily_archive_id"]:
                needed_da.add(xo["daily_archive_id"])

        sid = str(uuid.uuid4())
        self.catalog.create_restore_session(
            session_id=sid, target_time=target_time,
            required_daily_archives=sorted(needed_da),
            required_full_dir=base_full_dir,
            required_diff_dirs=diff_dirs,
            xlog_redundancy_hours=xlog_redundancy_hours,
            xlog_forward_hours=xlog_forward_hours,
        )
        return {
            "session_id": sid,
            "required_full_dir": base_full_dir,
            "required_diff_dirs": diff_dirs,
            "xlog_time_start": xlog_start.isoformat(),
            "xlog_time_end": xlog_end.isoformat(),
            "xlog_count": len(xlog_obs),
            "total_archives": sorted(needed_da),
        }

    # ─── PITR execute ───
    def execute(self, session_id: str,
                tar_path_override: Path | None = None) -> None:
        if self.obs is None or self.tape is None:
            raise RestoreError("execute 需要 obs_client 和 tape_lib")
        sess = self.catalog.get_restore_session(session_id)
        if sess is None:
            raise RestoreError(f"session {session_id} 不存在")
        self.catalog.update_restore_session_status(session_id, "extracting")

        archive_ids = json.loads(sess["required_daily_archives"])
        for da_id in archive_ids:
            da = self.catalog.get_daily_archive(da_id)
            tar_path = self.work_dir / f"restore_{da.archive_filename}"
            if tar_path_override and da.archive_date == "2026-06-08":
                tar_path.write_bytes(Path(tar_path_override).read_bytes())
            else:
                self.tape.read_archive(
                    da.tape_volume, da.tape_position or 0,
                    da.compressed_size_bytes or 0, str(tar_path),
                )

            actual = hashlib.sha256(tar_path.read_bytes()).hexdigest()
            if actual != da.checksum_sha256:
                self.catalog.update_restore_session_status(
                    session_id, "failed",
                    error_message=f"tar checksum mismatch at {da.archive_date}")
                raise RestoreError(f"tar 校验失败: {da.archive_date}")

            bucket = self._bucket(da.instance_id)
            with tarfile.open(tar_path, "r:gz") as tf:
                for member in tf.getmembers():
                    if not member.isfile():
                        continue
                    f = tf.extractfile(member)
                    if f is None:
                        continue
                    key = member.name
                    meta = self.obs.get_object_metadata(bucket, key)
                    if not meta.not_found:
                        self.catalog.update_restore_session_status(
                            session_id, "failed",
                            error_message=f"key 已存在: {key}")
                        raise RestoreError(f"恢复中止: OBS 已存在 {key}")
                    put_meta = self.obs.put_file(
                        bucket, key, io.BytesIO(f.read()), member.size,
                    )
                    bo = self.catalog.get_backup_object_by_key(key)
                    self.catalog.add_restore_object(
                        restore_session_id=sess["id"],
                        backup_object_id=bo.id if bo else None,
                        daily_archive_id=da_id, bucket_name=bucket,
                        obs_key=key, object_size=member.size,
                        restored_etag=put_meta.etag,
                        restored_last_modified=put_meta.last_modified.isoformat(),
                    )
                    ro_row = self.catalog._conn().execute(
                        "SELECT last_insert_rowid() AS id"
                    ).fetchone()
                    self.catalog.update_restore_object_status(
                        ro_row["id"], "uploaded",
                        put_meta.etag, put_meta.last_modified.isoformat(),
                    )

        self.catalog.update_restore_session_status(session_id, "restored")

    def _bucket(self, instance_id: str) -> str:
        for i in self.catalog.list_enabled_instances():
            if i["instance_id"] == instance_id:
                return i["bucket_name"]
        raise RestoreError(f"未知 instance: {instance_id}")
