"""Packer: 周度/日度打包到 archive_dir。

过滤: metadata / archive_only 跳过; xlog 按时间窗严格取;
full/diff/snapshot 按目录名时间戳 (ts_ms) 落窗口。

行为:
- pack_weekly(instance_id, week_start, week_end, preview=False) → WeeklyArchiveResult
- pack_daily(instance_id, archive_date, preview=False) → WeeklyArchiveResult
- 实际打包: 下载 obs 对象 → staging → tar.gz (或直接目录) + metadata.json → archive_dir
- preview: 只输出计划清单到 stdout, 不下载不写盘不创建 daily_archive 行
- compress=False 时: 直接拷贝文件到目录, 不打包 tar.gz, SHA256 基于 metadata.json
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tarfile
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from src.compat import date_fromisoformat, datetime_fromisoformat

from src.catalog import Catalog
from src.errors import ArchiveError
from src.manifest import (
    build_daily_manifest, build_dir_entry, build_weekly_manifest,
    build_xlog_summary, render_daily_preview, render_preview, write_metadata,
)
from src.models import BackupObject, DailyArchive
from src.obs_client import ObsClient
from src.utils import (
    ensure_utc_aware, format_beijing_short,
    safe_rel_path, utc_to_beijing,
)
from src.week_boundary import (
    week_range_to_filenames, week_range_to_iso_strings,
)


@dataclass
class WeeklyArchiveResult:
    instance_id: str
    week_start: date
    week_end: date
    full_dirs: list[dict] = field(default_factory=list)
    diff_dirs: list[dict] = field(default_factory=list)
    snapshot_dirs: list[dict] = field(default_factory=list)
    xlog_obs: list[BackupObject] = field(default_factory=list)
    metadata_skipped: int = 0
    archive_filename: str | None = None
    archive_path: Path | None = None
    metadata_path: Path | None = None
    checksum_sha256: str | None = None
    preview_text: str | None = None
    preview: bool = False


class Packer:
    def __init__(
        self, obs: ObsClient, catalog: Catalog,
        work_dir: Path, archive_dir: Path,
        compression_level: int = 6,
        compress: bool = True,
    ) -> None:
        self.obs = obs
        self.catalog = catalog
        self.work_dir = Path(work_dir)
        self.archive_dir = Path(archive_dir)
        self.compression_level = max(0, min(9, compression_level))
        self.compress = compress
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def pack_weekly(
        self,
        instance_id: str,
        week_start: date,
        week_end: date,
        preview: bool = False,
    ) -> WeeklyArchiveResult:
        ins = self._get_instance(instance_id)
        policy = self.catalog.get_policy(instance_id)

        # 0. 幂等: (instance, archive_date=week_start) 已存在 → 复用
        if not preview:
            existing = self._find_existing(instance_id, week_start)
            if existing is not None:
                return self._result_from_existing(existing, instance_id,
                                                    week_start, week_end, ins)

        # 1. 过滤本周对象 (排除 metadata / archive_only; full/diff 按 ts_ms 窗口; xlog 按时间窗)
        all_week = list(self.catalog.list_backup_objects_weekly(
            instance_id,
            *week_range_to_iso_strings(week_start, week_end),
        ))
        full_objs = [b for b in all_week if b.backup_type == "full"]
        diff_objs = [b for b in all_week if b.backup_type == "diff"]
        snap_objs = [b for b in all_week if b.backup_type == "snapshot"]
        xlog_objs = [b for b in all_week if b.backup_type == "xlog"]

        # 2. 统计跳过的 metadata / archive_only (在 [week_start, week_end) 窗口内)
        ws_iso, we_iso = week_range_to_iso_strings(week_start, week_end)
        from src.utils import ensure_utc_aware as _eut
        ws_ms = int(_eut(datetime_fromisoformat(ws_iso)).timestamp() * 1000)
        we_ms = int(_eut(datetime_fromisoformat(we_iso)).timestamp() * 1000)
        meta_rows = list(self.catalog._conn().execute(
            """SELECT COUNT(*) AS n FROM backup_objects
               WHERE instance_id = ?
                 AND status != 'obs_deleted'
                 AND (restore_policy = 'archive_only' OR backup_type = 'metadata')
                 AND (
                   (backup_type IN ('full','diff','snapshot')
                    AND backup_timestamp_ms IS NOT NULL
                    AND backup_timestamp_ms >= ? AND backup_timestamp_ms < ?)
                   OR (backup_type IN ('xlog','metadata')
                    AND obs_last_modified >= ? AND obs_last_modified < ?)
                 )""",
            (instance_id, ws_ms, we_ms, ws_iso, we_iso),
        ).fetchall())
        metadata_skipped = int(meta_rows[0]["n"]) if meta_rows else 0

        if not all_week:
            raise ArchiveError(
                f"周 {week_start}~{week_end} ({instance_id}) 没有待打包对象"
            )

        dir_name, tar_name = week_range_to_filenames(
            ins["alias"], week_start, week_end,
        )

        # 3. 构造 manifest
        full_dir_entries = sorted(
            {b.parent_backup_dir: b.backup_timestamp_ms
             for b in full_objs if b.backup_timestamp_ms}.items()
        )
        diff_dir_entries = sorted(
            {b.parent_backup_dir: b.backup_timestamp_ms
             for b in diff_objs if b.backup_timestamp_ms}.items()
        )
        snap_dir_entries = sorted(
            {b.parent_backup_dir: b.backup_timestamp_ms
             for b in snap_objs if b.backup_timestamp_ms}.items()
        )
        full_dirs_meta = [build_dir_entry(name, ts) for name, ts in full_dir_entries]
        diff_dirs_meta = [build_dir_entry(name, ts) for name, ts in diff_dir_entries]
        snap_dirs_meta = [build_dir_entry(name, ts) for name, ts in snap_dir_entries]
        xlog_summary = build_xlog_summary(xlog_objs)
        totals = {
            "full_count": sum(1 for b in full_objs),
            "diff_count": sum(1 for b in diff_objs),
            "snapshot_count": sum(1 for b in snap_objs),
            "xlog_count": len(xlog_objs),
            "total_uncompressed_bytes": 0,  # 在打包时累加
            "compressed_tar_bytes": 0,
        }
        manifest = build_weekly_manifest(
            instance_alias=ins["alias"],
            instance_id=instance_id,
            display_name=ins["display_name"],
            bucket_name=ins["bucket_name"],
            week_start_day=policy.week_start_day,
            week_start=week_start,
            week_end=week_end,
            full_dirs=full_dirs_meta,
            diff_dirs=diff_dirs_meta,
            snapshot_dirs=snap_dirs_meta,
            xlog_summary=xlog_summary,
            metadata_skipped=metadata_skipped,
            totals=totals,
        )
        preview_text = render_preview(manifest)

        result = WeeklyArchiveResult(
            instance_id=instance_id,
            week_start=week_start,
            week_end=week_end,
            full_dirs=full_dirs_meta,
            diff_dirs=diff_dirs_meta,
            snapshot_dirs=snap_dirs_meta,
            xlog_obs=xlog_objs,
            metadata_skipped=metadata_skipped,
            archive_filename=tar_name if not preview else None,
            preview_text=preview_text,
            preview=preview,
        )

        if preview:
            return result

        # 4. staging: 下载所有对象
        staging = self.work_dir / dir_name
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        total_uncompressed = 0
        for bo in all_week:
            self._download_object(bo, staging)
            total_uncompressed += bo.obs_size_bytes
            sha = self._sha256_file(staging / bo.obs_key)
            self.catalog._conn().execute(
                "UPDATE backup_objects SET checksum_sha256 = ? WHERE id = ?",
                (sha, bo.id),
            )

        # 5. 写 manifest 到 staging
        manifest["totals"]["total_uncompressed_bytes"] = total_uncompressed
        staging_metadata = staging / "metadata.json"
        write_metadata(manifest, staging_metadata)

        # 6. tar.gz 打包
        tar_path = self.archive_dir / tar_name
        self._write_tar(staging, tar_path)

        # 7. 算 SHA256 + 更新 manifest
        archive_sha = self._sha256_file(tar_path)
        manifest["checksum_sha256"] = archive_sha
        manifest["totals"]["compressed_tar_bytes"] = tar_path.stat().st_size
        write_metadata(manifest, staging_metadata)

        # 8. daily_archive 行
        compressed_size = tar_path.stat().st_size
        da = DailyArchive(
            instance_id=instance_id,
            archive_date=week_start.isoformat(),
            archive_week_end=week_end.isoformat(),
            archive_type="weekly",
            archive_filename=tar_name,
            backup_count=len(all_week),
            total_size_bytes=total_uncompressed,
            compressed_size_bytes=compressed_size,
            full_count=sum(1 for b in full_objs),
            diff_count=sum(1 for b in diff_objs),
            snapshot_count=sum(1 for b in snap_objs),
            xlog_count=len(xlog_objs),
            metadata_skipped_count=metadata_skipped,
            full_dirs=json.dumps([d["dir_name"] for d in full_dirs_meta]),
            diff_dirs=json.dumps([d["dir_name"] for d in diff_dirs_meta]),
            snapshot_dirs=json.dumps([d["dir_name"] for d in snap_dirs_meta]),
            xlog_lsn_start=xlog_summary.get("lsn_start"),
            xlog_lsn_end=xlog_summary.get("lsn_end"),
            xlog_time_start=xlog_summary.get("last_modified_first_utc"),
            xlog_time_end=xlog_summary.get("last_modified_last_utc"),
            checksum_sha256=archive_sha,
            status="archived",
            archived_at=datetime.now().astimezone().isoformat(),
            manifest_json=json.dumps(manifest, ensure_ascii=False),
        )
        da_id = self.catalog.upsert_daily_archive(da)

        # 9. backup_objects → archived, 关联到 daily_archive
        for bo in all_week:
            bo.daily_archive_id = da_id
            self.catalog._conn().execute(
                """UPDATE backup_objects
                   SET daily_archive_id = ?, status = 'archived', updated_at = datetime('now')
                   WHERE id = ?""",
                (da_id, bo.id),
            )

        # 10. 清理 staging
        shutil.rmtree(staging)

        result.archive_filename = tar_name
        result.archive_path = tar_path
        result.metadata_path = staging_metadata
        result.checksum_sha256 = archive_sha
        return result

    def pack_daily(
        self,
        instance_id: str,
        archive_date: str,  # YYYY-MM-DD
        preview: bool = False,
    ) -> WeeklyArchiveResult:
        """日度归档: 将指定日期的对象打包到 archive_dir/{date}_{alias}/。"""
        ins = self._get_instance(instance_id)
        policy = self.catalog.get_policy(instance_id)

        # 0. 幂等
        if not preview:
            existing = self._find_existing(instance_id, date_fromisoformat(archive_date))
            if existing is not None:
                return self._result_from_existing(
                    existing, instance_id,
                    date_fromisoformat(archive_date),
                    date_fromisoformat(archive_date) + __import__('datetime').timedelta(days=1),
                    ins,
                )

        # 1. 过滤当天对象
        all_day = list(self.catalog.list_backup_objects_daily(instance_id, archive_date))
        full_objs = [b for b in all_day if b.backup_type == "full"]
        diff_objs = [b for b in all_day if b.backup_type == "diff"]
        snap_objs = [b for b in all_day if b.backup_type == "snapshot"]
        xlog_objs = [b for b in all_day if b.backup_type == "xlog"]

        # 2. metadata skipped count
        meta_rows = list(self.catalog._conn().execute(
            """SELECT COUNT(*) AS n FROM backup_objects
               WHERE instance_id = ?
                 AND status != 'obs_deleted'
                 AND (restore_policy = 'archive_only' OR backup_type = 'metadata')
                 AND backup_date = ?""",
            (instance_id, archive_date),
        ).fetchall())
        metadata_skipped = int(meta_rows[0]["n"]) if meta_rows else 0

        if not all_day:
            raise ArchiveError(
                f"日期 {archive_date} ({instance_id}) 没有待打包对象"
            )

        # 目录命名: {date}_{alias}
        date_compact = archive_date.replace("-", "")
        dir_name = f"{date_compact}_{ins['alias']}"

        # 3. 构造 manifest
        full_dir_entries = sorted(
            {b.parent_backup_dir: b.backup_timestamp_ms
             for b in full_objs if b.backup_timestamp_ms}.items()
        )
        diff_dir_entries = sorted(
            {b.parent_backup_dir: b.backup_timestamp_ms
             for b in diff_objs if b.backup_timestamp_ms}.items()
        )
        snap_dir_entries = sorted(
            {b.parent_backup_dir: b.backup_timestamp_ms
             for b in snap_objs if b.backup_timestamp_ms}.items()
        )
        full_dirs_meta = [build_dir_entry(name, ts) for name, ts in full_dir_entries]
        diff_dirs_meta = [build_dir_entry(name, ts) for name, ts in diff_dir_entries]
        snap_dirs_meta = [build_dir_entry(name, ts) for name, ts in snap_dir_entries]
        xlog_summary = build_xlog_summary(xlog_objs)
        totals = {
            "full_count": sum(1 for b in full_objs),
            "diff_count": sum(1 for b in diff_objs),
            "snapshot_count": sum(1 for b in snap_objs),
            "xlog_count": len(xlog_objs),
            "total_uncompressed_bytes": 0,
            "compressed_tar_bytes": 0,
        }
        manifest = build_daily_manifest(
            instance_alias=ins["alias"],
            instance_id=instance_id,
            display_name=ins["display_name"],
            bucket_name=ins["bucket_name"],
            archive_date=archive_date,
            full_dirs=full_dirs_meta,
            diff_dirs=diff_dirs_meta,
            snapshot_dirs=snap_dirs_meta,
            xlog_summary=xlog_summary,
            metadata_skipped=metadata_skipped,
            totals=totals,
        )
        preview_text = render_daily_preview(manifest)

        result = WeeklyArchiveResult(
            instance_id=instance_id,
            week_start=date_fromisoformat(archive_date),
            week_end=date_fromisoformat(archive_date) + __import__('datetime').timedelta(days=1),
            full_dirs=full_dirs_meta,
            diff_dirs=diff_dirs_meta,
            snapshot_dirs=snap_dirs_meta,
            xlog_obs=xlog_objs,
            metadata_skipped=metadata_skipped,
            archive_filename=dir_name if not preview else None,
            preview_text=preview_text,
            preview=preview,
        )

        if preview:
            return result

        # 4. staging: 下载所有对象
        staging = self.work_dir / dir_name
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        total_uncompressed = 0
        for bo in all_day:
            self._download_object(bo, staging)
            total_uncompressed += bo.obs_size_bytes
            sha = self._sha256_file(staging / safe_rel_path(bo.obs_key))
            self.catalog._conn().execute(
                "UPDATE backup_objects SET checksum_sha256 = ? WHERE id = ?",
                (sha, bo.id),
            )

        # 5. 写 manifest
        manifest["totals"]["total_uncompressed_bytes"] = total_uncompressed
        staging_metadata = staging / "metadata.json"
        write_metadata(manifest, staging_metadata)

        # 6. 归档到 archive_dir
        if self.compress:
            # tar.gz 打包
            tar_name = f"{dir_name}.tar.gz"
            tar_path = self.archive_dir / tar_name
            self._write_tar(staging, tar_path)
            archive_sha = self._sha256_file(tar_path)
            archive_filename = tar_name
            archive_path = tar_path
            compressed_size = tar_path.stat().st_size
            manifest["totals"]["compressed_tar_bytes"] = compressed_size
        else:
            # 不压缩: 直接拷贝整个 staging 目录到 archive_dir
            archive_path = self.archive_dir / dir_name
            if archive_path.exists():
                shutil.rmtree(archive_path)
            shutil.copytree(staging, archive_path)
            # SHA256: 基于 metadata.json
            archive_sha = self._sha256_file(archive_path / "metadata.json")
            archive_filename = dir_name
            compressed_size = total_uncompressed
        manifest["checksum_sha256"] = archive_sha
        write_metadata(manifest, archive_path / "metadata.json" if not self.compress else staging_metadata)

        # 7. daily_archive 行
        da = DailyArchive(
            instance_id=instance_id,
            archive_date=archive_date,
            archive_week_end=None,
            archive_type="daily",
            archive_filename=archive_filename,
            backup_count=len(all_day),
            total_size_bytes=total_uncompressed,
            compressed_size_bytes=compressed_size,
            full_count=sum(1 for b in full_objs),
            diff_count=sum(1 for b in diff_objs),
            snapshot_count=sum(1 for b in snap_objs),
            xlog_count=len(xlog_objs),
            metadata_skipped_count=metadata_skipped,
            full_dirs=json.dumps([d["dir_name"] for d in full_dirs_meta]),
            diff_dirs=json.dumps([d["dir_name"] for d in diff_dirs_meta]),
            snapshot_dirs=json.dumps([d["dir_name"] for d in snap_dirs_meta]),
            xlog_lsn_start=xlog_summary.get("lsn_start"),
            xlog_lsn_end=xlog_summary.get("lsn_end"),
            xlog_time_start=xlog_summary.get("last_modified_first_utc"),
            xlog_time_end=xlog_summary.get("last_modified_last_utc"),
            checksum_sha256=archive_sha,
            status="archived",
            archived_at=datetime.now().astimezone().isoformat(),
            manifest_json=json.dumps(manifest, ensure_ascii=False),
        )
        da_id = self.catalog.upsert_daily_archive(da)

        # 8. backup_objects → archived
        for bo in all_day:
            bo.daily_archive_id = da_id
            self.catalog._conn().execute(
                """UPDATE backup_objects
                   SET daily_archive_id = ?, status = 'archived', updated_at = datetime('now')
                   WHERE id = ?""",
                (da_id, bo.id),
            )

        # 9. 清理 staging
        shutil.rmtree(staging)

        result.archive_filename = archive_filename
        result.archive_path = archive_path
        result.metadata_path = archive_path / "metadata.json" if not self.compress else None
        result.checksum_sha256 = archive_sha
        return result

    # ─── helpers ───
    def _write_tar(self, staging: Path, tar_path: Path) -> None:
        """流式写 tar.gz 临时文件, 完成后 rename — 原子, 中途失败不毁旧 tar。"""
        target_dir = tar_path.parent
        fd, tmp_path = tempfile.mkstemp(
            dir=str(target_dir), prefix=".tmp.", suffix=".tar.gz",
        )
        try:
            with os.fdopen(fd, "wb") as f_out:
                with tarfile.open(fileobj=f_out, mode="w:gz",
                                  compresslevel=self.compression_level) as tf:
                    for p in sorted(staging.rglob("*")):
                        if p.is_file():
                            tf.add(p, arcname=str(p.relative_to(staging)))
            os.replace(tmp_path, tar_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _download_object(self, bo: BackupObject, staging: Path) -> None:
        # CWE-22: 拒绝 ../ 绝对路径 / NUL, 限定 target 在 staging 内
        rel = safe_rel_path(bo.obs_key)
        target = staging / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        # 二次防御: resolve 后仍须在 staging 内
        target_resolved = target.resolve()
        staging_resolved = staging.resolve()
        if not str(target_resolved).startswith(str(staging_resolved) + "/") \
                and target_resolved != staging_resolved:
            raise ArchiveError(
                f"obs_key 解析后跳出 staging 目录: {bo.obs_key!r} → {target_resolved}"
            )
        bucket = self._bucket_of(bo)
        with target.open("wb") as out:
            self.obs.get_object(bucket, bo.obs_key, out)

    def _bucket_of(self, bo: BackupObject) -> str:
        ins = self.catalog.get_instance_by_id(bo.instance_id)
        if ins is None:
            from src.errors import CatalogError
            raise CatalogError(f"未知 instance: {bo.instance_id}")
        return ins["bucket_name"]

    def _sha256_file(self, p: Path) -> str:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    def _get_instance(self, instance_id: str):
        for i in self.catalog.list_enabled_instances():
            if i["instance_id"] == instance_id:
                return i
        from src.errors import CatalogError
        raise CatalogError(f"未知 instance: {instance_id}")

    def _find_existing(
        self, instance_id: str, archive_date: date,
    ) -> DailyArchive | None:
        r = self.catalog._conn().execute(
            """SELECT * FROM daily_archives
               WHERE instance_id = ? AND archive_date = ?
               ORDER BY id DESC LIMIT 1""",
            (instance_id, archive_date.isoformat()),
        ).fetchone()
        return self.catalog._row_to_da(r) if r else None

    def _result_from_existing(
        self, da: DailyArchive, instance_id: str,
        week_start: date, week_end: date, ins: dict,
    ) -> WeeklyArchiveResult:
        """幂等命中: 直接返回已存在 weekly_archive 的元数据。"""
        manifest = json.loads(da.manifest_json) if da.manifest_json else None
        return WeeklyArchiveResult(
            instance_id=instance_id,
            week_start=week_start,
            week_end=week_end,
            full_dirs=(manifest or {}).get("contents", {}).get("full_dirs", []),
            diff_dirs=(manifest or {}).get("contents", {}).get("diff_dirs", []),
            snapshot_dirs=(manifest or {}).get("contents", {}).get("snapshot_dirs", []),
            archive_filename=da.archive_filename,
            archive_path=self.archive_dir / da.archive_filename,
            checksum_sha256=da.checksum_sha256,
            preview_text=f"已存在 weekly archive (id={da.id}, "
            f"date={da.archive_date}, status={da.status})",
        )

