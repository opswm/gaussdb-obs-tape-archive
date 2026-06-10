"""Packer: 按天打包 tar.gz + manifest.json + SHA256。"""
from __future__ import annotations

import io
import json
import tarfile
import hashlib
from datetime import datetime
from pathlib import Path

from src.catalog import Catalog
from src.errors import ArchiveError
from src.manifest import build_manifest, write_manifest
from src.models import BackupObject, DailyArchive, Policy
from src.obs_client import ObsClient


class Packer:
    def __init__(self, obs: ObsClient, catalog: Catalog, work_dir: Path) -> None:
        self.obs = obs
        self.catalog = catalog
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def pack_daily(self, instance_id: str, date: str) -> DailyArchive:
        ins = self._get_instance(instance_id)
        policy = self.catalog.get_policy(instance_id)

        # 0. 幂等检查: 若 (instance_id, archive_date) 已有 daily_archive, 直接返回
        existing = self._find_existing_daily_archive(instance_id, date)
        if existing is not None:
            return existing

        # 1. 取已 queued_for_archive 的对象
        objs = [bo for bo in self.catalog.list_backup_objects_by_status(
            "queued_for_archive", instance_id=instance_id)
            if bo.backup_date == date]
        if not objs:
            raise ArchiveError(f"{date} 没有待打包对象 (instance={instance_id})")

        # 2. 创建 daily_archive 记录 (pending)
        archive_filename = f"{ins['alias']}_{date}.tar.gz"
        da = DailyArchive(
            instance_id=instance_id, archive_date=date,
            archive_filename=archive_filename, status="pending",
            backup_count=len(objs),
        )
        da_id = self.catalog.upsert_daily_archive(da)

        # 3. 在 work_dir 下建临时目录, 按 obs_key 结构放置
        staging = self.work_dir / f"stage_{ins['alias']}_{date}"
        if staging.exists():
            for f in staging.rglob("*"):
                if f.is_file():
                    f.unlink()
            for d in sorted(staging.rglob("*"), reverse=True):
                if d.is_dir():
                    d.rmdir()
            staging.rmdir()
        staging.mkdir(parents=True)

        total_size = 0
        for bo in objs:
            self._download_object(bo, staging)
            obj_size = bo.obs_size_bytes
            total_size += obj_size
            sha = self._sha256_file(staging / bo.obs_key)
            self.catalog._conn().execute(
                "UPDATE backup_objects SET checksum_sha256 = ? WHERE id = ?",
                (sha, bo.id),
            )

        # 4. 关联 backup_objects → daily_archive
        for bo in objs:
            bo.id = bo.id  # type: ignore
            self.catalog.attach_object_to_daily_archive(bo, da_id)

        # 5. 生成 manifest
        contents, dir_tree = self._build_manifest_contents(objs, staging)
        manifest = build_manifest(
            instance_alias=ins["alias"],
            instance_display_name=ins["display_name"],
            instance_id=instance_id, archive_date=date,
            archive_filename=archive_filename,
            contents=contents, directory_tree=dir_tree, work_dir=self.work_dir,
        )
        manifest_path = self.work_dir / f"{ins['alias']}_{date}.manifest.json"
        write_manifest(manifest, manifest_path)

        # 6. tar.gz 打包
        tar_path = self.work_dir / archive_filename
        with tarfile.open(tar_path, "w:gz", compresslevel=6) as tf:
            for p in sorted(staging.rglob("*")):
                if p.is_file():
                    tf.add(p, arcname=str(p.relative_to(staging)))
            tf.add(manifest_path, arcname=f"manifest.json")

        compressed_size = tar_path.stat().st_size
        archive_sha = hashlib.sha256(tar_path.read_bytes()).hexdigest()

        # 7. 写回 daily_archives
        manifest_json = json.dumps(manifest, ensure_ascii=False)
        self.catalog._conn().execute(
            """UPDATE daily_archives
               SET backup_count = ?, total_size_bytes = ?, compressed_size_bytes = ?,
                   full_count = ?, diff_count = ?, snapshot_count = ?, xlog_count = ?,
                   full_dirs = ?, diff_dirs = ?, snapshot_dirs = ?,
                   xlog_lsn_start = ?, xlog_lsn_end = ?,
                   xlog_time_start = ?, xlog_time_end = ?,
                   checksum_sha256 = ?, manifest_json = ?
               WHERE id = ?""",
            (
                len(objs), total_size, compressed_size,
                contents["full_count"], contents["diff_count"],
                contents["snapshot_count"], contents["xlog_count"],
                json.dumps(contents["full_dirs"]),
                json.dumps(contents["diff_dirs"]),
                json.dumps(contents["snapshot_dirs"]),
                contents.get("xlog_lsn_start"), contents.get("xlog_lsn_end"),
                contents.get("xlog_time_start"), contents.get("xlog_time_end"),
                archive_sha, manifest_json, da_id,
            ),
        )
        # 8. 清理 staging
        for f in staging.rglob("*"):
            if f.is_file():
                f.unlink()
        for d in sorted(staging.rglob("*"), reverse=True):
            if d.is_dir():
                d.rmdir()
        staging.rmdir()

        return self.catalog.get_daily_archive(da_id)

    def _download_object(self, bo: BackupObject, staging: Path) -> None:
        target = staging / bo.obs_key
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as out:
            self.obs.get_object(bo.obs_key.split("/", 1)[0] if False else
                                self._bucket_of(bo), bo.obs_key, out)

    def _bucket_of(self, bo: BackupObject) -> str:
        ins = self.catalog.get_instance_by_alias(
            next(i["alias"] for i in self.catalog.list_enabled_instances()
                 if i["instance_id"] == bo.instance_id)
        )
        return ins["bucket_name"]

    def _sha256_file(self, p: Path) -> str:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    def _build_manifest_contents(
        self, objs: list[BackupObject], staging: Path,
    ) -> tuple[dict, list[str]]:
        full_dirs: list[str] = []
        diff_dirs: list[str] = []
        snapshot_dirs: list[str] = []
        full_files = diff_files = snapshot_files = xlog_files = meta_files = 0
        lsn_min: str | None = None
        lsn_max: str | None = None
        t_min: str | None = None
        t_max: str | None = None
        dir_tree: list[str] = []

        for bo in objs:
            dir_tree.append(bo.obs_key)
            if bo.backup_type == "full":
                full_files += 1
                if bo.parent_backup_dir not in full_dirs:
                    full_dirs.append(bo.parent_backup_dir)
            elif bo.backup_type == "diff":
                diff_files += 1
                if bo.parent_backup_dir not in diff_dirs:
                    diff_dirs.append(bo.parent_backup_dir)
            elif bo.backup_type == "snapshot":
                snapshot_files += 1
                if bo.parent_backup_dir not in snapshot_dirs:
                    snapshot_dirs.append(bo.parent_backup_dir)
            elif bo.backup_type == "xlog":
                xlog_files += 1
                if bo.parent_backup_dir:
                    lsn_min = bo.parent_backup_dir if lsn_min is None or bo.parent_backup_dir < lsn_min else lsn_min
                    lsn_max = bo.parent_backup_dir if lsn_max is None or bo.parent_backup_dir > lsn_max else lsn_max
                lm = bo.obs_last_modified.isoformat()
                t_min = lm if t_min is None or lm < t_min else t_min
                t_max = lm if t_max is None or lm > t_max else t_max
            else:
                meta_files += 1

        contents = {
            "full_count": full_files, "diff_count": diff_files,
            "snapshot_count": snapshot_files, "xlog_count": xlog_files,
            "metadata_count": meta_files,
            "full_dirs": full_dirs, "diff_dirs": diff_dirs, "snapshot_dirs": snapshot_dirs,
            "xlog_lsn_start": lsn_min, "xlog_lsn_end": lsn_max,
            "xlog_time_start": t_min, "xlog_time_end": t_max,
        }
        return contents, dir_tree

    def _get_instance(self, instance_id: str):
        for i in self.catalog.list_enabled_instances():
            if i["instance_id"] == instance_id:
                return i
        from src.errors import CatalogError
        raise CatalogError(f"未知 instance: {instance_id}")

    def _find_existing_daily_archive(
        self, instance_id: str, archive_date: str,
    ) -> DailyArchive | None:
        r = self.catalog._conn().execute(
            """SELECT * FROM daily_archives
               WHERE instance_id = ? AND archive_date = ?
               ORDER BY id DESC LIMIT 1""",
            (instance_id, archive_date),
        ).fetchone()
        return self.catalog._row_to_da(r) if r else None
