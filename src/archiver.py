"""Archiver (过渡版): 把 daily_archive 的 tar.gz 从 work_dir 搬到 archive_dir
(磁带库映射目录), 算 SHA256, 标记 archived。

注: 完整重构后, 此模块将被删除 (Commit 8), 由 packer 直接写 archive_dir。
此过渡版用于让现有 e2e 测试继续工作。
"""
from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from src.catalog import Catalog
from src.errors import ArchiveError


class Archiver:
    def __init__(self, archive_dir: str, catalog: Catalog) -> None:
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.catalog = catalog

    def archive_to_tape(self, daily_archive_id: int, tar_path: str) -> None:
        """过渡实现: 把 tar.gz 从 work_dir 复制到 archive_dir, 算 SHA256, 标 archived。"""
        da = self.catalog.get_daily_archive(daily_archive_id)
        if da is None:
            raise ArchiveError(f"daily_archive {daily_archive_id} 不存在")
        if da.status != "pending":
            raise ArchiveError(
                f"daily_archive {da.archive_date} 状态为 {da.status}, 必须 pending")

        src = Path(tar_path)
        if not src.exists():
            raise ArchiveError(f"tar 文件不存在: {tar_path}")

        dest = self.archive_dir / da.archive_filename
        try:
            shutil.copy2(src, dest)
        except Exception as e:
            raise ArchiveError(f"复制到 archive_dir 失败: {e}") from e

        # 计算 SHA256
        sha = hashlib.sha256(dest.read_bytes()).hexdigest()

        # 1. daily_archive → archived
        self.catalog.update_daily_archive_status(
            daily_archive_id, "archived", checksum_sha256=sha,
        )

        # 2. 所有关联 backup_objects → archived
        for bo in self.catalog.get_objects_by_daily_archive(daily_archive_id):
            self.catalog.update_backup_object_status(bo.id, "archived")

        self.catalog.log_operation(
            operation="archive", run_id=None,
            target=f"daily_archive:{daily_archive_id}",
            detail=f'{{"archive_file":"{da.archive_filename}","size":{dest.stat().st_size}}}',
        )
