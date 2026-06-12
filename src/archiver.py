"""Archiver: 写 daily_archive 到磁带, 写后回读校验。"""
from __future__ import annotations

import hashlib
from pathlib import Path

from src.catalog import Catalog
from src.errors import ArchiveError
from src.tape_lib import TapeLibrary


class Archiver:
    def __init__(self, tape_lib: TapeLibrary, catalog: Catalog) -> None:
        self.tape_lib = tape_lib
        self.catalog = catalog

    def archive_to_tape(self, daily_archive_id: int, tar_path: str) -> None:
        da = self.catalog.get_daily_archive(daily_archive_id)
        if da is None:
            raise ArchiveError(f"daily_archive {daily_archive_id} 不存在")
        if da.status != "pending":
            raise ArchiveError(
                f"daily_archive {da.archive_date} 状态为 {da.status}, 必须 pending")

        # 1. writing
        self.catalog.update_daily_archive_status(daily_archive_id, "writing")

        # 2. 写入磁带
        try:
            result = self.tape_lib.write_archive(tar_path, archive_id=daily_archive_id)
        except Exception as e:
            self.catalog.update_daily_archive_status(daily_archive_id, "pending")
            raise ArchiveError(f"磁带写入失败: {e}") from e

        # 3. 校验 (verify_checksum 由 tape_lib 在 write_archive 内做回读并返回)
        if not result.verify_checksum:
            raise ArchiveError("磁带回读校验未通过 (verify_checksum 为空)")

        # 4. on_tape
        self.catalog.update_daily_archive_status(
            daily_archive_id, "on_tape",
            tape_volume=result.tape_volume, tape_position=result.tape_position,
            checksum_sha256=result.verify_checksum,
        )

        # 5. 所有关联 backup_objects → archived
        for bo in self.catalog.get_objects_by_daily_archive(daily_archive_id):
            self.catalog.update_backup_object_status(bo.id, "archived")
            self.catalog.set_backup_object_tape(
                bo.id, result.tape_volume, result.tape_position,
                bo.checksum_sha256 or "",
            )

        self.catalog.log_operation(
            operation="archive", run_id=None,
            target=f"daily_archive:{daily_archive_id}",
            detail=f'{{"tape_volume":"{result.tape_volume}","written_size":{result.written_size}}}',
        )
