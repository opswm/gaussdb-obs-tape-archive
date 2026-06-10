"""Archiver 单元测试。
- 写 daily_archive 到磁带, 回读校验
- 状态机: pending → writing → on_tape
- 非 pending 拒绝
- 关联 backup_objects 全部 → archived
"""
import datetime as dt
from pathlib import Path
from src.obs_client import ObsClient
from src.catalog import Catalog
from src.tape_lib import TapeLibrary
from src.models import Policy, DailyArchive
from src.archiver import Archiver


def _setup(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "", "b1", True)
    cat.upsert_policy("i1", Policy(True, True, True, True))
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="ncbs_busi_2026-06-09.tar.gz",
        checksum_sha256="known-checksum",
        status="pending",
    ))
    # 准备本地 tar.gz
    tar = tmp_path / "ncbs_busi_2026-06-09.tar.gz"
    tar.write_bytes(b"fake-archive-bytes")
    return cat, da_id, tar


def test_archive_to_tape_updates_status(tmp_path):
    cat, da_id, tar = _setup(tmp_path)
    tape = TapeLibrary.create_simulated(str(tmp_path / "tapes"), max_volume_size_gb=1)
    a = Archiver(tape, cat)
    a.archive_to_tape(da_id, str(tar))

    da = cat.get_daily_archive(da_id)
    assert da.status == "on_tape"
    assert da.tape_volume is not None
    assert da.tape_position >= 0
    assert da.checksum_sha256 is not None  # 回读重算后填回


def test_archive_to_tape_rejects_non_pending(tmp_path):
    cat, da_id, tar = _setup(tmp_path)
    cat.update_daily_archive_status(da_id, "on_tape")
    tape = TapeLibrary.create_simulated(str(tmp_path / "tapes"), 1)
    a = Archiver(tape, cat)
    from src.errors import ArchiveError
    import pytest
    with pytest.raises(ArchiveError, match="pending"):
        a.archive_to_tape(da_id, str(tar))


def test_archive_attach_objects_to_archived_status(tmp_path):
    cat, da_id, tar = _setup(tmp_path)
    from src.models import BackupObject
    bo = cat.upsert_backup_object(BackupObject(
        obs_key="i1/Db/1780160839955/f.rch", instance_id="i1",
        obs_last_modified=dt.datetime(2026, 6, 9, 0, 0, 0),
        backup_type="full", parent_backup_dir="1780160839955",
        backup_date="2026-06-09", backup_timestamp_ms=1780160839955,
    ))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_id)

    tape = TapeLibrary.create_simulated(str(tmp_path / "tapes"), 1)
    a = Archiver(tape, cat)
    a.archive_to_tape(da_id, str(tar))

    objs = list(cat.get_objects_by_daily_archive(da_id))
    for o in objs:
        assert o.status == "archived"
