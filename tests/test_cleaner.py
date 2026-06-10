"""Cleaner: 严格按 restore_objects 清单逐对象清理 OBS 恢复数据。"""
from __future__ import annotations

import datetime as dt

import pytest

from src.catalog import Catalog
from src.cleaner import Cleaner
from src.errors import CleanupSafetyError
from src.models import BackupObject, DailyArchive
from src.obs_client import ObsClient


def _seed(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant123_inst456", "ncbs_busi", "n", "", "b1", True)

    # 真实 daily_archive (FK 需要)
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="tenant123_inst456", archive_date="2026-06-09",
        archive_filename="ncbs_busi_2026-06-09.tar.gz", status="on_tape",
        checksum_sha256="sha",
    ))

    sid = "s-clean-1"
    cat.create_restore_session(
        session_id=sid, target_time=dt.datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[da_id], required_full_dir="d1",
    )
    cat.update_restore_session_status(sid, "restored")
    rs = cat.get_restore_session(sid)

    for i, key in enumerate([
        "tenant123_inst456/Db/d1/a.rch",
        "tenant123_inst456/Difference/d2/b.rch",
    ]):
        cat.add_restore_object(
            restore_session_id=rs["id"],
            backup_object_id=None, daily_archive_id=da_id,
            bucket_name="b1", obs_key=key,
            object_size=10, restored_etag=f"etag-{i}",
            restored_last_modified="2026-06-10T10:00:00+00:00",
        )

    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "tenant123_inst456/Db/d1/a.rch", 10,
         dt.datetime(2026, 6, 10, 10, 0, 0), "etag-0"),
        ("b1", "tenant123_inst456/Difference/d2/b.rch", 10,
         dt.datetime(2026, 6, 10, 10, 0, 0), "etag-1"),
    ])

    for key, btype, parent in [
        ("tenant123_inst456/Db/d1/a.rch", "full", "d1"),
        ("tenant123_inst456/Difference/d2/b.rch", "diff", "d2"),
    ]:
        bo_id = cat.upsert_backup_object(BackupObject(
            obs_key=key, instance_id="tenant123_inst456",
            obs_last_modified=dt.datetime(2026, 6, 10, 0, 0, 0),
            backup_type=btype, parent_backup_dir=parent,
            backup_date="2026-06-09", backup_timestamp_ms=1780160839955,
        ))
        cat.mark_backup_object_obs_deleted(bo_id, run_id="r1")
        cat._conn().execute(
            "UPDATE restore_objects SET backup_object_id=? WHERE bucket_name='b1' AND obs_key=?",
            (bo_id, key),
        )

    return cat, obs, sid


def test_cleanup_deletes_listed_objects_only(tmp_path):
    cat, obs, sid = _seed(tmp_path)
    c = Cleaner(obs, cat)
    summary = c.cleanup(sid)
    assert summary.deleted == 2
    remaining = [o.key for o in obs.list_objects("b1", prefix="")]
    assert remaining == []


def test_cleanup_etag_mismatch_aborts(tmp_path):
    cat, obs, sid = _seed(tmp_path)
    obs._store[("b1", "tenant123_inst456/Db/d1/a.rch")] = (
        10, dt.datetime(2026, 6, 10, 10, 0, 0), "CHANGED", b"",
    )
    c = Cleaner(obs, cat)
    with pytest.raises(CleanupSafetyError, match="ETag"):
        c.cleanup(sid)


def test_cleanup_rejects_already_cleaned(tmp_path):
    cat, obs, sid = _seed(tmp_path)
    cat.update_restore_session_status(sid, "cleaned")
    c = Cleaner(obs, cat)
    with pytest.raises(CleanupSafetyError, match="已经清理过"):
        c.cleanup(sid)


def test_cleanup_rejects_no_objects(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant123_inst456", "ncbs_busi", "n", "", "b1", True)
    sid = "s-empty"
    cat.create_restore_session(
        session_id=sid, target_time=dt.datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[], required_full_dir="d1",
    )
    obs = ObsClient.create_mock()
    c = Cleaner(obs, cat)
    with pytest.raises(CleanupSafetyError, match="没有 restore_objects"):
        c.cleanup(sid)
