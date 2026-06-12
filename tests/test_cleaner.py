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
    cat.upsert_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "ncbs_busi", "n", "", "b1", True)

    # 真实 daily_archive (FK 需要)
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", archive_date="2026-06-09",
        archive_filename="ncbs_busi_2026-06-09.tar.gz", status="archived",
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
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/d1/a.rch",
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Difference/d2/b.rch",
    ]):
        cat.add_restore_object(
            restore_session_id=rs["id"],
            backup_object_id=None, daily_archive_id=da_id,
            bucket_name="b1", obs_key=key,
            object_size=10, restored_etag=f"etag-{i}",
            restored_last_modified="2026-06-10T10:00:00+00:00",
        )

    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/d1/a.rch", 10,
         dt.datetime(2026, 6, 10, 10, 0, 0), "etag-0"),
        ("b1", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Difference/d2/b.rch", 10,
         dt.datetime(2026, 6, 10, 10, 0, 0), "etag-1"),
    ])

    for key, btype, parent in [
        ("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/d1/a.rch", "full", "d1"),
        ("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Difference/d2/b.rch", "diff", "d2"),
    ]:
        bo_id = cat.upsert_backup_object(BackupObject(
            obs_key=key, instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
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


def test_cleanup_etag_mismatch_records_failed_not_raises(tmp_path):
    """P0 修复: ETag mismatch 改为记 failed + 跳过, 不再 raise 中断整 loop。
    之前 raise 会让 session 永远卡 'cleaning'。
    修复后: mismatch 对象记 failed, session 终态 'failed' (运维介入)。
    """
    cat, obs, sid = _seed(tmp_path)
    obs._store[("b1", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/d1/a.rch")] = (
        10, dt.datetime(2026, 6, 10, 10, 0, 0), "CHANGED", b"",
    )
    c = Cleaner(obs, cat)
    summary = c.cleanup(sid)
    # 1 个对象 ETag 不匹配 → 记 failed, 不抛错
    assert len(summary.failed) >= 1
    assert any("ETag 已变化" in reason for _, reason in summary.failed)
    # session 终态是 'failed' (有 failed), 不是 'cleaned' (卡 cleaning)
    sess = cat.get_restore_session(sid)
    assert sess["status"] == "failed", f"应有 failed 对象 → status=failed, 实际 {sess['status']}"


def test_cleanup_rejects_already_cleaned(tmp_path):
    cat, obs, sid = _seed(tmp_path)
    cat.update_restore_session_status(sid, "cleaned")
    c = Cleaner(obs, cat)
    with pytest.raises(CleanupSafetyError, match="已经清理过"):
        c.cleanup(sid)


def test_cleanup_rejects_no_objects(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "ncbs_busi", "n", "", "b1", True)
    sid = "s-empty"
    cat.create_restore_session(
        session_id=sid, target_time=dt.datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[], required_full_dir="d1",
    )
    obs = ObsClient.create_mock()
    c = Cleaner(obs, cat)
    with pytest.raises(CleanupSafetyError, match="没有 restore_objects"):
        c.cleanup(sid)


def test_cleanup_failed_with_no_objects_marks_cleaned(tmp_path):
    """P1 修复: session='failed' + restore_objects=空 → 允许 mark cleaned。
    场景: execute 在 add_restore_object 之前 abort (e.g. "key 已存在"),
    没产生 OBS 数据可清, 也不能让 session 永远卡 'failed'。
    """
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "ncbs_busi", "n", "", "b1", True)
    sid = "s-failed-empty"
    cat.create_restore_session(
        session_id=sid, target_time=dt.datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[], required_full_dir="d1",
    )
    # 模拟 execute abort 后状态
    cat.update_restore_session_status(sid, "failed", error_message="key already exists")
    obs = ObsClient.create_mock()
    c = Cleaner(obs, cat)
    summary = c.cleanup(sid)
    assert summary.deleted == 0
    assert summary.failed == []
    sess = cat.get_restore_session(sid)
    assert sess["status"] == "cleaned", f"应 cleaned, 实际 {sess['status']}"
    assert "no restore_objects" in sess["error_message"]
