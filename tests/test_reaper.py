"""Reaper 单元测试。
- 6 道门禁: daily_archive 状态 / 对象 archived / 校验和存在 / 顺序依赖 / ETag / metadata 跳过
- 5 个测试覆盖 happy path + 4 个门禁拒绝
"""
import datetime as dt
import uuid
from pathlib import Path
from src.obs_client import ObsClient
from src.catalog import Catalog
from src.models import Policy, DailyArchive, BackupObject
from src.reaper import Reaper


def _seed(tmp_path, *, with_full=True, with_diff=True, with_xlog=True):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "", "b1", True)
    cat.upsert_policy("i1", Policy(True, True, True, True))

    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="ncbs_busi_2026-06-09.tar.gz",
        status="archived", checksum_sha256="da-checksum",
    ))

    objects = []
    if with_full:
        bo = cat.upsert_backup_object(BackupObject(
            obs_key="i1/Db/1780160839955/f.rch", instance_id="i1",
            obs_last_modified=dt.datetime(2026, 6, 9, 0, 0, 0),
            backup_type="full", parent_backup_dir="1780160839955",
            backup_date="2026-06-09", backup_timestamp_ms=1780160839955,
            status="archived", checksum_sha256="obj-sha-1",
        ))
        cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_id)
        cat.update_backup_object_status(bo, "archived")
        cat._conn().execute(
            "UPDATE backup_objects SET checksum_sha256 = ? WHERE id = ?",
            ("obj-sha-1", bo),
        )
        objects.append(("i1/Db/1780160839955/f.rch", "e1"))
    if with_diff:
        bo = cat.upsert_backup_object(BackupObject(
            obs_key="i1/Difference/1780177759671/f.rch", instance_id="i1",
            obs_last_modified=dt.datetime(2026, 6, 9, 0, 0, 0),
            backup_type="diff", parent_backup_dir="1780177759671",
            backup_date="2026-06-09", backup_timestamp_ms=1780177759671,
            status="archived", checksum_sha256="obj-sha-2",
        ))
        cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_id)
        cat.update_backup_object_status(bo, "archived")
        cat._conn().execute(
            "UPDATE backup_objects SET checksum_sha256 = ? WHERE id = ?",
            ("obj-sha-2", bo),
        )
        objects.append(("i1/Difference/1780177759671/f.rch", "e2"))
    if with_xlog:
        bo = cat.upsert_backup_object(BackupObject(
            obs_key="i1/Log/cn_5001/pg_xlog/.../f.rch", instance_id="i1",
            obs_last_modified=dt.datetime(2026, 6, 9, 0, 0, 0),
            backup_type="xlog", parent_backup_dir="000000010000023B000000B0",
            backup_date="2026-06-09",
            status="archived", checksum_sha256="obj-sha-3",
        ))
        cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_id)
        cat.update_backup_object_status(bo, "archived")
        cat._conn().execute(
            "UPDATE backup_objects SET checksum_sha256 = ? WHERE id = ?",
            ("obj-sha-3", bo),
        )
        objects.append(("i1/Log/cn_5001/pg_xlog/.../f.rch", "e3"))

    obs = ObsClient.create_mock(initial_objects=[
        ("b1", k, 100, dt.datetime(2026, 6, 9, 0, 0, 0), et)
        for k, et in objects
    ])
    # 记录 obs_etag 给 catalog
    for k, et in objects:
        existing = cat.get_backup_object_by_key(k)
        cat._conn().execute(
            "UPDATE backup_objects SET obs_etag = ? WHERE id = ?", (et, existing.id)
        )
    return cat, obs, da_id


def test_reap_deletes_all_objects(tmp_path):
    cat, obs, da_id = _seed(tmp_path)
    r = Reaper(obs, cat)
    summary = r.reap_daily_archive(da_id)
    assert summary.deleted == 3
    assert summary.failed == []


def test_reap_rejects_non_archived(tmp_path):
    cat, obs, da_id = _seed(tmp_path)
    # pending 状态不能 reap
    cat.update_daily_archive_status(da_id, "pending")
    r = Reaper(obs, cat)
    from src.errors import UnsafeDeleteError
    import pytest
    with pytest.raises(UnsafeDeleteError, match="archived"):
        r.reap_daily_archive(da_id)


def test_reap_rejects_object_not_archived(tmp_path):
    cat, obs, da_id = _seed(tmp_path)
    # 把第一个对象状态改回 queued_for_archive (非 archived)
    objs = list(cat.get_objects_by_daily_archive(da_id))
    cat.update_backup_object_status(objs[0].id, "queued_for_archive")
    r = Reaper(obs, cat)
    from src.errors import UnsafeDeleteError
    import pytest
    with pytest.raises(UnsafeDeleteError, match="非 archived"):
        r.reap_daily_archive(da_id)


def test_reap_etag_mismatch_raises_hard_fail(tmp_path):
    """P0 修复: ETag mismatch 必须硬失败 (PITR 链断裂风险)。
    之前版本软失败 (跳过, 累计仍前进) → diff/xlog 可能在 full 未真正删除时继续推进。
    修复后: 任何 ETag 不一致 → raise UnsafeDeleteError, 整个 daily_archive 不被部分删除。
    """
    cat, obs, da_id = _seed(tmp_path)
    # 修改 catalog 中 full 对象的 ETag, 模拟外部改动
    cat._conn().execute(
        "UPDATE backup_objects SET obs_etag = 'changed-etag' WHERE obs_key LIKE 'i1/Db/%'",
    )
    r = Reaper(obs, cat)
    from src.errors import UnsafeDeleteError
    import pytest
    with pytest.raises(UnsafeDeleteError, match="ETag 不匹配"):
        r.reap_daily_archive(da_id)
    # 关键: 抛错时, diff/xlog 也不能被删除 (硬失败语义)
    objs_after = list(cat.get_objects_by_daily_archive(da_id))
    not_deleted = [o for o in objs_after if o.status != "obs_deleted"]
    assert len(not_deleted) == 3, (
        f"硬失败后必须全部保持原状, 实际已删 {3 - len(not_deleted)} 个"
    )


def test_reap_depends_on_full(tmp_path):
    """reap_diff_xlog_for_daily 在 full 未 reap 前拒绝。"""
    cat, obs, da_id = _seed(tmp_path, with_full=False, with_diff=True, with_xlog=True)
    # 此时 full 不在 daily_archive, 但 diff/xlog 在
    r = Reaper(obs, cat)
    from src.errors import UnsafeDeleteError
    import pytest
    with pytest.raises(UnsafeDeleteError, match="full"):
        r.reap_daily_archive(da_id, allow_uncovered_types=False)
