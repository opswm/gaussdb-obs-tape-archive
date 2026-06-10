"""Scanner 单元测试。
- 动态发现 Db/ Difference/ Snapshot/ Log/ 下的对象
- 按 parent_backup_dir 分类 full/diff/snapshot/xlog/metadata
- 严格应用 policy.archive_* 开关
- retention_days 用作 age 门槛
- 节点元数据归类 (obs_last_clean_record / cn_build_history 等)
- recovery_interval 整目录归类
"""
import datetime as dt
import io

import pytest

from src.catalog import Catalog
from src.models import Policy
from src.obs_client import ObsClient


def _bootstrap(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "core", "b1", True)
    # 默认 retention_days=0, 让测试用更短的时间窗口
    cat.upsert_policy("i1", Policy(True, True, True, True, retention_days=0))
    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "i1/Db/1780160839955/file_0.rch", 100, dt.datetime(2026, 6, 1, 1, 7, 0), "e1"),
        ("b1", "i1/Db/1780160839955/roach/x.json", 50, dt.datetime(2026, 6, 1, 1, 7, 0), "e2"),
        ("b1", "i1/Difference/1780177759671/file_0.rch", 80, dt.datetime(2026, 6, 1, 6, 7, 0), "e3"),
        ("b1", "i1/Log/cn_5001/pg_xlog/tl_00000001/00000010/000000010000023B000000B0/f.rch", 4,
         dt.datetime(2026, 6, 1, 0, 0, 1), "e4"),
        ("b1", "i1/Log/cn_5001/obs_last_clean_record", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e5"),
        ("b1", "i1/Log/recovery_interval/interval.json", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e6"),
        ("b1", "i1/backup_metadata.cfg", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e7"),
        ("b1", "i1/incr_backup_metadata.cfg", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e8"),
    ])
    return cat, obs


def test_scanner_finds_all_objects(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    n = s.scan_instance("i1", cat.get_policy("i1"))
    assert n == 8


def test_scanner_classifies_full_diff_xlog_metadata(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    s.scan_instance("i1", cat.get_policy("i1"))

    bos = {(b.obs_key, b.backup_type, b.restore_policy)
           for b in cat.list_backup_objects_by_status("discovered", "i1")}
    assert ("i1/Db/1780160839955/file_0.rch", "full", "normal") in bos
    assert ("i1/Difference/1780177759671/file_0.rch", "diff", "normal") in bos
    assert (
        "i1/Log/cn_5001/pg_xlog/tl_00000001/00000010/000000010000023B000000B0/f.rch",
        "xlog",
        "normal",
    ) in bos
    assert ("i1/Log/cn_5001/obs_last_clean_record", "metadata", "archive_only") in bos
    assert ("i1/Log/recovery_interval/interval.json", "metadata", "archive_only") in bos
    assert ("i1/backup_metadata.cfg", "metadata", "archive_only") in bos


def test_scanner_respects_policy_off(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    # 关闭 archive_diff
    cat.upsert_policy("i1", Policy(True, True, False, True, retention_days=0))
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    n = s.scan_instance("i1", cat.get_policy("i1"))

    keys = [b.obs_key for b in cat.list_backup_objects_by_status("discovered", "i1")]
    assert not any("Difference/" in k for k in keys)
    assert n == 7  # 8 - 1 diff 对象


def test_scanner_idempotent(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    s.scan_instance("i1", cat.get_policy("i1"))
    s.scan_instance("i1", cat.get_policy("i1"))
    n = sum(1 for _ in cat.list_backup_objects_by_status("discovered", "i1"))
    assert n == 8


def test_scanner_dynamic_log_node_discovery(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    # 增加额外节点
    obs.put_file("b1", "i1/Log/cn_9999/obs_last_clean_record",
                 io.BytesIO(b"x"), 1)
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    s.scan_instance("i1", cat.get_policy("i1"))
    keys = [b.obs_key for b in cat.list_backup_objects_by_status("discovered", "i1")]
    assert any("Log/cn_9999/" in k for k in keys)


# ─── P0-5: retention_days 强制应用 ───
def test_scanner_uses_policy_retention_days(tmp_path):
    """P0-5: Scanner 必须用 policy.retention_days, 不能默认 0。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "core", "b1", True)
    # 老对象 (200 天前) + 短 retention (90 天)
    old_date = dt.datetime(2026, 6, 1, 0, 0, 0) - dt.timedelta(days=200)
    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "i1/Db/1780160839955/file_0.rch", 100, old_date, "e_old"),
    ])
    cat.upsert_policy("i1", Policy(True, True, True, True, retention_days=90))
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    n = s.scan_instance("i1", cat.get_policy("i1"))
    # 老对象 (200 天前) 在 retention=90 时被过滤掉
    assert n == 0

    # 改 retention=0, 应该扫到
    cat.upsert_policy("i1", Policy(True, True, True, True, retention_days=0))
    n2 = s.scan_instance("i1", cat.get_policy("i1"))
    assert n2 == 1


# ─── P1-2: 节点元数据分类 ───
def test_scanner_classifies_log_node_metadata(tmp_path):
    """P1-2: obs_last_clean_record / cn_build_history 等归类为 metadata+archive_only。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "core", "b1", True)
    cat.upsert_policy("i1", Policy(True, True, True, True, retention_days=0))
    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "i1/Log/cn_5001/obs_last_clean_record", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e1"),
        ("b1", "i1/Log/cn_5001/obs_archive_start_end_record", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e2"),
        ("b1", "i1/Log/cn_5001/cn_build_history", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e3"),
        ("b1", "i1/Log/dn1/obs_last_clean_record", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e4"),
        ("b1", "i1/Log/dn1/dn_build_history", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e5"),
    ])
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    s.scan_instance("i1", cat.get_policy("i1"))

    expected_keys = {
        "i1/Log/cn_5001/obs_last_clean_record",
        "i1/Log/cn_5001/obs_archive_start_end_record",
        "i1/Log/cn_5001/cn_build_history",
        "i1/Log/dn1/obs_last_clean_record",
        "i1/Log/dn1/dn_build_history",
    }
    rows = list(cat.list_backup_objects_by_status("discovered", "i1"))
    classified = {b.obs_key: (b.backup_type, b.restore_policy) for b in rows
                  if b.obs_key in expected_keys}
    assert len(classified) == 5
    for key in expected_keys:
        assert classified[key] == ("metadata", "archive_only"), f"{key} -> {classified[key]}"


def test_scanner_classifies_recovery_interval(tmp_path):
    """P1-2: Log/recovery_interval/ 全部归类为 metadata+archive_only。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "core", "b1", True)
    cat.upsert_policy("i1", Policy(True, True, True, True, retention_days=0))
    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "i1/Log/recovery_interval/interval.json", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e1"),
        ("b1", "i1/Log/recovery_interval/other.json", 1, dt.datetime(2026, 6, 1, 0, 0, 1), "e2"),
    ])
    from src.scanner import Scanner
    s = Scanner(obs, cat)
    s.scan_instance("i1", cat.get_policy("i1"))

    rows = list(cat.list_backup_objects_by_status("discovered", "i1"))
    classified = {b.obs_key: (b.backup_type, b.restore_policy) for b in rows}
    assert classified["i1/Log/recovery_interval/interval.json"] == ("metadata", "archive_only")
    assert classified["i1/Log/recovery_interval/other.json"] == ("metadata", "archive_only")
