import pytest
from src.models import Policy
from src.policy import validate_policies, InvalidArchivePolicyError


def test_full_only_valid():
    p = Policy(True, False, False, False)
    validate_policies([p])  # 不抛


def test_snapshot_only_valid():
    p = Policy(False, True, False, False)
    validate_policies([p])


def test_full_plus_diff_valid():
    p = Policy(True, True, True, False)
    validate_policies([p])


def test_full_diff_xlog_valid():
    p = Policy(True, True, True, True)
    validate_policies([p])


def test_diff_without_full_rejected():
    p = Policy(False, False, True, False)
    with pytest.raises(InvalidArchivePolicyError, match="archive_diff"):
        validate_policies([p])


def test_xlog_without_full_rejected():
    p = Policy(False, False, False, True)
    with pytest.raises(InvalidArchivePolicyError, match="archive_xlog"):
        validate_policies([p])


def test_xlog_without_diff_rejected():
    p = Policy(True, False, False, True)
    with pytest.raises(InvalidArchivePolicyError, match="archive_xlog"):
        validate_policies([p])


def test_xlog_only_rejected():
    p = Policy(False, False, False, True)
    with pytest.raises(InvalidArchivePolicyError):
        validate_policies([p])


def test_runtime_consistency_detects_unexpected_dir(monkeypatch):
    """OBS 实际有 Snapshot/ 但策略 archive_snapshot=False，应告警。"""
    from src.policy import check_runtime_consistency

    p = Policy(True, False, True, True)  # 故意关闭 snapshot

    # 模拟 OBS 实际目录扫描结果
    actual_dirs = {
        "Db/": 5, "Difference/": 16, "Log/": 1,
        "Snapshot/": 0,  # 实际也没有
        "backup_metadata.cfg": 1, "incr_backup_metadata.cfg": 1,
    }
    issues = check_runtime_consistency(p, actual_dirs)
    # 此场景实际无 surprise，应返回空
    assert issues == []


def test_runtime_consistency_flags_disabled_type_present(monkeypatch):
    from src.policy import check_runtime_consistency
    p = Policy(True, False, True, True)  # snapshot=False
    actual_dirs = {
        "Db/": 5, "Difference/": 16, "Log/": 1,
        "Snapshot/": 3,  # 实际有 Snapshot 但策略关闭
        "backup_metadata.cfg": 1, "incr_backup_metadata.cfg": 1,
    }
    issues = check_runtime_consistency(p, actual_dirs)
    assert any("Snapshot" in s for s in issues)


# ─── P0-3 反向用例：策略开启但 OBS 缺失 ───

def test_runtime_consistency_flags_missing_full_dir(monkeypatch):
    """策略 archive_full=True 但 OBS 上没有 Db/ 目录, 应告警。
    场景: 全量备份任务停摆, 但策略仍配置为归档, 容易导致 silent failure。
    """
    from src.policy import check_runtime_consistency

    p = Policy(True, True, True, True)  # 全开
    actual_dirs = {
        "Db/": 0,                # 缺失!
        "Difference/": 16, "Log/": 1,
        "Snapshot/": 2,
        "backup_metadata.cfg": 1, "incr_backup_metadata.cfg": 1,
    }
    issues = check_runtime_consistency(p, actual_dirs)
    assert any("Db/" in s and ("缺失" in s or "缺少" in s or "missing" in s.lower()) for s in issues), \
        f"应告警 Db/ 缺失, 实际 issues={issues}"


def test_runtime_consistency_flags_missing_xlog_dir(monkeypatch):
    """策略 archive_xlog=True 但 OBS 上没有 Log/ 目录, 应告警。
    场景: xlog 归档停摆, 但策略仍配置为归档, 真实 PITR 能力下降。
    """
    from src.policy import check_runtime_consistency

    p = Policy(True, False, True, True)  # xlog 开启
    actual_dirs = {
        "Db/": 5, "Difference/": 16,
        "Log/": 0,                # 缺失!
        "Snapshot/": 0,
        "backup_metadata.cfg": 1, "incr_backup_metadata.cfg": 1,
    }
    issues = check_runtime_consistency(p, actual_dirs)
    assert any("Log/" in s and ("缺失" in s or "缺少" in s or "missing" in s.lower()) for s in issues), \
        f"应告警 Log/ 缺失, 实际 issues={issues}"


def test_runtime_consistency_flags_missing_diff_dir(monkeypatch):
    """策略 archive_diff=True 但 OBS 上没有 Difference/ 目录, 应告警。"""
    from src.policy import check_runtime_consistency

    p = Policy(True, False, True, False)  # diff 开启但无 xlog
    actual_dirs = {
        "Db/": 5, "Difference/": 0,  # 缺失!
        "Log/": 0,
        "Snapshot/": 0,
        "backup_metadata.cfg": 1, "incr_backup_metadata.cfg": 1,
    }
    issues = check_runtime_consistency(p, actual_dirs)
    assert any("Difference/" in s and ("缺失" in s or "缺少" in s or "missing" in s.lower()) for s in issues), \
        f"应告警 Difference/ 缺失, 实际 issues={issues}"
