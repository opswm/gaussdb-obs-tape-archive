"""1.4.2.1 策略依赖约束校验。
- archive_diff=True 必须 archive_full=True
- archive_xlog=True 必须 archive_full=True 且 archive_diff=True
- archive_full / archive_snapshot 独立可开
"""
from __future__ import annotations

from src.errors import InvalidArchivePolicyError
from src.models import Policy


def validate_policies(policies: list[Policy]) -> None:
    """校验一组策略配置，全部通过或抛 InvalidArchivePolicyError。"""
    for idx, p in enumerate(policies):
        errors: list[str] = []
        if p.archive_diff and not p.archive_full:
            errors.append("archive_diff=True 必须 archive_full=True")
        if p.archive_xlog and not p.archive_full:
            errors.append("archive_xlog=True 必须 archive_full=True")
        if p.archive_xlog and not p.archive_diff:
            errors.append("archive_xlog=True 必须 archive_diff=True")
        if not 1 <= p.week_start_day <= 7:
            errors.append(
                f"week_start_day 必须是 1-7 (1=周一..7=周日), 得到 {p.week_start_day}"
            )
        if errors:
            label = (
                f"策略 #{idx} "
                f"(full={p.archive_full}, snapshot={p.archive_snapshot}, "
                f"diff={p.archive_diff}, xlog={p.archive_xlog}, "
                f"week_start_day={p.week_start_day})"
            )
            raise InvalidArchivePolicyError(f"{label} 违反依赖约束: " + "; ".join(errors))


def check_runtime_consistency(policy: Policy, actual_dirs: dict[str, int]) -> list[str]:
    """对比策略期望与 OBS 实际产物，产出告警列表。

    actual_dirs: 目录名 → 出现次数，例如 {"Db/": 5, "Log/": 1, "backup_metadata.cfg": 1}

    检测方向:
    1. 策略关闭但实际有 → 数据游离在归档之外
    2. 策略开启但实际缺失 → 备份链路停摆 (P0-3 反向检测)
    3. 实例级元数据缺失 → 实例配置异常
    """
    issues: list[str] = []

    # ── 方向 1: 策略关闭但实际有 ──
    if not policy.archive_full and actual_dirs.get("Db/", 0) > 0:
        issues.append(f"策略 archive_full=False 但 OBS 存在 {actual_dirs['Db/']} 个 Db/ 目录")
    if not policy.archive_snapshot and actual_dirs.get("Snapshot/", 0) > 0:
        issues.append(f"策略 archive_snapshot=False 但 OBS 存在 {actual_dirs['Snapshot/']} 个 Snapshot/ 目录")
    if not policy.archive_diff and actual_dirs.get("Difference/", 0) > 0:
        issues.append(f"策略 archive_diff=False 但 OBS 存在 {actual_dirs['Difference/']} 个 Difference/ 目录")
    if not policy.archive_xlog and actual_dirs.get("Log/", 0) > 0:
        issues.append(f"策略 archive_xlog=False 但 OBS 存在 {actual_dirs['Log/']} 个 Log/ 目录")

    # ── 方向 2: 策略开启但实际缺失 (P0-3) ──
    # 一旦策略要求归档, 但 OBS 上 0 个, 说明上游备份停摆, 归档系统不能
    # 静默接受空集, 必须告警以便运维介入。
    if policy.archive_full and actual_dirs.get("Db/", 0) == 0:
        issues.append(
            f"策略 archive_full=True 但 OBS 缺失 Db/ 目录 (实际: 0 个), "
            f"可能全量备份任务停摆"
        )
    if policy.archive_snapshot and actual_dirs.get("Snapshot/", 0) == 0:
        # Snapshot 缺失不一定是问题 (从未做过手动全备)
        # 只记 info 级, 不阻断; 但仍写入 issues 供运维 awareness
        issues.append(
            f"[info] 策略 archive_snapshot=True 但 OBS 缺失 Snapshot/ 目录 "
            f"(实际: 0 个), 如从未做过手动全备可忽略"
        )
    if policy.archive_diff and actual_dirs.get("Difference/", 0) == 0:
        issues.append(
            f"策略 archive_diff=True 但 OBS 缺失 Difference/ 目录 (实际: 0 个), "
            f"可能差量备份任务停摆"
        )
    if policy.archive_xlog and actual_dirs.get("Log/", 0) == 0:
        issues.append(
            f"策略 archive_xlog=True 但 OBS 缺失 Log/ 目录 (实际: 0 个), "
            f"可能 xlog 归档停摆, PITR 能力下降"
        )

    # ── 方向 3: 实例级元数据缺失 ──
    for required in ("backup_metadata.cfg", "incr_backup_metadata.cfg"):
        if actual_dirs.get(required, 0) == 0:
            issues.append(f"OBS 缺失实例级元数据 {required}，可能实例异常")

    return issues
