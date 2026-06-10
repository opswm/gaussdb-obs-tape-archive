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
        if errors:
            label = f"策略 #{idx} (full={p.archive_full}, snapshot={p.archive_snapshot}, "\
                    f"diff={p.archive_diff}, xlog={p.archive_xlog})"
            raise InvalidArchivePolicyError(f"{label} 违反依赖约束: " + "; ".join(errors))
