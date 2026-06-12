"""周度边界计算工具。

按 per-cluster `week_start_day` (1=周一..7=周日) 计算本周起止日。
返回 (本周 start_date, 下周 start_date) 闭开区间 [start, end)。
"""
from __future__ import annotations

from datetime import date, timedelta


def compute_week_range(
    today: date, week_start_day: int,
) -> tuple[date, date]:
    """返回 (week_start, week_end) — week_end 是排他下界。

    - week_start_day=1 → 周一 00:00 起点
    - week_start_day=7 → 周日 00:00 起点
    - week_start_day=6 → 周六 00:00 起点 (默认, 对齐 ncbs_busi)

    示例 (today=2026-06-11 周四, week_start_day=6):
        days_since = (3 - 5) % 7 = 5  # 周四回到上周六要退 5 天
        week_start = 2026-06-06 (周六)
        week_end   = 2026-06-13 (下周六, 排他)
    """
    if not 1 <= week_start_day <= 7:
        raise ValueError(
            f"week_start_day 必须是 1-7 (1=周一..7=周日), 得到 {week_start_day}"
        )
    # Python weekday: 0=周一..6=周日
    py_weekday = week_start_day - 1
    days_since = (today.weekday() - py_weekday) % 7
    week_start = today - timedelta(days=days_since)
    week_end = week_start + timedelta(days=7)
    return week_start, week_end


def week_range_to_iso_strings(
    week_start: date, week_end: date,
) -> tuple[str, str]:
    """转 UTC ISO 字符串 (含 tz 标记), 直接用于 SQL BETWEEN。

    week_start = 2026-05-30 → "2026-05-30T00:00:00+00:00"
    week_end   = 2026-06-06 → "2026-06-06T00:00:00+00:00"
    """
    start_iso = f"{week_start.isoformat()}T00:00:00+00:00"
    end_iso = f"{week_end.isoformat()}T00:00:00+00:00"
    return start_iso, end_iso


def week_range_to_filenames(
    alias: str, week_start: date, week_end: date,
) -> tuple[str, str]:
    """返回 (目录名, tar.gz 文件名) — 都不带扩展名前缀路径。

    目录示例: ncbs_busi_W20260530_20260606
    tar.gz : ncbs_busi_W20260530_20260606.tar.gz
    """
    start_str = week_start.strftime("%Y%m%d")
    end_str = week_end.strftime("%Y%m%d")
    dir_name = f"{alias}_W{start_str}_{end_str}"
    tar_name = f"{dir_name}.tar.gz"
    return dir_name, tar_name
