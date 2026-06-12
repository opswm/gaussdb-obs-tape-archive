"""通用工具: 时区转换、Beijing 时间显示等。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

# Beijing = UTC+8 (无夏令时)
BEIJING_TZ = timezone(timedelta(hours=8), name="Beijing")


def utc_to_beijing(dt: datetime) -> datetime:
    """UTC → Beijing (UTC+8) 时间转换。

    - naive datetime 视为 UTC
    - aware datetime 自动转换到 Beijing
    - 返回值 timezone 固定为 BEIJING_TZ
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BEIJING_TZ)


def format_beijing(dt: datetime | None) -> str:
    """格式化为 'YYYY-MM-DD HH:MM:SS (UTC+8)' 显示串。"""
    if dt is None:
        return "N/A"
    bj = utc_to_beijing(dt)
    return bj.strftime("%Y-%m-%d %H:%M:%S (UTC+8)")


def format_beijing_short(dt: datetime | None) -> str:
    """短格式: 'YYYY-MM-DD HH:MM:SS' (Beijing)"""
    if dt is None:
        return "N/A"
    bj = utc_to_beijing(dt)
    return bj.strftime("%Y-%m-%d %H:%M:%S")


def ensure_utc_aware(dt: datetime) -> datetime:
    """保证 datetime 是 UTC 且 TZ-aware (catalog 内统一使用)。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
