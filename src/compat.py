"""Python 3.7 兼容性模块。

本模块提供对 Python 3.8+ 特性的兼容实现:
- date.fromisoformat() - Python 3.8+
- datetime.fromisoformat() 带时区格式 - Python 3.11+
"""

import re
from datetime import date, datetime, timedelta, timezone


def date_fromisoformat(date_str: str) -> date:
    """兼容 Python 3.7 的 date.fromisoformat() 实现。
    
    支持格式: YYYY-MM-DD
    """
    if not isinstance(date_str, str):
        raise TypeError(f"fromisoformat: argument must be str, not {type(date_str).__name__}")
    
    match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', date_str)
    if not match:
        raise ValueError(f"Invalid isoformat string: '{date_str}'")
    
    year, month, day = map(int, match.groups())
    return date(year, month, day)


def datetime_fromisoformat(dt_str: str) -> datetime:
    """兼容 Python 3.7/3.8/3.9/3.10 的 datetime.fromisoformat() 实现。
    
    支持格式:
    - YYYY-MM-DD
    - YYYY-MM-DDTHH:MM:SS
    - YYYY-MM-DDTHH:MM:SS.ffffff
    - 以上格式带时区偏移 (+HH:MM 或 Z)
    """
    if not isinstance(dt_str, str):
        raise TypeError(f"fromisoformat: argument must be str, not {type(dt_str).__name__}")
    
    # 处理 Z 后缀 (UTC)
    s = dt_str.replace('Z', '+00:00')
    
    # 匹配格式: YYYY-MM-DD[THH:MM:SS[.ffffff]][+HH:MM]
    pattern = (
        r'^(\d{4})-(\d{2})-(\d{2})'
        r'(?:T(\d{2}):(\d{2}):(\d{2})'
        r'(?:\.(\d{6}))?)?'
        r'(?:([+-])(\d{2}):(\d{2}))?$'
    )
    
    match = re.match(pattern, s)
    if not match:
        raise ValueError(f"Invalid isoformat string: '{dt_str}'")
    
    groups = match.groups()
    year, month, day = map(int, groups[:3])
    
    # 时间部分 (可选)
    hour = int(groups[3]) if groups[3] else 0
    minute = int(groups[4]) if groups[4] else 0
    second = int(groups[5]) if groups[5] else 0
    microsecond = int(groups[6]) if groups[6] else 0
    
    # 时区部分 (可选)
    tz_sign = groups[7]
    if tz_sign is not None:
        tz_hour = int(groups[8])
        tz_minute = int(groups[9])
        tz_offset = timedelta(hours=tz_hour, minutes=tz_minute)
        if tz_sign == '-':
            tz_offset = -tz_offset
        tz = timezone(tz_offset)
    else:
        tz = None
    
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tz)