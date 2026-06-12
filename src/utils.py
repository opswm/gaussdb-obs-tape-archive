"""通用工具: 时区转换、Beijing 时间显示、路径安全校验。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.errors import ArchiveError

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


def safe_rel_path(key: str) -> str:
    """校验 key 是相对路径且不含父目录跳转 (CWE-22 防护)。

    拒绝:
    - 绝对路径 (以 / 开头, 或 Windows 风格 \\ 盘符)
    - 含 NUL 字节
    - 含 .. 段
    - 空字符串

    返回与输入等价的相对路径 (无前缀 /, 无尾部 slash)。
    适用于 obs_key (packer 写 staging) 和 tar member.name (restorer 读 + 写 OBS)。
    """
    if not key:
        raise ArchiveError("路径为空")
    if "\x00" in key:
        raise ArchiveError(f"路径含 NUL 字节: {key!r}")
    # 绝对路径检测: 必须先于 lstrip
    if key.startswith("/") or key.startswith("\\") or ":\\" in key or key.startswith("//"):
        raise ArchiveError(f"路径是绝对: {key!r}")
    # 隐藏的相对跳转: 中间段为 .. 或 . 或 空 (例如 a//b)
    parts = key.split("/")
    if any(p in ("..", ".", "") for p in parts):
        raise ArchiveError(f"路径含跳转/空段 (../..//.): {key!r}")
    if not parts or parts == [""]:
        raise ArchiveError(f"路径是空或仅 /: {key!r}")
    return key


def atomic_write(target_path: Path, data: bytes) -> None:
    """原子写: 先写 .tmp.{uuid}, 再 rename 到 target。
    保证中途失败不破坏 target 文件 (例如上次成功的 tar.gz)。
    """
    import os
    import tempfile
    target_path = Path(target_path)
    target_dir = target_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(target_dir), prefix=".tmp.", suffix=target_path.suffix or "",
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_path, target_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
