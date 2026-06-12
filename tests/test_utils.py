"""utils.py 单元测试: 时区转换 + Beijing time + 路径安全 + 原子写。"""
from __future__ import annotations

import datetime as dt
import os
import pathlib
import tempfile

import pytest

from src.errors import ArchiveError
from src.utils import (
    BEIJING_TZ,
    atomic_write,
    ensure_utc_aware,
    format_beijing,
    format_beijing_short,
    safe_rel_path,
    utc_to_beijing,
)


# ─── 时区 ───
def test_utc_to_beijing_naive_treated_as_utc():
    naive = dt.datetime(2026, 5, 30, 10, 0, 0)
    bj = utc_to_beijing(naive)
    assert bj.tzinfo == BEIJING_TZ
    assert bj.hour == 18  # UTC+8
    assert bj.day == 30


def test_utc_to_beijing_aware_converted():
    aware_utc = dt.datetime(2026, 5, 30, 10, 0, 0, tzinfo=dt.timezone.utc)
    bj = utc_to_beijing(aware_utc)
    assert bj.tzinfo == BEIJING_TZ
    assert bj.hour == 18


def test_format_beijing_full():
    utc = dt.datetime(2026, 5, 30, 18, 30, 0, tzinfo=dt.timezone.utc)
    assert format_beijing(utc) == "2026-05-31 02:30:00 (UTC+8)"


def test_format_beijing_short():
    utc = dt.datetime(2026, 5, 30, 18, 30, 0, tzinfo=dt.timezone.utc)
    assert format_beijing_short(utc) == "2026-05-31 02:30:00"


def test_format_beijing_none_returns_na():
    assert format_beijing(None) == "N/A"
    assert format_beijing_short(None) == "N/A"


def test_ensure_utc_aware_normalizes_naive():
    naive = dt.datetime(2026, 1, 1, 0, 0, 0)
    aware = ensure_utc_aware(naive)
    assert aware.tzinfo == dt.timezone.utc


def test_ensure_utc_aware_converts_other_tz():
    plus9 = dt.timezone(dt.timedelta(hours=9))
    in_jst = dt.datetime(2026, 1, 1, 9, 0, 0, tzinfo=plus9)
    out = ensure_utc_aware(in_jst)
    assert out.tzinfo == dt.timezone.utc
    assert out.hour == 0  # 9:00 JST = 0:00 UTC


# ─── safe_rel_path (CWE-22 防护) ───
class TestSafeRelPath:
    """10 个攻击 path 全部拒绝, 4 个合法 path 全部通过。"""

    @pytest.mark.parametrize("key", [
        "../etc/passwd",        # 父目录跳转
        "a/../../etc",          # 多重跳转
        "a/../b",               # 内部跳转
        "/etc/passwd",          # 绝对 (Unix)
        "/etc",                 # 绝对根
        "C:\\Windows\\System32", # 绝对 (Windows)
        "\\share\\path",        # UNC 路径
        "a/b\x00c",            # NUL 字节注入
        "a/./b",                # 当前目录段
        "a//b",                 # 双斜杠空段
        "",
    ])
    def test_blocks_malicious(self, key: str):
        with pytest.raises(ArchiveError):
            safe_rel_path(key)

    @pytest.mark.parametrize("key,expected", [
        ("instance/Db/123/file.rch", "instance/Db/123/file.rch"),
        ("Db/1780160839955/file_0.rch", "Db/1780160839955/file_0.rch"),
        ("tenant_abc/Difference/d.rch", "tenant_abc/Difference/d.rch"),
        ("a/b/c", "a/b/c"),
    ])
    def test_accepts_legitimate(self, key: str, expected: str):
        assert safe_rel_path(key) == expected


# ─── atomic_write (CWE-377 防护) ───
class TestAtomicWrite:
    def test_success_overwrites_via_rename(self, tmp_path: pathlib.Path):
        target = tmp_path / "archive.tar.gz"
        target.write_bytes(b"OLD")
        atomic_write(target, b"NEW")
        assert target.read_bytes() == b"NEW"
        # 不应有残留 .tmp 文件
        leftovers = [p for p in tmp_path.iterdir() if p.name.startswith(".tmp.")]
        assert leftovers == [], f"orphan tmp files: {leftovers}"

    def test_failure_preserves_old_file(self, tmp_path: pathlib.Path):
        target = tmp_path / "archive.tar.gz"
        target.write_bytes(b"OLD")
        with pytest.raises((TypeError, AttributeError)):
            atomic_write(target, None)  # 模拟 data 错误
        assert target.read_bytes() == b"OLD", "old must survive failure"
        # 失败时 tmp 文件应被清理
        leftovers = [p for p in tmp_path.iterdir() if p.name.startswith(".tmp.")]
        assert leftovers == [], f"orphan tmp on failure: {leftovers}"

    def test_creates_parent_dir(self, tmp_path: pathlib.Path):
        target = tmp_path / "nested" / "deeper" / "archive.tar.gz"
        atomic_write(target, b"hello")
        assert target.exists()
        assert target.read_bytes() == b"hello"

    def test_concurrent_overwrite_via_rename(self, tmp_path: pathlib.Path):
        """两次顺序写, 第二次应原子覆盖 (不读到中间态)。"""
        target = tmp_path / "x"
        atomic_write(target, b"v1")
        atomic_write(target, b"v2")
        assert target.read_bytes() == b"v2"
