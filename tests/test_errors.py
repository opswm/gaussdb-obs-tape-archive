"""所有项目内异常都应继承 ArchiveError 或其子类。"""
from src.errors import (
    ArchiveError,
    InvalidArchivePolicyError,
    UnsafeDeleteError,
    CleanupSafetyError,
    TapeWriteError,
    RestoreError,
)


def test_invalid_policy_is_archive_error():
    err = InvalidArchivePolicyError("xlog without full")
    assert isinstance(err, ArchiveError)
    assert "xlog without full" in str(err)


def test_unsafe_delete_is_archive_error():
    err = UnsafeDeleteError("not on tape")
    assert isinstance(err, ArchiveError)


def test_cleanup_safety_is_restore_error():
    err = CleanupSafetyError("etag mismatch")
    assert isinstance(err, RestoreError)


def test_tape_write_includes_position():
    err = TapeWriteError("verify failed", tape_position=12345)
    assert isinstance(err, ArchiveError)
    assert err.tape_position == 12345
