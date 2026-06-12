"""错误类型层级存在性测试。"""
from src.errors import (
    ArchiveError, ConfigError, InvalidArchivePolicyError,
    InvalidWeekStartDayError, ArchiveDirNotFoundError,
    CatalogError, ObsError, UnsafeDeleteError,
    RestoreError, SnapshotNotFoundError, PitrNotCapableError,
    CleanupSafetyError,
)


def test_invalid_policy_is_archive_error():
    err = InvalidArchivePolicyError("xlog without full")
    assert isinstance(err, ArchiveError)
    assert "xlog without full" in str(err)


def test_unsafe_delete_is_archive_error():
    err = UnsafeDeleteError("not archived")
    assert isinstance(err, ArchiveError)


def test_cleanup_safety_is_restore_error():
    err = CleanupSafetyError("etag mismatch")
    assert isinstance(err, RestoreError)


def test_policy_chain():
    assert issubclass(InvalidWeekStartDayError, InvalidArchivePolicyError)
    assert issubclass(InvalidWeekStartDayError, ArchiveError)


def test_archive_dir_not_found_inherits_archive_error():
    assert issubclass(ArchiveDirNotFoundError, ArchiveError)


def test_restore_subclass_chain():
    assert issubclass(SnapshotNotFoundError, RestoreError)
    assert issubclass(PitrNotCapableError, RestoreError)
    assert issubclass(CleanupSafetyError, RestoreError)
    assert issubclass(RestoreError, ArchiveError)
