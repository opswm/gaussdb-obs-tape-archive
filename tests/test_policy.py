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
