"""Tests for storage estimator: pending size calculation and disk checks."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.catalog import Catalog
from src.storage_estimator import (
    StorageEstimate, WeekEstimate, _format_bytes,
    estimate_pending, find_pending_weeks,
)


class TestFormatBytes:
    def test_zero(self):
        assert _format_bytes(0) == "0 B"

    def test_bytes(self):
        assert _format_bytes(500) == "500 B"

    def test_kb(self):
        assert _format_bytes(2048) == "2.0 KB"

    def test_mb(self):
        assert _format_bytes(5 * 1024 * 1024) == "5.0 MB"

    def test_gb(self):
        assert _format_bytes(3 * 1024**3) == "3.0 GB"

    def test_tb(self):
        assert _format_bytes(2 * 1024**4) == "2.0 TB"


class TestStorageEstimateFormat:
    def test_empty_format(self):
        est = StorageEstimate(
            total_pending_bytes=0,
            total_pending_human="0 B",
            disk_free_bytes=100 * 1024**3,
            disk_free_human="100.0 GB",
            sufficient=True,
        )
        output = est.format_display()
        assert "待归档总大小: 0 B" in output
        assert "磁盘空间: 充足" in output

    def test_with_weeks_format(self):
        est = StorageEstimate(
            total_pending_bytes=10 * 1024**3,
            total_pending_human="10.0 GB",
            per_week=[
                WeekEstimate(
                    cluster_alias="ncbs_busi",
                    week_start="2026-05-30", week_end="2026-06-06",
                    total_bytes=6 * 1024**3, total_human="6.0 GB",
                    full_count=1, diff_count=6, snapshot_count=0, xlog_count=128,
                ),
                WeekEstimate(
                    cluster_alias="ncbs_busi",
                    week_start="2026-06-06", week_end="2026-06-13",
                    total_bytes=4 * 1024**3, total_human="4.0 GB",
                    full_count=1, diff_count=5, snapshot_count=0, xlog_count=110,
                ),
            ],
            disk_free_bytes=50 * 1024**3,
            disk_free_human="50.0 GB",
            sufficient=True,
        )
        output = est.format_display()
        assert "ncbs_busi" in output
        assert "2026-05-30~2026-06-06" in output
        assert "6.0 GB" in output

    def test_insufficient_warning(self):
        est = StorageEstimate(
            total_pending_bytes=30 * 1024**3,
            total_pending_human="30.0 GB",
            disk_free_bytes=40 * 1024**3,
            disk_free_human="40.0 GB",
            sufficient=False,
            warning="磁盘空间不足",
        )
        output = est.format_display()
        assert "磁盘空间不足" in output


class TestEstimatePending:
    def test_empty_catalog(self, tmp_path):
        """No queued objects -> zero estimate."""
        db_path = tmp_path / "test.db"
        cat = Catalog(str(db_path))
        cat.init_schema()

        estimate = estimate_pending(cat, [], tmp_path)
        assert estimate.total_pending_bytes == 0
        assert estimate.sufficient is True

    def test_with_queued_objects(self, tmp_path):
        """Objects with status=queued_for_archive are counted."""
        db_path = tmp_path / "test.db"
        cat = Catalog(str(db_path))
        cat.init_schema()

        # Register an instance so the JOIN works
        cat.upsert_instance(
            "tenant_a_test", "test-cluster", "Test Cluster",
            "", "test-bucket", True,
        )
        from src.models import Policy
        cat.upsert_policy("tenant_a_test", Policy(
            archive_full=True, archive_diff=True,
            archive_snapshot=True, archive_xlog=True,
            week_start_day=6,
        ))

        # Insert a backup object directly (bypassing the normal flow)
        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("tenant_a_test/Db/1780160839955/file.rch",
             "tenant_a_test", 100_000_000,
             dt.datetime(2026, 6, 1).isoformat(),
             "full", "1780160839955", "2026-06-01",
             "queued_for_archive"),
        )
        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("tenant_a_test/Db/1780160839955/file2.rch",
             "tenant_a_test", 50_000_000,
             dt.datetime(2026, 6, 1).isoformat(),
             "xlog", "1780160839955", "2026-06-01",
             "queued_for_archive"),
        )

        from src.config import InstanceConfig
        inst = InstanceConfig(
            alias="test-cluster", instance_id="tenant_a_test",
            display_name="Test", description="", enabled=True,
            policy=Policy(
                archive_full=True, archive_diff=True,
                archive_snapshot=True, archive_xlog=True,
                week_start_day=6,
            ),
        )
        estimate = estimate_pending(cat, [inst], tmp_path)
        assert estimate.total_pending_bytes == 150_000_000
        assert len(estimate.per_week) == 1

    def test_skips_non_queued_objects(self, tmp_path):
        """Objects not in queued_for_archive status are ignored."""
        db_path = tmp_path / "test.db"
        cat = Catalog(str(db_path))
        cat.init_schema()

        cat.upsert_instance(
            "tenant_a_test", "test-cluster", "Test Cluster",
            "", "test-bucket", True,
        )
        from src.models import Policy
        cat.upsert_policy("tenant_a_test", Policy(
            archive_full=True, archive_diff=True,
            archive_snapshot=True, archive_xlog=True,
            week_start_day=6,
        ))

        # discovered object (not queued)
        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("tenant_a_test/Db/1780160839955/file.rch",
             "tenant_a_test", 100_000_000,
             dt.datetime(2026, 6, 1).isoformat(),
             "full", "1780160839955", "2026-06-01",
             "discovered"),
        )

        estimate = estimate_pending(cat, [], tmp_path)
        assert estimate.total_pending_bytes == 0


class TestFindPendingWeeks:
    def test_empty(self, tmp_path):
        db_path = tmp_path / "test.db"
        cat = Catalog(str(db_path))
        cat.init_schema()

        weeks = find_pending_weeks(cat, "test-instance", 6)
        assert weeks == []

    def test_single_week(self, tmp_path):
        db_path = tmp_path / "test.db"
        cat = Catalog(str(db_path))
        cat.init_schema()

        # Register instance for FK constraint
        cat.upsert_instance(
            "test-instance", "test-cluster", "Test",
            "", "test-bucket", True,
        )

        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("inst/Log/xlog1", "test-instance", 1000,
             dt.datetime(2026, 6, 1).isoformat(),
             "xlog", "seg1", "2026-06-01", "queued_for_archive"),
        )
        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("inst/Log/xlog2", "test-instance", 2000,
             dt.datetime(2026, 6, 3).isoformat(),
             "xlog", "seg2", "2026-06-03", "queued_for_archive"),
        )

        weeks = find_pending_weeks(cat, "test-instance", 6)
        # Both dates should fall in the same week (week_start_day=6 means Saturday)
        assert len(weeks) >= 1

    def test_multi_week(self, tmp_path):
        db_path = tmp_path / "test.db"
        cat = Catalog(str(db_path))
        cat.init_schema()

        # Register instance for FK constraint
        cat.upsert_instance(
            "test-instance", "test-cluster", "Test",
            "", "test-bucket", True,
        )

        # Two dates 10 days apart should be in different weeks
        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("inst/Log/xlog1", "test-instance", 1000,
             dt.datetime(2026, 6, 1).isoformat(),
             "xlog", "seg1", "2026-06-01", "queued_for_archive"),
        )
        cat._conn().execute(
            """INSERT INTO backup_objects
               (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                backup_type, parent_backup_dir, backup_date, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("inst/Log/xlog2", "test-instance", 2000,
             dt.datetime(2026, 6, 15).isoformat(),
             "xlog", "seg2", "2026-06-15", "queued_for_archive"),
        )

        weeks = find_pending_weeks(cat, "test-instance", 6)
        assert len(weeks) >= 2
        # Should be sorted by week_start
        assert weeks[0][0] < weeks[-1][0]
