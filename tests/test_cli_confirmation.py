"""Tests for CLI confirmation flags (--yes) on destructive operations."""
from __future__ import annotations

import pytest

from src.cli import build_parser


class TestYesFlagParsing:
    """Verify --yes flag is accepted by reap, restore, cleanup parsers."""

    def test_reap_accepts_yes(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "reap",
            "--cluster", "ncbs_busi", "--week-start", "2026-05-30",
            "--yes",
        ])
        assert args.yes is True

    def test_reap_defaults_no_yes(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "reap",
            "--cluster", "ncbs_busi", "--week-start", "2026-05-30",
        ])
        assert args.yes is False

    def test_restore_accepts_yes(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "restore",
            "--cluster", "ncbs_busi", "--target", "2026-06-04 14:30:00",
            "--session-id", "abc-123", "--yes",
        ])
        assert args.yes is True

    def test_restore_defaults_no_yes(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "restore",
            "--cluster", "ncbs_busi", "--target", "2026-06-04 14:30:00",
            "--session-id", "abc-123",
        ])
        assert args.yes is False

    def test_cleanup_accepts_yes(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "cleanup",
            "--session-id", "abc-123", "--yes",
        ])
        assert args.yes is True

    def test_cleanup_defaults_no_yes(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "cleanup",
            "--session-id", "abc-123",
        ])
        assert args.yes is False

    def test_scan_has_no_yes_flag(self):
        """scan is always auto -- should not have --yes."""
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "scan", "--cluster", "ncbs_busi",
        ])
        assert not hasattr(args, "yes")

    def test_pack_weekly_has_no_yes_flag(self):
        """pack-weekly is auto when params complete -- should not have --yes."""
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "pack-weekly",
            "--cluster", "ncbs_busi", "--week-start", "2026-05-30",
        ])
        assert not hasattr(args, "yes")
