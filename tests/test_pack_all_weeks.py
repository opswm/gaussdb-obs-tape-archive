"""Tests for pack-all-weeks CLI subcommand and scheduler --pack-all mode."""
from __future__ import annotations

from src.cli import build_parser


class TestPackAllWeeksCLI:
    """Verify pack-all-weeks subcommand parses correctly."""

    def test_required_cluster(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "pack-all-weeks",
            "--cluster", "ncbs_busi",
        ])
        assert args.command == "pack-all-weeks"
        assert args.cluster == "ncbs_busi"
        assert args.stop_on_error is False

    def test_stop_on_error_flag(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "pack-all-weeks",
            "--cluster", "ncbs_busi", "--stop-on-error",
        ])
        assert args.stop_on_error is True

    def test_stop_on_error_default(self):
        parser = build_parser()
        args = parser.parse_args([
            "--config", "config/test.json", "pack-all-weeks",
            "--cluster", "ncbs_busi",
        ])
        assert args.stop_on_error is False
