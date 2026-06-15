"""CLI argparse 子命令解析测试。"""
from __future__ import annotations


def test_scan_subcommand():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "scan", "--cluster", "ncbs_busi"])
    assert args.command == "scan"
    assert args.cluster == "ncbs_busi"
    assert args.config == "cfg.json"


def test_restore_plan_subcommand():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "restore-plan",
                         "--cluster", "trgl_busi",
                         "--target", "2026-06-09 14:30:00"])
    assert args.command == "restore-plan"
    assert args.target == "2026-06-09 14:30:00"
    assert args.cluster == "trgl_busi"


def test_reap_requires_dry_run_or_confirm():
    from src.cli import build_parser
    p = build_parser()
    a1 = p.parse_args(["--config", "cfg.json", "reap",
                       "--cluster", "ncbs_busi", "--week-start", "2026-05-30"])
    assert a1.dry_run is False
    a2 = p.parse_args(["--config", "cfg.json", "reap",
                       "--cluster", "ncbs_busi", "--week-start", "2026-05-30",
                       "--dry-run"])
    assert a2.dry_run is True


def test_cleanup_subcommand():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "cleanup", "--session-id", "abc-123"])
    assert args.command == "cleanup"
    assert args.session_id == "abc-123"


def test_cluster_show_subcommand():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "cluster", "show",
                         "--cluster", "itps_busi"])
    assert args.command == "cluster"
    assert args.cluster_command == "show"
    assert args.cluster == "itps_busi"


def test_scheduler_defines_weekly_job():
    """确保 scheduler 模块暴露 weekly_archive_job。"""
    from scheduler import weekly_archive_job
    assert callable(weekly_archive_job)


def test_pack_daily_subcommand():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "pack-daily",
                         "--cluster", "ncbs_busi", "--date", "2026-06-15"])
    assert args.command == "pack-daily"
    assert args.cluster == "ncbs_busi"
    assert args.date == "2026-06-15"


def test_pack_daily_default_date():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "pack-daily",
                         "--cluster", "ncbs_busi"])
    assert args.date is None  # 默认今天


def test_pack_all_days_subcommand():
    from src.cli import build_parser
    p = build_parser()
    args = p.parse_args(["--config", "cfg.json", "pack-all-days",
                         "--cluster", "ncbs_busi", "--stop-on-error"])
    assert args.command == "pack-all-days"
    assert args.stop_on_error is True
