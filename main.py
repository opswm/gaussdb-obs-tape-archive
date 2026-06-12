"""CLI 入口。把子命令路由到对应模块。"""
from __future__ import annotations

import datetime as dt
import logging
import sys
from datetime import date
from pathlib import Path

from src.catalog import Catalog
from src.cli import build_parser
from src.config import load_config
from src.errors import ArchiveError, ArchiveDirNotFoundError
from src.obs_client import ObsClient
from src.policy import validate_policies
from src.week_boundary import compute_week_range


def _build_obs(cfg) -> ObsClient:
    """生产实现走真实 SDK; CLI 默认走 mock, 方便本地测试。"""
    return ObsClient.create_mock()


def _archive_dir_or_die(cfg) -> Path:
    """解析 archive_dir 并确保存在 (用于写周度 tar.gz)。"""
    p = Path(cfg.archive_dir.path)
    if not p.exists():
        raise ArchiveDirNotFoundError(
            f"archive_dir 不存在: {p} (请先 mkdir 或修改配置)"
        )
    return p


def _parse_week_start_or_today(arg: str | None, week_start_day: int) -> tuple[date, date]:
    """解析 --week-start 参数; None → 当前周。"""
    if arg:
        return date.fromisoformat(arg), None  # 仅起; 止由 main 计算
    ws = dt.date.today()
    s, e = compute_week_range(ws, week_start_day)
    return s, e


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("gaussdb-archive")

    cfg = load_config(args.config)
    validate_policies([i.policy for i in cfg.instances])
    cat = Catalog(cfg.catalog.path)
    cat.init_schema()
    for ins in cfg.instances:
        cat.upsert_instance(ins.instance_id, ins.alias, ins.display_name,
                            ins.description, cfg.obs.bucket_name, ins.enabled)
        cat.upsert_policy(ins.instance_id, ins.policy)

    if args.command == "scan":
        from src.scanner import Scanner
        obs = _build_obs(cfg)
        s = Scanner(obs, cat)
        for ins in cfg.instances:
            if args.cluster and ins.alias != args.cluster:
                continue
            n = s.scan_instance(ins.instance_id, ins.policy)
            log.info(f"scanned {ins.alias}: {n} objects")
        return 0

    if args.command in ("pack", "pack-weekly"):
        from src.packer import Packer
        obs = _build_obs(cfg)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        archive_dir = _archive_dir_or_die(cfg)
        p = Packer(obs, cat, Path(cfg.work_dir), archive_dir)
        week_start, week_end = compute_week_range(
            date.fromisoformat(args.week_start)
            if args.week_start else dt.date.today(),
            ins.policy.week_start_day,
        )
        result = p.pack_weekly(ins.instance_id, week_start, week_end,
                                preview=args.preview)
        if args.preview:
            print(result.preview_text or "(空)")
        else:
            log.info(
                f"packed weekly: {result.archive_filename} "
                f"sha256={result.checksum_sha256[:12] if result.checksum_sha256 else 'N/A'}..."
            )
        return 0

    if args.command == "reap":
        from src.reaper import Reaper
        obs = _build_obs(cfg)
        r = Reaper(obs, cat)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        week_start, week_end = compute_week_range(
            date.fromisoformat(args.week_start), ins.policy.week_start_day,
        )
        from src.week_boundary import week_range_to_iso_strings
        ws_iso, we_iso = week_range_to_iso_strings(week_start, week_end)
        for da in cat.list_weekly_archives_in_range(ins.instance_id,
                                                     week_start.isoformat(),
                                                     week_end.isoformat()):
            summary = r.reap_daily_archive(da.id)
            log.info(f"reaped weekly {da.archive_date} (id={da.id}): {summary}")
        return 0

    if args.command == "restore-plan":
        from src.restorer import Restorer
        obs = _build_obs(cfg)
        archive_dir = _archive_dir_or_die(cfg)
        r = Restorer(obs, cat, Path(cfg.work_dir), archive_dir)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        plan = r.plan(dt.datetime.fromisoformat(args.target), ins.instance_id)
        print(plan)
        return 0

    if args.command == "restore":
        from src.restorer import Restorer
        obs = _build_obs(cfg)
        archive_dir = _archive_dir_or_die(cfg)
        r = Restorer(obs, cat, Path(cfg.work_dir), archive_dir)
        r.execute(args.session_id)
        log.info(f"restored session {args.session_id}")
        return 0

    if args.command == "cleanup":
        from src.cleaner import Cleaner
        obs = _build_obs(cfg)
        c = Cleaner(obs, cat)
        summary = c.cleanup(args.session_id)
        log.info(f"cleaned session {args.session_id}: {summary}")
        return 0

    if args.command == "status":
        from src.week_boundary import compute_week_range
        for ins in cfg.instances:
            if ins.alias != args.cluster:
                continue
            if args.week_start:
                week_start, week_end = compute_week_range(
                    date.fromisoformat(args.week_start),
                    ins.policy.week_start_day,
                )
                archives = list(cat.list_weekly_archives_in_range(
                    ins.instance_id, week_start.isoformat(), week_end.isoformat()))
                for da in archives:
                    print(
                        f"  weekly {da.archive_date} → {da.archive_week_end}: "
                        f"{da.archive_filename} status={da.status}"
                    )
            else:
                n_archived = sum(
                    1 for _ in cat.list_daily_archives_by_status("archived")
                )
                n_pending = sum(
                    1 for _ in cat.list_daily_archives_by_status("pending")
                )
                print(f"{ins.alias}: pending={n_pending} archived={n_archived}")
        return 0

    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ArchiveError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
