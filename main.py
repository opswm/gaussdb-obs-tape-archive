"""CLI 入口。把子命令路由到对应模块。"""
from __future__ import annotations

import datetime as dt
import logging
import sys
from pathlib import Path

from src.catalog import Catalog
from src.cli import build_parser
from src.config import load_config
from src.errors import ArchiveError
from src.obs_client import ObsClient
from src.policy import validate_policies
from src.tape_lib import TapeLibrary


def _build_obs(cfg) -> ObsClient:
    """生产实现走真实 SDK; CLI 默认走 mock, 方便本地测试。"""
    return ObsClient.create_mock()


def _build_tape(cfg) -> TapeLibrary:
    return TapeLibrary.create_simulated(
        cfg.tape.simulated_path, cfg.tape.max_volume_size_gb,
    )


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

    if args.command == "pack":
        from src.packer import Packer
        obs = _build_obs(cfg)
        p = Packer(obs, cat, Path(cfg.work_dir))
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        da = p.pack_daily(ins.instance_id, args.date)
        log.info(f"packed: {da.archive_filename}")
        return 0

    if args.command == "archive":
        from src.archiver import Archiver
        tape = _build_tape(cfg)
        a = Archiver(tape, cat)
        for da in cat.list_daily_archives_by_status("pending"):
            tar = Path(cfg.work_dir) / da.archive_filename
            if not tar.exists():
                log.warning(f"missing tar: {tar}")
                continue
            a.archive_to_tape(da.id, str(tar))
            log.info(f"archived: {da.archive_filename}")
        return 0

    if args.command == "reap":
        from src.reaper import Reaper
        obs = _build_obs(cfg)
        r = Reaper(obs, cat)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        for da in cat._conn().execute(
            "SELECT id FROM daily_archives WHERE instance_id=? AND archive_date=?",
            (ins.instance_id, args.date),
        ).fetchall():
            summary = r.reap_daily_archive(da["id"])
            log.info(f"reaped daily_archive {da['id']}: {summary}")
        return 0

    if args.command == "restore-plan":
        from src.restorer import Restorer
        obs = _build_obs(cfg)
        tape = _build_tape(cfg)
        r = Restorer(obs, tape, cat, Path(cfg.work_dir))
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        plan = r.plan(dt.datetime.fromisoformat(args.target), ins.instance_id)
        print(plan)
        return 0

    if args.command == "restore":
        from src.restorer import Restorer
        obs = _build_obs(cfg)
        tape = _build_tape(cfg)
        r = Restorer(obs, tape, cat, Path(cfg.work_dir))
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
        for ins in cfg.instances:
            if ins.alias != args.cluster:
                continue
            n_pending = sum(1 for _ in cat.list_daily_archives_by_status("pending"))
            n_on_tape = sum(1 for _ in cat.list_daily_archives_by_status("on_tape"))
            print(f"{ins.alias}: pending={n_pending} on_tape={n_on_tape}")
        return 0

    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ArchiveError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
