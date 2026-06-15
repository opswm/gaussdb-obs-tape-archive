"""CLI 入口。把子命令路由到对应模块。"""
from __future__ import annotations

import datetime as dt
import logging
import sys
from datetime import date
from pathlib import Path

from src.compat import date_fromisoformat

from src.catalog import Catalog
from src.cli import build_parser
from src.config import load_config
from src.errors import ArchiveError, ArchiveDirNotFoundError
from src.obs_client import ObsClient
from src.policy import validate_policies
from src.week_boundary import compute_week_range


def _build_obs(cfg) -> ObsClient:
    """根据配置创建 OBS 客户端。

    - obs.use_mock=true (或未配置 access_key): 使用 Mock OBS (内存模拟)
    - obs.use_mock=false + 真实凭证: 使用内置 obs SDK 对接真实华为云 OBS
    """
    obs_cfg = cfg.obs
    if getattr(obs_cfg, "use_mock", False) or not obs_cfg.access_key:
        return ObsClient.create_mock()
    return ObsClient.create_real(
        access_key=obs_cfg.access_key,
        secret_key=obs_cfg.secret_key,
        endpoint=obs_cfg.endpoint,
        bucket=obs_cfg.bucket_name,
        timeout=getattr(obs_cfg, "timeout", 60),
        max_retry_count=getattr(obs_cfg, "max_retry_count", 3),
    )


def _archive_dir_or_die(cfg) -> Path:
    """解析 archive_dir 并确保存在 (用于写周度 tar.gz)。"""
    p = Path(cfg.archive_dir.path)
    if not p.exists():
        raise ArchiveDirNotFoundError(
            f"archive_dir 不存在: {p} (请先 mkdir 或修改配置)"
        )
    return p


def _confirm_or_die(prompt: str, yes_flag: bool) -> None:
    """如果 --yes 未设置, 交互式确认; 拒绝则 sys.exit(1)。"""
    if yes_flag:
        return
    try:
        answer = input(f"{prompt} [y/N]: ").strip().lower()
    except EOFError:
        print("非交互环境且未指定 --yes, 已取消。", file=sys.stderr)
        sys.exit(1)
    if answer not in ("y", "yes"):
        print("已取消。", file=sys.stderr)
        sys.exit(0)


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
        from src.storage_estimator import estimate_pending
        obs = _build_obs(cfg)
        s = Scanner(obs, cat)
        for ins in cfg.instances:
            if args.cluster and ins.alias != args.cluster:
                continue
            n = s.scan_instance(ins.instance_id, ins.policy)
            log.info(f"scanned {ins.alias}: {n} objects")
        # Storage estimation after scan
        archive_dir_path = Path(cfg.archive_dir.path)
        estimate = estimate_pending(cat, cfg.instances, archive_dir_path)
        print(estimate.format_display())
        if estimate.warning:
            log.warning(estimate.warning)
        return 0

    if args.command in ("pack", "pack-weekly"):
        from src.packer import Packer
        obs = _build_obs(cfg)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        archive_dir = _archive_dir_or_die(cfg)
        p = Packer(obs, cat, Path(cfg.work_dir), archive_dir,
                   compression_level=cfg.archive.compression_level,
                   compress=cfg.archive.compress)
        week_start, week_end = compute_week_range(
            date_fromisoformat(args.week_start)
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

    if args.command == "pack-all-weeks":
        from src.packer import Packer
        from src.storage_estimator import find_pending_weeks
        obs = _build_obs(cfg)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        archive_dir = _archive_dir_or_die(cfg)
        p = Packer(obs, cat, Path(cfg.work_dir), archive_dir,
                   compression_level=cfg.archive.compression_level,
                   compress=cfg.archive.compress)

        weeks = find_pending_weeks(cat, ins.instance_id, ins.policy.week_start_day)
        if not weeks:
            log.info(f"集群 {ins.alias}: 没有待打包的周")
            return 0

        log.info(f"集群 {ins.alias}: 发现 {len(weeks)} 个待打包周")
        for ws, we in weeks:
            log.info(f"  待打包: {ws} ~ {we}")
        log.info("")

        success_count = 0
        fail_count = 0
        for ws, we in weeks:
            log.info(f"开始打包: {ws} ~ {we}")
            try:
                result = p.pack_weekly(ins.instance_id, ws, we)
                # Verify SHA256 on disk
                actual_sha = p._sha256_file(result.archive_path)
                if actual_sha != result.checksum_sha256:
                    raise ArchiveError(
                        f"SHA256 校验失败: "
                        f"expected={result.checksum_sha256[:12]}..., "
                        f"actual={actual_sha[:12]}..."
                    )
                size_gb = result.archive_path.stat().st_size / 1e9
                log.info(
                    f"  OK {result.archive_filename} "
                    f"({size_gb:.1f} GB, sha256={result.checksum_sha256[:12]}...)"
                )
                success_count += 1
            except Exception as e:
                log.error(f"  失败 {ws}~{we}: {e}")
                fail_count += 1
                if args.stop_on_error:
                    log.error("--stop-on-error 已设置, 中止")
                    return 1

        log.info(f"完成: {success_count} 成功, {fail_count} 失败 (共 {len(weeks)} 周)")
        return 0 if fail_count == 0 else 1

    if args.command == "pack-daily":
        from src.packer import Packer
        obs = _build_obs(cfg)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        archive_dir = _archive_dir_or_die(cfg)
        p = Packer(obs, cat, Path(cfg.work_dir), archive_dir,
                   compression_level=cfg.archive.compression_level,
                   compress=cfg.archive.compress)
        archive_date = args.date or dt.date.today().isoformat()
        result = p.pack_daily(ins.instance_id, archive_date, preview=args.preview)
        if args.preview:
            print(result.preview_text or "(空)")
        else:
            log.info(
                f"packed daily: {result.archive_filename} "
                f"sha256={result.checksum_sha256[:12] if result.checksum_sha256 else 'N/A'}..."
            )
        return 0

    if args.command == "pack-all-days":
        from src.packer import Packer
        obs = _build_obs(cfg)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        archive_dir = _archive_dir_or_die(cfg)
        p = Packer(obs, cat, Path(cfg.work_dir), archive_dir,
                   compression_level=cfg.archive.compression_level,
                   compress=cfg.archive.compress)

        dates = cat.find_pending_daily_dates(ins.instance_id)
        if not dates:
            log.info(f"集群 {ins.alias}: 没有待打包的日期")
            return 0

        log.info(f"集群 {ins.alias}: 发现 {len(dates)} 个待打包日期")
        for d in dates:
            log.info(f"  待打包: {d}")
        log.info("")

        success_count = 0
        fail_count = 0
        for d in dates:
            log.info(f"开始打包: {d}")
            try:
                result = p.pack_daily(ins.instance_id, d)
                if result.archive_path and result.archive_path.exists():
                    if result.archive_path.is_file():
                        actual_sha = p._sha256_file(result.archive_path)
                    else:
                        actual_sha = p._sha256_file(result.archive_path / "metadata.json")
                    if actual_sha != result.checksum_sha256:
                        raise ArchiveError(
                            f"SHA256 校验失败: "
                            f"expected={result.checksum_sha256[:12]}..., "
                            f"actual={actual_sha[:12]}..."
                        )
                log.info(f"  OK {result.archive_filename}")
                success_count += 1
            except Exception as e:
                log.error(f"  失败 {d}: {e}")
                fail_count += 1
                if args.stop_on_error:
                    log.error("--stop-on-error 已设置, 中止")
                    return 1

        log.info(f"完成: {success_count} 成功, {fail_count} 失败 (共 {len(dates)} 天)")
        return 0 if fail_count == 0 else 1

    if args.command == "reap":
        from src.reaper import Reaper
        _confirm_or_die(
            "即将删除 OBS 上的原始备份对象, 此操作不可逆。确认?",
            args.yes,
        )
        obs = _build_obs(cfg)
        r = Reaper(obs, cat)
        ins = next(i for i in cfg.instances if i.alias == args.cluster)
        week_start, week_end = compute_week_range(
            date_fromisoformat(args.week_start), ins.policy.week_start_day,
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
        _confirm_or_die(
            "即将上传数据到 OBS 进行恢复, 此操作会修改 OBS 内容。确认?",
            args.yes,
        )
        obs = _build_obs(cfg)
        archive_dir = _archive_dir_or_die(cfg)
        r = Restorer(obs, cat, Path(cfg.work_dir), archive_dir)
        r.execute(args.session_id)
        log.info(f"restored session {args.session_id}")
        return 0

    if args.command == "cleanup":
        from src.cleaner import Cleaner
        _confirm_or_die(
            "即将删除 OBS 上的恢复临时对象, 确认?",
            args.yes,
        )
        obs = _build_obs(cfg)
        c = Cleaner(obs, cat)
        summary = c.cleanup(args.session_id)
        log.info(f"cleaned session {args.session_id}: {summary}")
        return 0

    if args.command == "cluster":
        if args.cluster_command == "list":
            for ins in cfg.instances:
                print(
                    f"{ins.alias:14s} {ins.instance_id:42s} "
                    f"{ins.display_name:16s} enabled={ins.enabled}  "
                    f"week_start_day={ins.policy.week_start_day}"
                )
            return 0
        if args.cluster_command == "show":
            ins = next(i for i in cfg.instances if i.alias == args.cluster)
            print(f"alias: {ins.alias}")
            print(f"instance_id: {ins.instance_id}")
            print(f"display_name: {ins.display_name}")
            print(f"bucket: {cfg.obs.bucket_name}")
            print(f"enabled: {ins.enabled}")
            print(f"")
            print(f"策略:")
            print(f"  archive_full: {ins.policy.archive_full}")
            print(f"  archive_snapshot: {ins.policy.archive_snapshot}")
            print(f"  archive_diff: {ins.policy.archive_diff}")
            print(f"  archive_xlog: {ins.policy.archive_xlog}")
            print(f"  retention_days: {ins.policy.retention_days}")
            print(f"  xlog_redundancy_hours: {ins.policy.xlog_redundancy_hours}")
            print(f"  xlog_forward_hours: {ins.policy.xlog_forward_hours}")
            print(f"  week_start_day: {ins.policy.week_start_day}")
            print(f"")
            print(f"PITR 能力: {'✓ (full + diff + xlog 全开)' if ins.policy.is_full_pitr_capable() else '✗'}")
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
                    1 for _ in cat.list_daily_archives_by_status("archived", instance_id=ins.instance_id)
                )
                n_pending = sum(
                    1 for _ in cat.list_daily_archives_by_status("pending", instance_id=ins.instance_id)
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
