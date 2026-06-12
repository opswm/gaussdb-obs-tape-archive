"""周度调度入口: 调用各模块完成 scan → pack_weekly。

Scheduler 永远不调用 Reaper; Reaper 是单独的人工触发子命令。
Reaper 涉及"删除线上数据", 必须人工二次确认。
"""
from __future__ import annotations

import datetime as dt
import logging
import sys
import uuid
from pathlib import Path

from src.catalog import Catalog
from src.config import load_config
from src.obs_client import ObsClient
from src.packer import Packer
from src.policy import validate_policies
from src.scanner import Scanner
from src.week_boundary import compute_week_range


def weekly_archive_job(config_path: str) -> None:
    """周度流水线: scan → pack_weekly (per-cluster 按 week_start_day 算当前周)。"""
    log = logging.getLogger("gaussdb-archive.weekly")
    run_id = str(uuid.uuid4())
    cfg = load_config(config_path)
    validate_policies([i.policy for i in cfg.instances])

    cat = Catalog(cfg.catalog.path)
    cat.init_schema()
    cat.log_operation("weekly_job_start", run_id=run_id, status="running")
    obs = ObsClient.create_mock()  # 生产替换为真实 OBS
    work_dir = Path(cfg.work_dir)
    archive_dir = Path(cfg.archive_dir.path)
    archive_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Phase 1: scan
        s = Scanner(obs, cat)
        for ins in cfg.instances:
            if not ins.enabled:
                continue
            n = s.scan_instance(ins.instance_id, ins.policy)
            log.info(f"[{run_id}] scanned {ins.alias}: {n}")
        cat.log_operation("scan", run_id=run_id, status="success")

        # Phase 2: 推进到 queued_for_archive (scan 不自动推进)
        for ins in cfg.instances:
            if not ins.enabled:
                continue
            for bo in cat.list_backup_objects_by_status(
                    "discovered", instance_id=ins.instance_id):
                cat.update_backup_object_status(bo.id, "queued_for_archive")

        # Phase 3: pack_weekly (per-cluster 当前周)
        p = Packer(obs, cat, work_dir, archive_dir)
        packed_count = 0
        for ins in cfg.instances:
            if not ins.enabled:
                continue
            week_start, week_end = compute_week_range(
                dt.date.today(), ins.policy.week_start_day,
            )
            try:
                result = p.pack_weekly(ins.instance_id, week_start, week_end)
                packed_count += 1
                log.info(
                    f"[{run_id}] packed {ins.alias} week={week_start}: "
                    f"{result.archive_filename} "
                    f"sha256={result.checksum_sha256[:12] if result.checksum_sha256 else 'N/A'}"
                )
            except Exception as e:
                log.exception(
                    f"pack failed for {ins.alias} week={week_start}: {e}"
                )
        log.info(f"[{run_id}] packed: {packed_count}")
        cat.log_operation("pack_weekly", run_id=run_id, status="success")

        log.info(
            f"[{run_id}] weekly job done. Reaper 必须人工触发。"
        )

    except Exception as e:
        cat.log_operation("weekly_job_failed", run_id=run_id,
                          status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    weekly_archive_job(
        sys.argv[1] if len(sys.argv) > 1 else "config/archive_config.json"
    )
