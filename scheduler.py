"""周度调度入口: 调用 main.py 各子命令完成流水线。

Scheduler 永远不调用 Reaper; Reaper 是单独的人工触发子命令。
Reaper 涉及"删除线上数据", 必须人工二次确认。
"""
from __future__ import annotations

import logging
import sys
import uuid
from pathlib import Path

from src.archiver import Archiver
from src.catalog import Catalog
from src.config import load_config
from src.obs_client import ObsClient
from src.packer import Packer
from src.policy import validate_policies
from src.scanner import Scanner
from src.tape_lib import TapeLibrary


def weekly_archive_job(config_path: str) -> None:
    log = logging.getLogger("gaussdb-archive.weekly")
    run_id = str(uuid.uuid4())
    cfg = load_config(config_path)
    validate_policies([i.policy for i in cfg.instances])

    cat = Catalog(cfg.catalog.path)
    cat.init_schema()
    cat.log_operation("weekly_job_start", run_id=run_id, status="running")
    obs = ObsClient.create_mock()  # 生产替换为真实 OBS
    tape = TapeLibrary.create_simulated(
        cfg.tape.simulated_path, cfg.tape.max_volume_size_gb)
    work_dir = Path(cfg.work_dir)

    try:
        # Phase 1: scan
        s = Scanner(obs, cat)
        for ins in cfg.instances:
            if not ins.enabled:
                continue
            n = s.scan_instance(ins.instance_id, ins.policy)
            log.info(f"[{run_id}] scanned {ins.alias}: {n}")
        cat.log_operation("scan", run_id=run_id, status="success")

        # Phase 2: pack
        p = Packer(obs, cat, work_dir)
        packed_count = 0
        for da in list(cat.list_daily_archives_by_status("pending")):
            ins_cfg = next(i for i in cfg.instances if i.instance_id == da.instance_id)
            try:
                p.pack_daily(da.instance_id, da.archive_date)
                packed_count += 1
            except Exception as e:
                log.exception(f"pack failed for {da.archive_date}: {e}")
        log.info(f"[{run_id}] packed: {packed_count}")
        cat.log_operation("pack", run_id=run_id, status="success")

        # Phase 3: archive
        a = Archiver(tape, cat)
        for da in list(cat.list_daily_archives_by_status("pending")):
            tar = work_dir / da.archive_filename
            if not tar.exists():
                log.warning(f"missing tar: {tar}")
                continue
            a.archive_to_tape(da.id, str(tar))
        cat.log_operation("archive", run_id=run_id, status="success")

        log.info(f"[{run_id}] weekly job done. Reaper 必须人工触发。")

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
