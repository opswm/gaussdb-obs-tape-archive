"""端到端冒烟: 单集群 → 单日完整流水线 → PITR 恢复 → Cleaner 清理。

不依赖真实 OBS/磁带; 全 mock 跑通业务逻辑。
覆盖:
- 完整流水线 (scan → pack → archive → reap → restore → cleanup)
- P1-4: 恢复数据不重复入库
- P2-4: itps_busi 关闭 xlog 时 PITR 必须被拒绝
- P2-5: 非法配置启动拒绝
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from src.catalog import Catalog
from src.cleaner import Cleaner
from src.config import load_config
from src.errors import PitrNotCapableError
from src.obs_client import ObsClient
from src.packer import Packer
from src.policy import validate_policies
from src.reaper import Reaper
from src.restorer import Restorer, plan_snapshot_restore
from src.scanner import Scanner


def _write_cfg(tmp_path: Path, instances: list[dict]) -> Path:
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps({
        "obs": {"bucket_name": "b", "endpoint": "http://x",
                "access_key": "a", "secret_key": "s"},
        "instances": instances,
        "archive_dir": str(tmp_path / "tape_mapping"),
        "catalog": {"path": str(tmp_path / "cat.db"),
                    "backup_enabled": False, "backup_path": "",
                    "backup_retention_days": 90},
        "work_dir": str(tmp_path / "work"),
        "archive": {"required_manual_confirm_for_delete": True,
                    "max_concurrent_pack_jobs": 1,
                    "daily_archive_format": "tar.gz", "compression_level": 6},
        "restore": {"local_work_retention_hours": 24},
    }, ensure_ascii=False))
    return cfg_path


def _ncbs_instance() -> dict:
    return {
        "alias": "ncbs_busi", "instance_id": "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        "display_name": "核心", "description": "", "enabled": True,
        "archive_policy": {"archive_full": True, "archive_snapshot": True,
                           "archive_diff": True, "archive_xlog": True,
                           "retention_days": 90,
                           "xlog_redundancy_hours": 6.0,
                           "xlog_forward_hours": 6.0},
    }


def test_e2e_full_pipeline(tmp_path: Path):
    cfg_path = _write_cfg(tmp_path, [_ncbs_instance()])
    cfg = load_config(str(cfg_path))
    validate_policies([i.policy for i in cfg.instances])

    cat = Catalog(cfg.catalog.path); cat.init_schema()
    for ins in cfg.instances:
        cat.upsert_instance(ins.instance_id, ins.alias, ins.display_name,
                            ins.description, cfg.obs.bucket_name, ins.enabled)
        cat.upsert_policy(ins.instance_id, ins.policy)

    obs = ObsClient.create_mock(initial_objects=[
        ("b", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/1780160839955/file_0.rch", 50,
         dt.datetime(2026, 6, 9, 1, 7, 0), "e1"),
        ("b", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Difference/1780177759671/file_0.rch", 30,
         dt.datetime(2026, 6, 9, 6, 7, 0), "e2"),
        ("b", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Log/cn_5001/pg_xlog/tl_3/9/"
              "00000001000002400000000A_00_00_00000020", 4,
         dt.datetime(2026, 6, 9, 10, 0, 0), "e3"),
        ("b", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Log/cn_5001/pg_xlog/tl_3/9/"
              "00000001000002400000000B_00_00_00000020", 4,
         dt.datetime(2026, 6, 9, 11, 0, 0), "e4"),
        ("b", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Log/cn_5001/obs_last_clean_record", 1,
         dt.datetime(2026, 6, 9, 0, 0, 0), "e5"),
        ("b", "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/backup_metadata.cfg", 1,
         dt.datetime(2026, 6, 9, 0, 0, 0), "e6"),
    ])
    work_dir = tmp_path / "work"; work_dir.mkdir()
    archive_dir = tmp_path / "tape_mapping"
    archive_dir.mkdir()

    # 1. scan
    Scanner(obs, cat).scan_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", cat.get_policy("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2"))
    n = sum(1 for _ in cat.list_backup_objects_by_status("discovered",
            instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2"))
    assert n == 6, f"scan 漏对象: {n}"

    # 推进到 queued_for_archive
    for bo in cat.list_backup_objects_by_status("discovered",
                  instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2"):
        cat.update_backup_object_status(bo.id, "queued_for_archive")

    # 2. pack_weekly — 周度流水线: ncbs_busi 默认 week_start_day=6, 2026-06-09 周二
    #    落在 2026-06-06 (周六) ~ 2026-06-13 (下周六) 窗口
    p = Packer(obs, cat, work_dir, archive_dir)
    # 让策略 week_start_day=6
    from src.models import Policy as _Policy
    cat.upsert_policy("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
                      _Policy(archive_full=True, archive_snapshot=True,
                              archive_diff=True, archive_xlog=True,
                              week_start_day=6))
    from src.week_boundary import compute_week_range as _cwr
    from datetime import date as _date
    week_start, week_end = _cwr(_date(2026, 6, 9), 6)
    result = p.pack_weekly("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
                            week_start, week_end)
    assert result.archive_filename is not None
    assert result.archive_filename.startswith("ncbs_busi_W")
    da_id = cat._conn().execute(
        "SELECT id FROM daily_archives ORDER BY id DESC LIMIT 1"
    ).fetchone()["id"]
    da = cat.get_daily_archive(da_id)
    assert da.archive_filename == result.archive_filename

    # 3. (no separate archive step in v2.0; pack_weekly writes directly to archive_dir)
    #     verify daily_archive is already 'archived' with checksum
    da_after = cat.get_daily_archive(da_id)
    assert da_after.status == "archived"
    assert da_after.checksum_sha256 is not None

    # 4. PITR 准备
    cat.upsert_pitr_chain(
        chain_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2_chain_1780160839955",
        instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        base_full_dir="1780160839955",
        base_full_time=dt.datetime(2026, 6, 9, 1, 7, 19),
        diff_dirs=["1780177759671"],
        chain_start_time=dt.datetime(2026, 6, 9, 1, 7, 19),
        chain_end_time=dt.datetime(2026, 6, 10, 0, 0, 0),
    )

    # 5. PITR plan (用全新目标 OBS)
    obs2 = ObsClient.create_mock()
    restorer = Restorer(obs2, cat, work_dir, archive_dir)
    plan = restorer.plan(target_time=dt.datetime(2026, 6, 9, 14, 30),
                         instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2")
    assert plan["required_full_dir"] == "1780160839955"

    # 6. Reap (在 PITR 之前把 archived 对象标 obs_deleted)
    Reaper(obs, cat).reap_daily_archive(da.id)
    # 顺序门禁: full/snapshot/diff/xlog 全部 → obs_deleted, metadata 跳过
    for bo in cat.get_objects_by_daily_archive(da.id):
        if bo.backup_type == "metadata":
            continue
        assert bo.status == "obs_deleted", f"{bo.obs_key} = {bo.status}"

    # 7. Restore (execute)
    restorer.execute(plan["session_id"])

    # 8. Cleaner 清理
    Cleaner(obs2, cat).cleanup(plan["session_id"])

    # 9. 验收: obs2 中无任何本次恢复对象残留
    remaining = [o.key for o in obs2.list_objects("b", prefix="")]
    assert remaining == [], f"残留: {remaining}"


def test_e2e_pitr_rejected_when_xlog_disabled(tmp_path: Path):
    """P2-4: itps_busi 关闭 xlog 时, PITR 必须被拒绝。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("itps_tenant_8b3f9c1a_inst_9d2e4567b9f0c1a2", "itps_busi", "柜面", "", "b1", True)
    from src.models import Policy
    cat.upsert_policy("itps_tenant_8b3f9c1a_inst_9d2e4567b9f0c1a2", Policy(
        archive_full=True, archive_snapshot=True, archive_diff=True,
        archive_xlog=False, retention_days=90,
    ))
    r = Restorer(obs_client=None, catalog=cat,
                 work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")
    with pytest.raises(PitrNotCapableError):
        r.plan(target_time=dt.datetime(2026, 5, 1),
               instance_id="itps_tenant_8b3f9c1a_inst_9d2e4567b9f0c1a2")


def test_e2e_invalid_config_rejected_at_startup(tmp_path: Path):
    """P2-5: diff→full 违反 1.4.2.1 约束的配置必须被启动拒绝。"""
    bad_instance = {
        "alias": "bad_busi", "instance_id": "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a9",
        "display_name": "bad", "description": "", "enabled": True,
        "archive_policy": {"archive_full": False, "archive_snapshot": False,
                           "archive_diff": True, "archive_xlog": True,
                           "retention_days": 90,
                           "xlog_redundancy_hours": 6.0,
                           "xlog_forward_hours": 6.0},
    }
    cfg_path = _write_cfg(tmp_path, [bad_instance])
    from src.policy import validate_policies
    from src.models import Policy
    cfg = load_config(str(cfg_path))
    with pytest.raises(Exception, match="策略"):
        validate_policies([i.policy for i in cfg.instances])


def test_e2e_plan_snapshot_restore_works(tmp_path: Path):
    """P0-4: Snapshot 独立恢复入口 end-to-end。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "ncbs_busi", "核心", "", "b1", True)
    from src.models import Policy, DailyArchive, BackupObject
    cat.upsert_policy("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", Policy(
        archive_full=True, archive_snapshot=True, archive_diff=True,
        archive_xlog=True, retention_days=90,
    ))
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", archive_date="2026-06-10",
        archive_filename="ncbs_busi_2026-06-10.tar.gz", status="archived",
        checksum_sha256="sha",
    ))
    bo_id = cat.upsert_backup_object(BackupObject(
        obs_key="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Snapshot/1781000000000/s.rch",
        instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        obs_last_modified=dt.datetime(2026, 6, 10, 0, 0, 0),
        backup_type="snapshot", parent_backup_dir="1781000000000",
        backup_date="2026-06-10", backup_timestamp_ms=1781000000000,
        status="archived",
    ))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo_id), da_id)
    plan = plan_snapshot_restore(cat, "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "1781000000000")
    assert plan["required_full"]["dir_name"] == "1781000000000"
    assert da_id in plan["required_full"]["daily_archive_ids"]
