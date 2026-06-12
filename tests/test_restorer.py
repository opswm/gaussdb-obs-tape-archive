"""Restorer: PITR 计划生成 + 执行 + Snapshot 独立恢复 (P0-4)。"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from src.catalog import Catalog
from src.errors import RestoreError, SnapshotNotFoundError, PitrNotCapableError
from src.obs_client import ObsClient
from src.restorer import Restorer, plan_snapshot_restore


# ─────────────────── Fixtures ───────────────────
def _seed_full_pipeline(tmp_path: Path):
    """建 scanner→packer→archiver 完整流水线的产物。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "ncbs_busi", "核心", "", "b1", True)
    cat.upsert_policy("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", _full_policy())

    # PITR chain: base full 2026-06-08, 1 个 diff, chain 覆盖 06-08~06-10
    cat.upsert_pitr_chain(
        chain_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2_chain_1780160839955",
        instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        base_full_dir="1780160839955",
        base_full_time=dt.datetime(2026, 6, 8, 1, 7, 19),
        diff_dirs=["1780177759671"],
        chain_start_time=dt.datetime(2026, 6, 8, 1, 7, 19),
        chain_end_time=dt.datetime(2026, 6, 10, 0, 0, 0),
    )

    # 2 个 daily_archives (full + diff+xlog)
    da_full = cat.upsert_daily_archive(_da("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "2026-06-08",
                                           "ncbs_busi_2026-06-08.tar.gz", "sha-full"))
    da_diff = cat.upsert_daily_archive(_da("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "2026-06-09",
                                           "ncbs_busi_2026-06-09.tar.gz", "sha-diff"))

    # backup_objects
    bo_full = cat.upsert_backup_object(_bo("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/1780160839955/f.rch", "full", "1780160839955",
        "2026-06-08", 1780160839955, dt.datetime(2026, 6, 8, 1, 7, 19), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo_full), da_full)
    bo_diff = cat.upsert_backup_object(_bo("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Difference/1780177759671/d.rch", "diff", "1780177759671",
        "2026-06-09", 1780177759671, dt.datetime(2026, 6, 9, 1, 7, 19), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo_diff), da_diff)
    bo_xlog = cat.upsert_backup_object(_bo("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Log/cn_5001/pg_xlog/tl_3/9/00000001000002400000000A_00_00_00000020",
        "xlog", "00000001000002400000000A", "2026-06-09", None,
        dt.datetime(2026, 6, 9, 10, 0, 0), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo_xlog), da_diff)

    return cat, da_full, da_diff


def _full_policy():
    from src.models import Policy
    return Policy(archive_full=True, archive_snapshot=True,
                  archive_diff=True, archive_xlog=True, retention_days=90,
                  xlog_redundancy_hours=6.0, xlog_forward_hours=6.0)


def _da(instance_id, date, filename, sha):
    from src.models import DailyArchive
    return DailyArchive(instance_id=instance_id, archive_date=date,
                        archive_filename=filename, status="archived",
                        checksum_sha256=sha)


def _bo(instance_id, key, btype, parent, date, ts_ms, lm, status):
    from src.models import BackupObject
    return BackupObject(obs_key=key, instance_id=instance_id,
                        obs_last_modified=lm, backup_type=btype,
                        parent_backup_dir=parent, backup_date=date,
                        backup_timestamp_ms=ts_ms, status=status)


# ─────────────────── Tests: plan ───────────────────
def test_plan_pitr_restore_generates_correct_set(tmp_path):
    cat, _, _ = _seed_full_pipeline(tmp_path)
    r = Restorer(obs_client=None, catalog=cat,
                 work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")
    plan = r.plan(target_time=dt.datetime(2026, 6, 9, 14, 30),
                  instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2")
    assert plan["required_full_dir"] == "1780160839955"
    assert "1780177759671" in plan["required_diff_dirs"]
    # xlog 窗口起点 = base_full_time (无 diff_time 概念, 简化为 base)
    assert plan["xlog_time_start"].startswith("2026-06-08T01:07:19")
    assert plan["xlog_time_end"].startswith("2026-06-09T20:30")


def test_plan_rejects_no_chain(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "ncbs_busi", "核心", "", "b1", True)
    cat.upsert_policy("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", _full_policy())
    r = Restorer(obs_client=None, catalog=cat,
                 work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")
    with pytest.raises(RestoreError, match="PITR 链"):
        r.plan(dt.datetime(2026, 6, 9, 14, 30), "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2")


def test_plan_rejects_instance_without_xlog_policy(tmp_path):
    """P2-4: itps_busi 关闭 xlog 时, 计划 PITR 必须被拒绝。"""
    cat, _, _ = _seed_full_pipeline(tmp_path)
    # 把 xlog 改为 False 模拟 itps_busi
    from src.models import Policy
    cat.upsert_policy("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", Policy(
        archive_full=True, archive_snapshot=True, archive_diff=True,
        archive_xlog=False, retention_days=90,
    ))
    r = Restorer(obs_client=None, catalog=cat,
                 work_dir=tmp_path / "work", archive_dir=tmp_path / "archive")
    with pytest.raises(PitrNotCapableError):
        r.plan(dt.datetime(2026, 6, 9, 14, 30), "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2")


# ─────────────────── Tests: execute ───────────────────
def test_execute_creates_restore_session_and_objects(tmp_path):
    """execute 端到端: archive_dir 直接放 tar.gz, tar_path_override 仍兼容。"""
    cat, da_full, _ = _seed_full_pipeline(tmp_path)
    obs = ObsClient.create_mock()
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    import io, tarfile, hashlib, json as _json
    tar = archive_dir / "ncbs_busi_2026-06-08.tar.gz"
    with tarfile.open(tar, "w:gz") as tf:
        data = b"PAYLOAD_FULL"
        info = tarfile.TarInfo(name="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Db/1780160839955/f.rch")
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
        man_bytes = _json.dumps({"instance_alias": "ncbs_busi",
                                 "manifest_version": 1}).encode()
        info2 = tarfile.TarInfo(name="manifest.json")
        info2.size = len(man_bytes)
        tf.addfile(info2, io.BytesIO(man_bytes))

    full_sha = hashlib.sha256(tar.read_bytes()).hexdigest()

    # 把 da_diff 状态设成无需磁带回读: 我们直接清除 session 里的 da_diff
    # 只保留 da_full 在 required_daily_archives 里
    cat._conn().execute(
        "UPDATE daily_archives SET checksum_sha256=?, status='archived' WHERE archive_date='2026-06-08'",
        (full_sha,),
    )

    # 让 session 只引用 da_full: 重新 plan 后, 手动改 required_daily_archives
    r = Restorer(obs, cat, tmp_path / "work", tmp_path / "archive")
    sid = r.plan(target_time=dt.datetime(2026, 6, 9, 14, 30),
                 instance_id="tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2")["session_id"]
    # 截断 daily_archives 到只剩 da_full
    cat._conn().execute(
        "UPDATE restore_sessions SET required_daily_archives=? WHERE session_id=?",
        (_json.dumps([da_full]), sid),
    )

    r.execute(sid, tar_path_override=tar)

    ro = list(cat.list_restore_objects_for_session(sid))
    assert len(ro) >= 1
    for o in ro:
        assert o["uploaded_by_session"] == 1

    obs_keys = {o.key for o in obs.list_objects("b1", prefix="")}
    assert ("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/"
            "Db/1780160839955/f.rch") in obs_keys


# ─────────────────── Tests: plan_snapshot_restore (P0-4) ───────────────────
def test_plan_snapshot_restore_creates_session(tmp_path):
    cat, da_full, _ = _seed_full_pipeline(tmp_path)
    # 注入一个 snapshot 类型的 archived 对象, 挂到 da_full
    bo = cat.upsert_backup_object(_bo("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Snapshot/1781000000000/s.rch", "snapshot",
        "1781000000000", "2026-06-10", 1781000000000,
        dt.datetime(2026, 6, 10, 0, 0, 0), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_full)

    plan = plan_snapshot_restore(cat, "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "1781000000000")
    assert plan["required_full"]["dir_name"] == "1781000000000"
    assert plan["required_full"]["backup_type"] == "snapshot"
    assert da_full in plan["required_full"]["daily_archive_ids"]


def test_plan_snapshot_restore_not_found(tmp_path):
    cat, _, _ = _seed_full_pipeline(tmp_path)
    with pytest.raises(SnapshotNotFoundError):
        plan_snapshot_restore(cat, "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "9999999999999")


def test_plan_snapshot_restore_not_archived_rejected(tmp_path):
    cat, da_full, _ = _seed_full_pipeline(tmp_path)
    bo = cat.upsert_backup_object(_bo("tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2",
        "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2/Snapshot/1781000000000/s.rch", "snapshot",
        "1781000000000", "2026-06-10", 1781000000000,
        dt.datetime(2026, 6, 10, 0, 0, 0), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_full)
    # 改成 pending
    cat._conn().execute(
        "UPDATE daily_archives SET status='pending' WHERE id=?", (da_full,),
    )
    with pytest.raises(SnapshotNotFoundError, match="archived"):
        plan_snapshot_restore(cat, "tenant_8b3f9c1a_inst_7d2e4567b9f0c1a2", "1781000000000")


def test_pitr_plan_xlog_boundary_tz_normalization(tmp_path):
    """P1 修复: xlog 窗口边界 (== xlog_end) 必须包含。
    历史 bug: obs_last_modified 存为 naive ISO, 而 base_full_time 为 TZ-aware,
    字符串比较时 '2026-06-08T20:00:00' < '2026-06-08T20:00:00+00:00' (因 '+' < '0'),
    导致 == xlog_end 的边界 xlog 漏判。
    """
    import datetime as dt
    from src.models import Policy
    from src.scanner import Scanner
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    inst = "tenant_tz"
    cat.upsert_instance(inst, "n", "n", "", "b1", True)
    cat.upsert_policy(inst, Policy(
        archive_full=True, archive_snapshot=False, archive_diff=True,
        archive_xlog=True, retention_days=90,
    ))
    obs = ObsClient.create_mock()
    # 注入 base (06-08 00:00)
    obs._store[("b1", f"{inst}/Db/1780876800000/f.rch")] = (
        50, dt.datetime(2026, 6, 8, 0, 0, 0), "e-f", b"",
    )
    Scanner(obs, cat).scan_instance(inst, cat.get_policy(inst))
    # 注入 xlog 在窗口边界 == xlog_end (target 14:00 + 6h = 20:00)
    obs._store[("b1", f"{inst}/Log/cn1/pg_xlog/{0:024d}/001/x.rch")] = (
        10, dt.datetime(2026, 6, 8, 20, 0, 0), "e-x-edge", b"",
    )
    Scanner(obs, cat).scan_instance(inst, cat.get_policy(inst))
    # 验证 obs_last_modified 存为 TZ-aware
    r = cat._conn().execute(
        "SELECT obs_last_modified FROM backup_objects "
        "WHERE instance_id=? AND backup_type='xlog'",
        (inst,),
    ).fetchone()
    assert "+00:00" in r["obs_last_modified"], (
        f"应存 TZ-aware ISO, 实际 {r['obs_last_modified']}"
    )
    # plan target=14:00 → xlog_end=20:00, 边界 xlog 必须命中
    r_inst = Restorer(obs, cat, tmp_path / "wd", tmp_path / "archive")
    plan = r_inst.plan(dt.datetime(2026, 6, 8, 14, 0, 0), inst)
    assert plan["xlog_count"] == 1, (
        f"边界 xlog 应 1 个, 实际 {plan['xlog_count']}"
    )
