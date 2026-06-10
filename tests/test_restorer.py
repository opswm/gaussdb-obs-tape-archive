"""Restorer: PITR 计划生成 + 执行 + Snapshot 独立恢复 (P0-4)。"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from src.catalog import Catalog
from src.errors import RestoreError, SnapshotNotFoundError, PitrNotCapableError
from src.obs_client import ObsClient
from src.restorer import Restorer, plan_snapshot_restore
from src.tape_lib import TapeLibrary


# ─────────────────── Fixtures ───────────────────
def _seed_full_pipeline(tmp_path: Path):
    """建 scanner→packer→archiver 完整流水线的产物。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant123_inst456", "ncbs_busi", "核心", "", "b1", True)
    cat.upsert_policy("tenant123_inst456", _full_policy())

    # PITR chain: base full 2026-06-08, 1 个 diff, chain 覆盖 06-08~06-10
    cat.upsert_pitr_chain(
        chain_id="tenant123_inst456_chain_1780160839955",
        instance_id="tenant123_inst456",
        base_full_dir="1780160839955",
        base_full_time=dt.datetime(2026, 6, 8, 1, 7, 19),
        diff_dirs=["1780177759671"],
        chain_start_time=dt.datetime(2026, 6, 8, 1, 7, 19),
        chain_end_time=dt.datetime(2026, 6, 10, 0, 0, 0),
    )

    # 2 个 daily_archives (full + diff+xlog)
    da_full = cat.upsert_daily_archive(_da("tenant123_inst456", "2026-06-08",
                                           "ncbs_busi_2026-06-08.tar.gz", "sha-full"))
    da_diff = cat.upsert_daily_archive(_da("tenant123_inst456", "2026-06-09",
                                           "ncbs_busi_2026-06-09.tar.gz", "sha-diff"))

    # backup_objects
    bo_full = cat.upsert_backup_object(_bo("tenant123_inst456",
        "tenant123_inst456/Db/1780160839955/f.rch", "full", "1780160839955",
        "2026-06-08", 1780160839955, dt.datetime(2026, 6, 8, 1, 7, 19), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo_full), da_full)
    bo_diff = cat.upsert_backup_object(_bo("tenant123_inst456",
        "tenant123_inst456/Difference/1780177759671/d.rch", "diff", "1780177759671",
        "2026-06-09", 1780177759671, dt.datetime(2026, 6, 9, 1, 7, 19), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo_diff), da_diff)
    bo_xlog = cat.upsert_backup_object(_bo("tenant123_inst456",
        "tenant123_inst456/Log/cn_5001/pg_xlog/tl_3/9/00000001000002400000000A_00_00_00000020",
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
                        archive_filename=filename, status="on_tape",
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
    r = Restorer(obs_client=None, tape_lib=None, catalog=cat,
                 work_dir=tmp_path / "work")
    plan = r.plan(target_time=dt.datetime(2026, 6, 9, 14, 30),
                  instance_id="tenant123_inst456")
    assert plan["required_full_dir"] == "1780160839955"
    assert "1780177759671" in plan["required_diff_dirs"]
    assert plan["xlog_time_start"].startswith("2026-06-09")
    assert plan["xlog_time_end"].startswith("2026-06-09T20:30")


def test_plan_rejects_no_chain(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("tenant123_inst456", "ncbs_busi", "核心", "", "b1", True)
    cat.upsert_policy("tenant123_inst456", _full_policy())
    r = Restorer(obs_client=None, tape_lib=None, catalog=cat,
                 work_dir=tmp_path / "work")
    with pytest.raises(RestoreError, match="PITR 链"):
        r.plan(dt.datetime(2026, 6, 9, 14, 30), "tenant123_inst456")


def test_plan_rejects_instance_without_xlog_policy(tmp_path):
    """P2-4: itps_busi 关闭 xlog 时, 计划 PITR 必须被拒绝。"""
    cat, _, _ = _seed_full_pipeline(tmp_path)
    # 把 xlog 改为 False 模拟 itps_busi
    from src.models import Policy
    cat.upsert_policy("tenant123_inst456", Policy(
        archive_full=True, archive_snapshot=True, archive_diff=True,
        archive_xlog=False, retention_days=90,
    ))
    r = Restorer(obs_client=None, tape_lib=None, catalog=cat,
                 work_dir=tmp_path / "work")
    with pytest.raises(PitrNotCapableError):
        r.plan(dt.datetime(2026, 6, 9, 14, 30), "tenant123_inst456")


# ─────────────────── Tests: execute ───────────────────
def test_execute_creates_restore_session_and_objects(tmp_path):
    cat, _, _ = _seed_full_pipeline(tmp_path)
    obs = ObsClient.create_mock()  # 目标 OBS 空
    tape_lib = TapeLibrary.create_simulated(str(tmp_path / "tapes"), 10)

    # 把 tar.gz 放到磁带 (直接通过 tape_lib 写一份即可)
    tar = tmp_path / "ncbs_busi_2026-06-08.tar.gz"
    # 内容: 包含 manifest.json + 1 个 OBS key 文件
    import io, tarfile
    with tarfile.open(tar, "w:gz") as tf:
        data = b"PAYLOAD_FULL"
        info = tarfile.TarInfo(name="tenant123_inst456/Db/1780160839955/f.rch")
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
        # 假 manifest
        import json as _json
        man_bytes = _json.dumps({"instance_alias": "ncbs_busi", "manifest_version": 1}).encode()
        info2 = tarfile.TarInfo(name="manifest.json")
        info2.size = len(man_bytes)
        tf.addfile(info2, io.BytesIO(man_bytes))

    # 写磁带 + 更新 daily_archive 指向磁带
    res = tape_lib.write_archive(str(tar), archive_id=1)
    cat.update_daily_archive_status(
        cat._conn().execute(
            "SELECT id FROM daily_archives WHERE archive_date='2026-06-08'"
        ).fetchone()["id"],
        "on_tape", tape_volume=res.tape_volume,
        tape_position=res.tape_position,
    )

    # 写第二盘 (diff+xlog)
    tar2 = tmp_path / "ncbs_busi_2026-06-09.tar.gz"
    with tarfile.open(tar2, "w:gz") as tf:
        data = b"PAYLOAD_DIFF"
        info = tarfile.TarInfo(name="tenant123_inst456/Difference/1780177759671/d.rch")
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
        data2 = b"PAYLOAD_XLOG"
        info2 = tarfile.TarInfo(
            name="tenant123_inst456/Log/cn_5001/pg_xlog/tl_3/9/x.rch")
        info2.size = len(data2)
        tf.addfile(info2, io.BytesIO(data2))
        import json as _json
        man_bytes = _json.dumps({"instance_alias": "ncbs_busi", "manifest_version": 1}).encode()
        info3 = tarfile.TarInfo(name="manifest.json")
        info3.size = len(man_bytes)
        tf.addfile(info3, io.BytesIO(man_bytes))

    res2 = tape_lib.write_archive(str(tar2), archive_id=2)
    cat.update_daily_archive_status(
        cat._conn().execute(
            "SELECT id FROM daily_archives WHERE archive_date='2026-06-09'"
        ).fetchone()["id"],
        "on_tape", tape_volume=res2.tape_volume,
        tape_position=res2.tape_position,
    )

    r = Restorer(obs, tape_lib, cat, tmp_path / "work")
    sid = r.plan(target_time=dt.datetime(2026, 6, 9, 14, 30),
                 instance_id="tenant123_inst456")["session_id"]
    # 把 full 那盘的 sha 更新成 tar 的实际 sha, 否则 checksum mismatch
    import hashlib
    full_sha = hashlib.sha256(tar.read_bytes()).hexdigest()
    diff_sha = hashlib.sha256(tar2.read_bytes()).hexdigest()
    cat._conn().execute(
        "UPDATE daily_archives SET checksum_sha256=? WHERE archive_date='2026-06-08'",
        (full_sha,),
    )
    cat._conn().execute(
        "UPDATE daily_archives SET checksum_sha256=? WHERE archive_date='2026-06-09'",
        (diff_sha,),
    )

    r.execute(sid)

    ro = list(cat.list_restore_objects_for_session(sid))
    assert len(ro) >= 1
    for o in ro:
        assert o["uploaded_by_session"] == 1

    # 检查目标 OBS 真的写入了 key
    obs_keys = {o.key for o in obs.list_objects("b1", prefix="")}
    assert "tenant123_inst456/Db/1780160839955/f.rch" in obs_keys
    assert "tenant123_inst456/Difference/1780177759671/d.rch" in obs_keys


# ─────────────────── Tests: plan_snapshot_restore (P0-4) ───────────────────
def test_plan_snapshot_restore_creates_session(tmp_path):
    cat, da_full, _ = _seed_full_pipeline(tmp_path)
    # 注入一个 snapshot 类型的 archived 对象, 挂到 da_full
    bo = cat.upsert_backup_object(_bo("tenant123_inst456",
        "tenant123_inst456/Snapshot/1781000000000/s.rch", "snapshot",
        "1781000000000", "2026-06-10", 1781000000000,
        dt.datetime(2026, 6, 10, 0, 0, 0), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_full)

    plan = plan_snapshot_restore(cat, "tenant123_inst456", "1781000000000")
    assert plan["required_full"]["dir_name"] == "1781000000000"
    assert plan["required_full"]["backup_type"] == "snapshot"
    assert da_full in plan["required_full"]["daily_archive_ids"]


def test_plan_snapshot_restore_not_found(tmp_path):
    cat, _, _ = _seed_full_pipeline(tmp_path)
    with pytest.raises(SnapshotNotFoundError):
        plan_snapshot_restore(cat, "tenant123_inst456", "9999999999999")


def test_plan_snapshot_restore_not_on_tape_rejected(tmp_path):
    cat, da_full, _ = _seed_full_pipeline(tmp_path)
    bo = cat.upsert_backup_object(_bo("tenant123_inst456",
        "tenant123_inst456/Snapshot/1781000000000/s.rch", "snapshot",
        "1781000000000", "2026-06-10", 1781000000000,
        dt.datetime(2026, 6, 10, 0, 0, 0), "archived"))
    cat.attach_object_to_daily_archive(cat.get_backup_object(bo), da_full)
    # 改成 pending
    cat._conn().execute(
        "UPDATE daily_archives SET status='pending' WHERE id=?", (da_full,),
    )
    with pytest.raises(SnapshotNotFoundError, match="on_tape"):
        plan_snapshot_restore(cat, "tenant123_inst456", "1781000000000")
