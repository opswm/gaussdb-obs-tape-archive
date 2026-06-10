"""Packer 单元测试。
- 按天打包 (instance_id, archive_date) 维度
- tar.gz 内含所有对象 (按 obs_key 路径) + manifest.json
- 单对象 SHA256 入库, archive 整体 SHA256 入库
- 幂等: 同一 (instance, date) 二次调用返回同一 daily_archive.id
"""
import io
import json
import tarfile
import datetime as dt
import hashlib
from src.obs_client import ObsClient
from src.catalog import Catalog
from src.models import Policy, BackupObject
from src.packer import Packer


def _bootstrap(tmp_path):
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "core", "b1", True)
    cat.upsert_policy("i1", Policy(True, True, True, True))
    # 三个对象, 全部 2026-06-09
    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "i1/Db/1780160839955/file_0.rch", 5, dt.datetime(2026, 6, 9, 1, 7, 0), "e1"),
        ("b1", "i1/Difference/1780177759671/file_0.rch", 3, dt.datetime(2026, 6, 9, 6, 7, 0), "e2"),
        ("b1", "i1/Log/cn_5001/pg_xlog/.../f.rch", 4,
         dt.datetime(2026, 6, 9, 0, 0, 1), "e3"),
    ])
    # 录入 discovered 状态
    for key, typ, parent, ts, size, etag in [
        ("i1/Db/1780160839955/file_0.rch", "full", "1780160839955", 1780160839955, 5, "e1"),
        ("i1/Difference/1780177759671/file_0.rch", "diff", "1780177759671", 1780177759671, 3, "e2"),
        ("i1/Log/cn_5001/pg_xlog/.../f.rch", "xlog", "000000010000023B000000B0", None, 4, "e3"),
    ]:
        cat.upsert_backup_object(BackupObject(
            obs_key=key, instance_id="i1",
            obs_last_modified=dt.datetime(2026, 6, 9, 0, 0, 0),
            backup_type=typ, parent_backup_dir=parent, backup_date="2026-06-09",
            backup_timestamp_ms=ts, obs_size_bytes=size, obs_etag=etag,
        ))
        bo = cat.get_backup_object_by_key(key)
        cat.update_backup_object_status(bo.id, "queued_for_archive")
    return cat, obs


def test_pack_creates_tar_gz_with_objects(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = Packer(obs, cat, tmp_path / "work")
    da = p.pack_daily(instance_id="i1", date="2026-06-09")

    assert da.archive_filename == "ncbs_busi_2026-06-09.tar.gz"
    tar_path = tmp_path / "work" / da.archive_filename
    assert tar_path.exists()
    # 解包验证
    with tarfile.open(tar_path, "r:gz") as tf:
        names = tf.getnames()
    assert any("Db/1780160839955/file_0.rch" in n for n in names)
    assert any("Difference/1780177759671/file_0.rch" in n for n in names)


def test_pack_generates_manifest_json(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = Packer(obs, cat, tmp_path / "work")
    da = p.pack_daily(instance_id="i1", date="2026-06-09")
    manifest_path = tmp_path / "work" / "ncbs_busi_2026-06-09.manifest.json"
    assert manifest_path.exists()
    m = json.loads(manifest_path.read_text())
    assert m["instance_alias"] == "ncbs_busi"
    assert m["archive_date"] == "2026-06-09"
    assert m["contents"]["full_count"] == 1  # 实际数据中 full_count 在 manifest 是多少？


def test_pack_records_sha256_and_attaches_objects(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = Packer(obs, cat, tmp_path / "work")
    da = p.pack_daily(instance_id="i1", date="2026-06-09")

    # 所有 backup_objects 应 status=archiving, daily_archive_id 关联
    bos = list(cat.get_objects_by_daily_archive(da.id))
    assert len(bos) == 3
    for bo in bos:
        assert bo.status == "archiving"
        assert bo.checksum_sha256 is not None  # 单对象 SHA256 已记录


def test_pack_idempotent_on_existing_daily_archive(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = Packer(obs, cat, tmp_path / "work")
    da1 = p.pack_daily("i1", "2026-06-09")
    da2 = p.pack_daily("i1", "2026-06-09")
    assert da1.id == da2.id
