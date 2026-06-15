"""Packer 周度/日度打包测试。
- pack_weekly 过滤 metadata / archive_only
- pack_daily 按日打包 (压缩 + 非压缩)
- xlog 时间窗 [week_start, week_end) 严格取
- 目录命名 W{start}_{end} (周) / {date}_{alias} (日)
- metadata.json 含 Beijing time
- 写 archive_dir
- 幂等
- preview 模式: 无 IO, 只输出清单
"""
import io
import json
import tarfile
import datetime as dt
import hashlib
from datetime import date

from src.obs_client import ObsClient
from src.catalog import Catalog
from src.models import Policy, BackupObject
from src.packer import Packer, WeeklyArchiveResult
from src.week_boundary import compute_week_range


# ─── bootstrap helpers ───
def _bootstrap(tmp_path, with_metadata=True):
    """3 个非元数据对象 + 1-2 个 metadata/archive_only 对象, 全部 2026-06-09 当周。"""
    cat = Catalog(str(tmp_path / "cat.db"))
    cat.init_schema()
    cat.upsert_instance("i1", "ncbs_busi", "核心", "core", "b1", True)
    cat.upsert_policy("i1", Policy(True, True, True, True, week_start_day=6))

    # 3 个正常对象 (full + diff + xlog), 都在 2026-05-30 ~ 2026-06-06 周内
    obs = ObsClient.create_mock(initial_objects=[
        ("b1", "i1/Db/1780160839955/file_0.rch", 5,
         dt.datetime(2026, 6, 1, 1, 7, 0), "e1"),
        ("b1", "i1/Difference/1780177759671/file_0.rch", 3,
         dt.datetime(2026, 6, 2, 6, 7, 0), "e2"),
        ("b1", "i1/Log/cn_5001/pg_xlog/tl_3/9/000000010000023B000000B0_00_00_00000020", 4,
         dt.datetime(2026, 6, 3, 0, 0, 1), "e3"),
    ])
    rows = [
        ("i1/Db/1780160839955/file_0.rch", "full", "1780160839955",
         1780160839955, 5, "e1", "normal", "2026-06-01"),
        ("i1/Difference/1780177759671/file_0.rch", "diff", "1780177759671",
         1780177759671, 3, "e2", "normal", "2026-06-02"),
        ("i1/Log/cn_5001/pg_xlog/tl_3/9/000000010000023B000000B0_00_00_00000020",
         "xlog", "000000010000023B000000B0", None, 4, "e3", "normal", "2026-06-03"),
    ]
    if with_metadata:
        # 2 个 metadata + 1 个 archive_only, 都应被跳过
        obs._store[("b1", "i1/Log/cn_5001/obs_last_clean_record")] = (
            1, dt.datetime(2026, 6, 1, 0, 0, 0), "e-m1", b"m1",
        )
        obs._store[("b1", "i1/Log/cn_5001/cn_build_history")] = (
            1, dt.datetime(2026, 6, 2, 0, 0, 0), "e-m2", b"m2",
        )
        obs._store[("b1", "i1/backup_metadata.cfg")] = (
            1, dt.datetime(2026, 6, 4, 0, 0, 0), "e-m3", b"m3",
        )
        rows.extend([
            ("i1/Log/cn_5001/obs_last_clean_record", "metadata", "cn_5001",
             None, 1, "e-m1", "archive_only", "2026-06-01"),
            ("i1/Log/cn_5001/cn_build_history", "metadata", "cn_5001",
             None, 1, "e-m2", "archive_only", "2026-06-02"),
            ("i1/backup_metadata.cfg", "metadata", "i1",
             None, 1, "e-m3", "archive_only", "2026-06-04"),
        ])

    for (key, typ, parent, ts, size, etag, rp, date_str) in rows:
        cat.upsert_backup_object(BackupObject(
            obs_key=key, instance_id="i1",
            obs_last_modified=dt.datetime(2026, 6, 1, 0, 0, 0),
            backup_type=typ, parent_backup_dir=parent, backup_date=date_str,
            backup_timestamp_ms=ts, obs_size_bytes=size, obs_etag=etag,
            restore_policy=rp,
        ))
        bo = cat.get_backup_object_by_key(key)
        cat.update_backup_object_status(bo.id, "queued_for_archive")
    return cat, obs


def _packer(tmp_path, obs, cat, compress=True):
    return Packer(obs, cat, tmp_path / "work", tmp_path / "archive_dir",
                  compress=compress)


# ─── tests ───
def test_pack_weekly_writes_tar_gz_to_archive_dir(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)  # 周六起点
    result = p.pack_weekly("i1", week_start, week_end)

    assert result.archive_filename is not None
    assert result.archive_filename.startswith("ncbs_busi_W")
    assert result.archive_filename.endswith(".tar.gz")
    tar_path = tmp_path / "archive_dir" / result.archive_filename
    assert tar_path.exists()
    # 验证 tar 内部含 3 个对象 + metadata.json
    with tarfile.open(tar_path, "r:gz") as tf:
        names = tf.getnames()
    assert any("Db/1780160839955/file_0.rch" in n for n in names)
    assert any("Difference/1780177759671/file_0.rch" in n for n in names)
    assert any("pg_xlog" in n for n in names)
    assert "metadata.json" in names


def test_pack_weekly_filters_metadata_and_archive_only(tmp_path):
    cat, obs = _bootstrap(tmp_path, with_metadata=True)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    result = p.pack_weekly("i1", week_start, week_end)

    # 3 个正常 + 0 metadata
    assert result.metadata_skipped == 3
    tar_path = tmp_path / "archive_dir" / result.archive_filename
    with tarfile.open(tar_path, "r:gz") as tf:
        names = tf.getnames()
    assert not any("obs_last_clean_record" in n for n in names)
    assert not any("cn_build_history" in n for n in names)
    assert not any("backup_metadata.cfg" in n for n in names)


def test_pack_weekly_xlog_time_window(tmp_path):
    """xlog 在窗口外的不应被打包。"""
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    # 注入一个 xlog, 时间在窗口外 (上周末)
    obs._store[("b1", "i1/Log/cn_5001/pg_xlog/tl_3/9/000000010000020000000000_00_00_00000020")] = (
        4, dt.datetime(2026, 5, 28, 12, 0, 0), "e-out", b"out-of-window",
    )
    cat.upsert_backup_object(BackupObject(
        obs_key="i1/Log/cn_5001/pg_xlog/tl_3/9/000000010000020000000000_00_00_00000020",
        instance_id="i1",
        obs_last_modified=dt.datetime(2026, 5, 28, 12, 0, 0),
        backup_type="xlog", parent_backup_dir="000000010000020000000000",
        backup_date="2026-05-28", backup_timestamp_ms=None,
        obs_size_bytes=4, obs_etag="e-out",
    ))
    cat.update_backup_object_status(
        cat.get_backup_object_by_key(
            "i1/Log/cn_5001/pg_xlog/tl_3/9/000000010000020000000000_00_00_00000020"
        ).id, "queued_for_archive",
    )
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    result = p.pack_weekly("i1", week_start, week_end)
    # 窗口内仅 1 个 xlog
    assert len(result.xlog_obs) == 1
    # 校验 tar 不含窗口外 xlog
    tar_path = tmp_path / "archive_dir" / result.archive_filename
    with tarfile.open(tar_path, "r:gz") as tf:
        names = tf.getnames()
    assert "000000010000020000000000" not in " ".join(names)


def test_pack_weekly_directory_naming(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    result = p.pack_weekly("i1", week_start, week_end)
    # ncbs_busi_W{start_YYYYMMDD}_{end_YYYYMMDD}.tar.gz
    expected = f"ncbs_busi_W{week_start.strftime('%Y%m%d')}_{week_end.strftime('%Y%m%d')}.tar.gz"
    assert result.archive_filename == expected


def test_pack_weekly_metadata_json_contents(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    result = p.pack_weekly("i1", week_start, week_end)
    # 从 tar 读 metadata.json
    tar_path = tmp_path / "archive_dir" / result.archive_filename
    with tarfile.open(tar_path, "r:gz") as tf:
        member = next(m for m in tf.getmembers() if m.name == "metadata.json")
        meta = json.loads(tf.extractfile(member).read().decode())
    assert meta["schema_version"] == "2.0"
    assert meta["archive_type"] == "weekly"
    assert meta["cluster"]["alias"] == "ncbs_busi"
    assert meta["archive_period"]["week_start_day"] == 6
    # 验证 Beijing time 转换
    full_0 = meta["contents"]["full_dirs"][0]
    assert "beijing" in full_0
    assert "(UTC+8)" in meta["archive_period"]["week_start_beijing"]
    assert meta["contents"]["xlog_summary"]["count"] >= 1


def test_pack_weekly_records_sha256_and_attaches_objects(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    result = p.pack_weekly("i1", week_start, week_end)

    # 读 daily_archive 行
    da = cat.get_daily_archive(
        cat._conn().execute(
            "SELECT id FROM daily_archives ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
    )
    assert da.status == "archived"
    assert da.checksum_sha256 is not None
    assert da.archive_filename == result.archive_filename

    # 关联的 3 个 backup_objects 应 status=archived, daily_archive_id 关联
    bos = list(cat.get_objects_by_daily_archive(da.id))
    assert len(bos) == 3
    for bo in bos:
        assert bo.status == "archived"
        assert bo.daily_archive_id == da.id
        assert bo.checksum_sha256 is not None


def test_pack_weekly_idempotent(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    r1 = p.pack_weekly("i1", week_start, week_end)
    r2 = p.pack_weekly("i1", week_start, week_end)
    # 二次调用应命中已存在, 复用 tar.gz
    assert r1.archive_filename == r2.archive_filename
    assert "已存在 weekly archive" in (r2.preview_text or "")


def test_pack_weekly_preview_no_io(tmp_path):
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    week_start, week_end = compute_week_range(date(2026, 6, 3), 6)
    result = p.pack_weekly("i1", week_start, week_end, preview=True)

    # 不写 tar, 不创建 daily_archive
    assert result.preview is True
    assert result.archive_filename is None
    assert result.archive_path is None
    assert result.checksum_sha256 is None
    # archive_dir 应无 tar.gz
    assert list((tmp_path / "archive_dir").iterdir()) == []
    # preview_text 含人类可读清单
    assert result.preview_text is not None
    assert "周度范围" in result.preview_text
    assert "Beijing=" in result.preview_text or "Beijing" in result.preview_text
    # catalog 中 daily_archive 仍空
    rows = list(cat._conn().execute("SELECT * FROM daily_archives"))
    assert rows == []


# ─── daily pack tests ───
def test_pack_daily_writes_to_archive_dir(tmp_path):
    """日度打包: 压缩模式, 写 tar.gz。"""
    cat, obs = _bootstrap(tmp_path, with_metadata=False)
    p = _packer(tmp_path, obs, cat, compress=True)
    result = p.pack_daily("i1", "2026-06-01")
    assert result.archive_filename is not None
    assert result.archive_filename.endswith(".tar.gz")
    assert "ncbs_busi" in result.archive_filename
    tar_path = tmp_path / "archive_dir" / result.archive_filename
    assert tar_path.exists()


def test_pack_daily_uncompressed(tmp_path):
    """日度打包: 非压缩模式, 直接写目录。"""
    cat, obs = _bootstrap(tmp_path, with_metadata=False)
    p = _packer(tmp_path, obs, cat, compress=False)
    result = p.pack_daily("i1", "2026-06-01")
    assert result.archive_filename is not None
    assert not result.archive_filename.endswith(".tar.gz")
    dir_path = tmp_path / "archive_dir" / result.archive_filename
    assert dir_path.is_dir()
    assert (dir_path / "metadata.json").exists()


def test_pack_daily_preview_no_io(tmp_path):
    """日度 preview 不写盘。"""
    cat, obs = _bootstrap(tmp_path)
    p = _packer(tmp_path, obs, cat)
    result = p.pack_daily("i1", "2026-06-01", preview=True)
    assert result.preview is True
    assert result.archive_filename is None
    assert result.preview_text is not None
    assert "归档日期" in result.preview_text


def test_pack_daily_metadata_skipped(tmp_path):
    """日度打包跳过 metadata。"""
    cat, obs = _bootstrap(tmp_path, with_metadata=True)
    p = _packer(tmp_path, obs, cat)
    result = p.pack_daily("i1", "2026-06-01")
    assert result.metadata_skipped >= 1
