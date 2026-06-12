# tests/test_catalog.py
import sqlite3
from datetime import datetime, timedelta
from src.catalog import Catalog
from src.models import BackupObject, DailyArchive, Policy
from src.errors import CatalogError


def test_init_creates_all_tables(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()

    with sqlite3.connect(str(tmp_catalog_path)) as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    expected = {
        "instance_mappings", "cluster_archive_policies", "backup_objects",
        "daily_archives", "pitr_chains", "restore_sessions",
        "restore_objects", "operation_log",
    }
    assert expected.issubset(tables)


def test_init_is_idempotent(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.init_schema()  # 不应报错
    assert tmp_catalog_path.exists()


def test_upsert_instance_and_policy(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()

    cat.upsert_instance(
        instance_id="2c61167d2f1f42858bc2a719a1275eae_in00aaaaaaaaaaaaaaaaaaaaaain00",
        alias="ncbs_busi",
        display_name="核心",
        description="core",
        bucket_name="b1",
        enabled=True,
    )
    cat.upsert_policy(
        instance_id="2c61167d2f1f42858bc2a719a1275eae_in00aaaaaaaaaaaaaaaaaaaaaain00",
        policy=Policy(True, True, True, True, retention_days=90,
                      xlog_redundancy_hours=6.0, xlog_forward_hours=6.0),
    )

    ins = cat.get_instance_by_alias("ncbs_busi")
    assert ins is not None
    assert ins["instance_id"] == "2c61167d2f1f42858bc2a719a1275eae_in00aaaaaaaaaaaaaaaaaaaaaain00"
    pol = cat.get_policy("2c61167d2f1f42858bc2a719a1275eae_in00aaaaaaaaaaaaaaaaaaaaaain00")
    assert pol.archive_xlog is True


def test_upsert_instance_is_idempotent(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()

    for _ in range(2):
        cat.upsert_instance("i1", "a1", "n", "", "b", True)
    rows = list(cat.list_enabled_instances())
    assert len(rows) == 1


def test_backup_object_upsert_and_transition(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)

    bo = BackupObject(
        obs_key="i1/Db/1780160839955/file_0.rch",
        instance_id="i1",
        obs_last_modified=datetime(2026, 6, 1, 0, 0, 0),
        backup_type="full",
        parent_backup_dir="1780160839955",
        backup_date="2026-06-01",
        obs_size_bytes=1024,
        backup_timestamp_ms=1780160839955,
        obs_etag="etag-abc",
    )
    cat.upsert_backup_object(bo)

    loaded = cat.get_backup_object_by_key("i1/Db/1780160839955/file_0.rch")
    assert loaded is not None
    assert loaded.status == "discovered"

    # 状态机推进
    cat.update_backup_object_status(loaded.id, "queued_for_archive")
    loaded2 = cat.get_backup_object_by_key("i1/Db/1780160839955/file_0.rch")
    assert loaded2.status == "queued_for_archive"


def test_list_pending_archives(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    for i, status in enumerate(["discovered", "queued_for_archive", "archived", "queued_for_archive"]):
        cat.upsert_backup_object(BackupObject(
            obs_key=f"i1/Db/1780160839955/file_{i}.rch",
            instance_id="i1",
            obs_last_modified=datetime(2026, 6, 1) + timedelta(hours=i),
            backup_type="full", parent_backup_dir=f"dir{i}",
            backup_date="2026-06-01", backup_timestamp_ms=1000 + i,
        ))
        if status != "discovered":
            cat.update_backup_object_status(
                cat.get_backup_object_by_key(f"i1/Db/1780160839955/file_{i}.rch").id,
                status,
            )

    pending = list(cat.list_backup_objects_by_status("queued_for_archive", instance_id="i1"))
    assert len(pending) == 2


def test_daily_archive_unique_per_instance_date(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)

    da = DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="a1_2026-06-09.tar.gz",
    )
    da_id = cat.upsert_daily_archive(da)

    # 重复插入应 ON CONFLICT 走更新
    da2 = DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="a1_2026-06-09.tar.gz",
        backup_count=10, total_size_bytes=2048,
    )
    cat.upsert_daily_archive(da2)
    loaded = cat.get_daily_archive(da_id)
    assert loaded.backup_count == 10


def test_daily_archive_attach_objects(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="a1_2026-06-09.tar.gz",
    ))

    bo = BackupObject(
        obs_key="i1/Db/1780160839955/f.rch", instance_id="i1",
        obs_last_modified=datetime(2026, 6, 9, 0, 0, 0),
        backup_type="full", parent_backup_dir="1780160839955",
        backup_date="2026-06-09", backup_timestamp_ms=1780160839955,
    )
    bo_id = cat.upsert_backup_object(bo)
    bo.id = bo_id
    cat.attach_object_to_daily_archive(bo, da_id)
    objs = list(cat.get_objects_by_daily_archive(da_id))
    assert len(objs) == 1


def test_daily_archive_status_transition(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="a1_2026-06-09.tar.gz",
    ))
    cat.update_daily_archive_status(da_id, "archived",
                                     checksum_sha256="abc123")
    loaded = cat.get_daily_archive(da_id)
    assert loaded.status == "archived"
    assert loaded.checksum_sha256 == "abc123"
    assert loaded.archived_at is not None


def test_list_pending_daily_archives(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    for d, st in [("2026-06-08", "pending"), ("2026-06-09", "archived"), ("2026-06-10", "pending")]:
        da_id = cat.upsert_daily_archive(DailyArchive(
            instance_id="i1", archive_date=d, archive_filename=f"a1_{d}.tar.gz",
        ))
        if st != "pending":
            cat.update_daily_archive_status(da_id, st)
    pending = list(cat.list_daily_archives_by_status("pending"))
    assert {p.archive_date for p in pending} == {"2026-06-08", "2026-06-10"}


def test_pitr_chain_crud(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)

    cat.upsert_pitr_chain(
        chain_id="i1_chain_dir1",
        instance_id="i1",
        base_full_dir="dir1",
        base_full_time=datetime(2026, 6, 1, 1, 0, 0),
        diff_dirs=["dir2", "dir3"],
        chain_start_time=datetime(2026, 6, 1, 1, 0, 0),
        chain_end_time=datetime(2026, 6, 8, 1, 0, 0),
    )
    found = cat.find_pitr_chain_at(instance_id="i1", target_time=datetime(2026, 6, 5))
    assert found is not None
    assert found["base_full_dir"] == "dir1"
    assert found["diff_count"] == 2


def test_restore_session_lifecycle(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()

    sid = "uuid-test"
    cat.create_restore_session(
        session_id=sid,
        target_time=datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[42, 43, 44],
        required_full_dir="1780160839955",
        required_diff_dirs=["1780177759671"],
    )
    s = cat.get_restore_session(sid)
    assert s["status"] == "retrieving"
    assert s["required_full_dir"] == "1780160839955"

    cat.update_restore_session_status(sid, "restored")
    s2 = cat.get_restore_session(sid)
    assert s2["status"] == "restored"
    assert s2["restored_at"] is not None


def test_restore_object_lifecycle(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="a1_2026-06-09.tar.gz",
    ))
    sid = "s1"
    cat.create_restore_session(
        session_id=sid, target_time=datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[da_id],
    )
    rs = cat.get_restore_session(sid)
    rid = cat.add_restore_object(
        restore_session_id=rs["id"], backup_object_id=None,
        daily_archive_id=da_id, bucket_name="b",
        obs_key="i1/Db/1780160839955/f.rch",
        object_size=1024, source_checksum="abc",
        restored_etag="etag-xyz",
        restored_last_modified="2026-06-10T10:00:00+00:00",
    )
    obj = cat.get_restore_object(rid)
    assert obj["uploaded_by_session"] == 1

    cat.mark_restore_object_cleaned(rid, note="ok")
    obj2 = cat.get_restore_object(rid)
    assert obj2["cleanup_status"] == "cleaned"


def test_list_restore_objects_for_session(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    da_id = cat.upsert_daily_archive(DailyArchive(
        instance_id="i1", archive_date="2026-06-09",
        archive_filename="a1_2026-06-09.tar.gz",
    ))
    sid = "s2"
    cat.create_restore_session(
        session_id=sid, target_time=datetime(2026, 6, 9, 14, 30),
        required_daily_archives=[da_id],
    )
    rs = cat.get_restore_session(sid)
    for i in range(3):
        cat.add_restore_object(
            restore_session_id=rs["id"], backup_object_id=None,
            daily_archive_id=da_id, bucket_name="b",
            obs_key=f"i1/Db/dir{i}/f.rch",
        )
    objs = list(cat.list_restore_objects_for_session(sid))
    assert len(objs) == 3
    assert all(o["uploaded_by_session"] == 1 for o in objs)
