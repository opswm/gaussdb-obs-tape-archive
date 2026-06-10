# tests/test_catalog.py
import sqlite3
from datetime import datetime, timedelta
from src.catalog import Catalog
from src.models import BackupObject, DailyArchive, Policy


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
    cat.update_daily_archive_status(da_id, "writing", tape_volume="TAPE001", tape_position=0)
    cat.update_daily_archive_status(da_id, "on_tape")
    loaded = cat.get_daily_archive(da_id)
    assert loaded.status == "on_tape"
    assert loaded.tape_volume == "TAPE001"


def test_list_pending_daily_archives(tmp_catalog_path):
    cat = Catalog(str(tmp_catalog_path))
    cat.init_schema()
    cat.upsert_instance("i1", "a1", "n", "", "b", True)
    for d, st in [("2026-06-08", "pending"), ("2026-06-09", "on_tape"), ("2026-06-10", "pending")]:
        da_id = cat.upsert_daily_archive(DailyArchive(
            instance_id="i1", archive_date=d, archive_filename=f"a1_{d}.tar.gz",
        ))
        if st != "pending":
            cat.update_daily_archive_status(da_id, st)
    pending = list(cat.list_daily_archives_by_status("pending"))
    assert {p.archive_date for p in pending} == {"2026-06-08", "2026-06-10"}
