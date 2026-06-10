# tests/test_catalog.py
import sqlite3
from src.catalog import Catalog
from src.models import Policy


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
