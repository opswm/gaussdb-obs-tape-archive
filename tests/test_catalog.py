# tests/test_catalog.py
import sqlite3
from src.catalog import Catalog


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
