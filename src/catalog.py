"""SQLite Catalog：所有模块共享的运行时真相。
设计原则:
- 单例连接管理 (线程局部)
- schema 一次性 init, 后续操作仅用 prepared statements
- 所有写操作同时记录 operation_log
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable

from src.errors import CatalogError
from src.models import BackupObject, Policy


# 8 张表 + 索引，与设计稿 2.1 节完全一致
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS instance_mappings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT NOT NULL UNIQUE,
    alias           TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    description     TEXT,
    bucket_name     TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS cluster_archive_policies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT NOT NULL UNIQUE,
    archive_full    INTEGER NOT NULL DEFAULT 1,
    archive_snapshot INTEGER NOT NULL DEFAULT 1,
    archive_diff    INTEGER NOT NULL DEFAULT 1,
    archive_xlog    INTEGER NOT NULL DEFAULT 1,
    retention_days  INTEGER NOT NULL DEFAULT 90,
    xlog_redundancy_hours REAL NOT NULL DEFAULT 6.0,
    xlog_forward_hours REAL NOT NULL DEFAULT 6.0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES instance_mappings(instance_id)
);

CREATE TABLE IF NOT EXISTS backup_objects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    obs_key         TEXT NOT NULL UNIQUE,
    instance_id     TEXT NOT NULL,
    obs_size_bytes  BIGINT NOT NULL DEFAULT 0,
    obs_last_modified TEXT NOT NULL,
    backup_type     TEXT NOT NULL CHECK(backup_type IN ('full','diff','snapshot','xlog','metadata')),
    parent_backup_dir TEXT NOT NULL,
    restore_policy  TEXT NOT NULL DEFAULT 'normal' CHECK(restore_policy IN ('normal','archive_only')),
    backup_date     TEXT NOT NULL,
    backup_timestamp_ms BIGINT,
    status          TEXT NOT NULL DEFAULT 'discovered' CHECK(status IN (
        'discovered','queued_for_archive','archiving','archived','obs_deleted')),
    tape_volume     TEXT,
    tape_position   BIGINT,
    daily_archive_id INTEGER,
    checksum_sha256 TEXT,
    verified_at     TEXT,
    obs_deleted_at  TEXT,
    obs_deleted_by  TEXT,
    obs_etag        TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES instance_mappings(instance_id)
);

CREATE INDEX IF NOT EXISTS idx_bo_status ON backup_objects(status);
CREATE INDEX IF NOT EXISTS idx_bo_backup_date ON backup_objects(backup_date);
CREATE INDEX IF NOT EXISTS idx_bo_type ON backup_objects(backup_type);
CREATE INDEX IF NOT EXISTS idx_bo_parent_dir ON backup_objects(parent_backup_dir);
CREATE INDEX IF NOT EXISTS idx_bo_daily_archive ON backup_objects(daily_archive_id);
CREATE INDEX IF NOT EXISTS idx_bo_instance ON backup_objects(instance_id);
CREATE INDEX IF NOT EXISTS idx_bo_instance_date ON backup_objects(instance_id, backup_date);

CREATE TABLE IF NOT EXISTS daily_archives (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT NOT NULL,
    archive_date    TEXT NOT NULL,
    archive_filename TEXT NOT NULL,
    backup_count    INTEGER NOT NULL DEFAULT 0,
    total_size_bytes BIGINT NOT NULL DEFAULT 0,
    compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    full_count      INTEGER NOT NULL DEFAULT 0,
    diff_count      INTEGER NOT NULL DEFAULT 0,
    snapshot_count  INTEGER NOT NULL DEFAULT 0,
    xlog_count      INTEGER NOT NULL DEFAULT 0,
    full_dirs       TEXT,
    diff_dirs       TEXT,
    snapshot_dirs   TEXT,
    xlog_lsn_start  TEXT,
    xlog_lsn_end    TEXT,
    xlog_time_start TEXT,
    xlog_time_end   TEXT,
    tape_volume     TEXT,
    tape_position   BIGINT,
    checksum_sha256 TEXT,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
        'pending','writing','on_tape','deleted')),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    tape_written_at TEXT,
    manifest_json   TEXT,
    UNIQUE(instance_id, archive_date),
    FOREIGN KEY (instance_id) REFERENCES instance_mappings(instance_id)
);

CREATE TABLE IF NOT EXISTS pitr_chains (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT NOT NULL,
    chain_id        TEXT NOT NULL UNIQUE,
    base_full_dir   TEXT NOT NULL,
    base_full_time  TEXT NOT NULL,
    diff_dirs       TEXT NOT NULL DEFAULT '[]',
    diff_count      INTEGER NOT NULL DEFAULT 0,
    next_chain_id   TEXT,
    chain_start_time TEXT NOT NULL,
    chain_end_time  TEXT,
    xlog_start_lsn  TEXT,
    xlog_end_lsn    TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES instance_mappings(instance_id)
);

CREATE TABLE IF NOT EXISTS restore_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL UNIQUE,
    target_time     TEXT NOT NULL,
    required_daily_archives TEXT NOT NULL,
    required_full_dir  TEXT,
    required_diff_dirs TEXT,
    xlog_redundancy_hours REAL NOT NULL DEFAULT 6.0,
    xlog_forward_hours REAL NOT NULL DEFAULT 6.0,
    status          TEXT NOT NULL DEFAULT 'retrieving' CHECK(status IN (
        'retrieving','extracting','uploading','restored','cleaning','cleaned','failed')),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    retrieved_at    TEXT,
    restored_at     TEXT,
    cleaned_at      TEXT,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS restore_objects (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    restore_session_id  INTEGER NOT NULL,
    backup_object_id    INTEGER,
    daily_archive_id    INTEGER,
    bucket_name         TEXT NOT NULL,
    obs_key             TEXT NOT NULL,
    object_size         INTEGER,
    source_checksum     TEXT,
    restored_etag       TEXT,
    restored_last_modified TEXT,
    restore_status      TEXT NOT NULL DEFAULT 'pending' CHECK(restore_status IN (
        'pending','extracting','uploading','uploaded','verified','failed')),
    cleanup_status      TEXT NOT NULL DEFAULT 'not_cleaned' CHECK(cleanup_status IN (
        'not_cleaned','cleaning','cleaned','failed','skipped')),
    overwrite_checked   INTEGER NOT NULL DEFAULT 0,
    uploaded_by_session INTEGER NOT NULL DEFAULT 1,
    error_message       TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    uploaded_at         TEXT,
    verified_at         TEXT,
    cleaned_at          TEXT,
    FOREIGN KEY (restore_session_id) REFERENCES restore_sessions(id),
    FOREIGN KEY (backup_object_id) REFERENCES backup_objects(id),
    FOREIGN KEY (daily_archive_id) REFERENCES daily_archives(id)
);

CREATE INDEX IF NOT EXISTS idx_restore_objects_session ON restore_objects(restore_session_id);
CREATE INDEX IF NOT EXISTS idx_restore_objects_cleanup ON restore_objects(cleanup_status);
CREATE INDEX IF NOT EXISTS idx_restore_objects_key ON restore_objects(bucket_name, obs_key);

CREATE TABLE IF NOT EXISTS operation_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    operation       TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    target          TEXT,
    detail          TEXT,
    status          TEXT NOT NULL DEFAULT 'success',
    error_message   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_oplog_run ON operation_log(run_id);
CREATE INDEX IF NOT EXISTS idx_oplog_op ON operation_log(operation);
"""


class Catalog:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._local = threading.local()

    def _conn(self) -> sqlite3.Connection:
        """线程局部连接 (sqlite3 默认连接不可跨线程)。"""
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(str(self.path), isolation_level=None, timeout=30.0)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA foreign_keys=ON")
            c.row_factory = sqlite3.Row
            self._local.conn = c
        return c

    @contextmanager
    def transaction(self):
        """显式事务上下文。"""
        c = self._conn()
        c.execute("BEGIN")
        try:
            yield c
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise

    def init_schema(self) -> None:
        # NOTE: executescript() auto-commits any pending transaction, so we
        # intentionally do NOT wrap it in self.transaction(); the script is
        # idempotent (CREATE TABLE/INDEX IF NOT EXISTS) and any partial
        # failure leaves the DB in a consistent state.
        try:
            self._conn().executescript(SCHEMA_SQL)
        except sqlite3.Error as e:
            raise CatalogError(f"Catalog schema init 失败: {e}") from e

    def log_operation(
        self, operation: str, run_id: str | None = None,
        target: str | None = None, detail: str | None = None,
        status: str = "success", error_message: str | None = None,
    ) -> int:
        if run_id is None:
            run_id = str(uuid.uuid4())
        with self.transaction() as c:
            cur = c.execute(
                """INSERT INTO operation_log (operation, run_id, target, detail, status, error_message)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (operation, run_id, target, detail, status, error_message),
            )
            return cur.lastrowid

    # ─── instance_mappings ───
    def upsert_instance(
        self, instance_id: str, alias: str, display_name: str,
        description: str, bucket_name: str, enabled: bool,
    ) -> None:
        with self.transaction() as c:
            c.execute(
                """INSERT INTO instance_mappings
                       (instance_id, alias, display_name, description, bucket_name, enabled)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(instance_id) DO UPDATE SET
                       alias=excluded.alias, display_name=excluded.display_name,
                       description=excluded.description, bucket_name=excluded.bucket_name,
                       enabled=excluded.enabled, updated_at=datetime('now')""",
                (instance_id, alias, display_name, description, bucket_name, int(enabled)),
            )

    def get_instance_by_alias(self, alias: str) -> sqlite3.Row | None:
        return self._conn().execute(
            "SELECT * FROM instance_mappings WHERE alias = ?", (alias,)
        ).fetchone()

    def list_enabled_instances(self) -> Iterable[sqlite3.Row]:
        return self._conn().execute(
            "SELECT * FROM instance_mappings WHERE enabled = 1 ORDER BY alias"
        ).fetchall()

    # ─── cluster_archive_policies ───
    def upsert_policy(self, instance_id: str, policy: Policy) -> None:
        with self.transaction() as c:
            c.execute(
                """INSERT INTO cluster_archive_policies
                       (instance_id, archive_full, archive_snapshot, archive_diff,
                        archive_xlog, retention_days, xlog_redundancy_hours, xlog_forward_hours)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(instance_id) DO UPDATE SET
                       archive_full=excluded.archive_full,
                       archive_snapshot=excluded.archive_snapshot,
                       archive_diff=excluded.archive_diff,
                       archive_xlog=excluded.archive_xlog,
                       retention_days=excluded.retention_days,
                       xlog_redundancy_hours=excluded.xlog_redundancy_hours,
                       xlog_forward_hours=excluded.xlog_forward_hours,
                       updated_at=datetime('now')""",
                (instance_id, int(policy.archive_full), int(policy.archive_snapshot),
                 int(policy.archive_diff), int(policy.archive_xlog),
                 policy.retention_days, policy.xlog_redundancy_hours,
                 policy.xlog_forward_hours),
            )

    def get_policy(self, instance_id: str) -> Policy:
        r = self._conn().execute(
            "SELECT * FROM cluster_archive_policies WHERE instance_id = ?",
            (instance_id,),
        ).fetchone()
        if r is None:
            raise CatalogError(f"集群 {instance_id} 无策略记录")
        return Policy(
            archive_full=bool(r["archive_full"]),
            archive_snapshot=bool(r["archive_snapshot"]),
            archive_diff=bool(r["archive_diff"]),
            archive_xlog=bool(r["archive_xlog"]),
            retention_days=r["retention_days"],
            xlog_redundancy_hours=r["xlog_redundancy_hours"],
            xlog_forward_hours=r["xlog_forward_hours"],
        )

    # ─── backup_objects ───
    def upsert_backup_object(self, bo: BackupObject) -> int:
        with self.transaction() as c:
            cur = c.execute(
                """INSERT INTO backup_objects
                       (obs_key, instance_id, obs_size_bytes, obs_last_modified,
                        backup_type, parent_backup_dir, restore_policy,
                        backup_date, backup_timestamp_ms, status, obs_etag)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(obs_key) DO UPDATE SET
                       obs_size_bytes=excluded.obs_size_bytes,
                       obs_last_modified=excluded.obs_last_modified,
                       obs_etag=excluded.obs_etag,
                       updated_at=datetime('now')""",
                (bo.obs_key, bo.instance_id, bo.obs_size_bytes,
                 bo.obs_last_modified.isoformat(),
                 bo.backup_type, bo.parent_backup_dir, bo.restore_policy,
                 bo.backup_date, bo.backup_timestamp_ms, bo.status, bo.obs_etag),
            )
            return cur.lastrowid

    def get_backup_object_by_key(self, obs_key: str) -> BackupObject | None:
        r = self._conn().execute(
            "SELECT * FROM backup_objects WHERE obs_key = ?", (obs_key,)
        ).fetchone()
        return self._row_to_bo(r) if r else None

    def update_backup_object_status(self, bo_id: int, new_status: str) -> None:
        # 校验状态机合法性
        valid = {"discovered", "queued_for_archive", "archiving",
                 "archived", "obs_deleted"}
        if new_status not in valid:
            raise CatalogError(f"非法 backup_object 状态: {new_status}")
        with self.transaction() as c:
            c.execute(
                "UPDATE backup_objects SET status = ?, updated_at = datetime('now') WHERE id = ?",
                (new_status, bo_id),
            )

    def list_backup_objects_by_status(
        self, status: str, instance_id: str | None = None,
    ) -> Iterable[BackupObject]:
        sql = "SELECT * FROM backup_objects WHERE status = ?"
        params: list = [status]
        if instance_id:
            sql += " AND instance_id = ?"
            params.append(instance_id)
        sql += " ORDER BY backup_date, obs_key"
        for r in self._conn().execute(sql, params):
            yield self._row_to_bo(r)

    def set_backup_object_tape(
        self, bo_id: int, tape_volume: str, tape_position: int,
        checksum: str,
    ) -> None:
        with self.transaction() as c:
            c.execute(
                """UPDATE backup_objects
                   SET tape_volume = ?, tape_position = ?, checksum_sha256 = ?,
                       verified_at = datetime('now'), updated_at = datetime('now')
                   WHERE id = ?""",
                (tape_volume, tape_position, checksum, bo_id),
            )

    def mark_backup_object_obs_deleted(self, bo_id: int, run_id: str) -> None:
        with self.transaction() as c:
            c.execute(
                """UPDATE backup_objects
                   SET status = 'obs_deleted', obs_deleted_at = datetime('now'),
                       obs_deleted_by = ?, updated_at = datetime('now')
                   WHERE id = ?""",
                (run_id, bo_id),
            )

    def get_backup_object(self, bo_id: int) -> BackupObject | None:
        r = self._conn().execute(
            "SELECT * FROM backup_objects WHERE id = ?", (bo_id,)
        ).fetchone()
        return self._row_to_bo(r) if r else None

    def _row_to_bo(self, r: sqlite3.Row) -> BackupObject:
        return BackupObject(
            id=r["id"], obs_key=r["obs_key"], instance_id=r["instance_id"],
            obs_size_bytes=r["obs_size_bytes"],
            obs_last_modified=datetime.fromisoformat(r["obs_last_modified"]),
            backup_type=r["backup_type"], parent_backup_dir=r["parent_backup_dir"],
            restore_policy=r["restore_policy"], backup_date=r["backup_date"],
            backup_timestamp_ms=r["backup_timestamp_ms"],
            status=r["status"], tape_volume=r["tape_volume"],
            tape_position=r["tape_position"],
            daily_archive_id=r["daily_archive_id"],
            checksum_sha256=r["checksum_sha256"],
            verified_at=datetime.fromisoformat(r["verified_at"]) if r["verified_at"] else None,
            obs_deleted_at=datetime.fromisoformat(r["obs_deleted_at"]) if r["obs_deleted_at"] else None,
            obs_deleted_by=r["obs_deleted_by"],
            obs_etag=r["obs_etag"],
        )
