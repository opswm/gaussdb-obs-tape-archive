"""SQLite Catalog：所有模块共享的运行时真相。
设计原则:
- 单例连接管理 (线程局部)
- schema 一次性 init, 后续操作仅用 prepared statements
- 所有写操作同时记录 operation_log
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable

from src.compat import datetime_fromisoformat

from src.errors import CatalogError
from src.models import BackupObject, DailyArchive, Policy


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
    week_start_day  INTEGER NOT NULL DEFAULT 6,
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
        'discovered','queued_for_archive','archived','obs_deleted')),
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
CREATE INDEX IF NOT EXISTS idx_bo_instance_type ON backup_objects(instance_id, backup_type);

CREATE TABLE IF NOT EXISTS daily_archives (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     TEXT NOT NULL,
    archive_date    TEXT NOT NULL,
    archive_week_end TEXT,
    archive_type    TEXT NOT NULL DEFAULT 'weekly' CHECK(archive_type IN ('daily','weekly')),
    archive_filename TEXT NOT NULL,
    backup_count    INTEGER NOT NULL DEFAULT 0,
    total_size_bytes BIGINT NOT NULL DEFAULT 0,
    compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    full_count      INTEGER NOT NULL DEFAULT 0,
    diff_count      INTEGER NOT NULL DEFAULT 0,
    snapshot_count  INTEGER NOT NULL DEFAULT 0,
    xlog_count      INTEGER NOT NULL DEFAULT 0,
    metadata_skipped_count INTEGER NOT NULL DEFAULT 0,
    full_dirs       TEXT,
    diff_dirs       TEXT,
    snapshot_dirs   TEXT,
    xlog_lsn_start  TEXT,
    xlog_lsn_end    TEXT,
    xlog_time_start TEXT,
    xlog_time_end   TEXT,
    checksum_sha256 TEXT,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK(status IN (
        'pending','archived')),
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    archived_at     TEXT,
    manifest_json   TEXT,
    UNIQUE(instance_id, archive_date),
    FOREIGN KEY (instance_id) REFERENCES instance_mappings(instance_id)
);

CREATE INDEX IF NOT EXISTS idx_da_instance_week ON daily_archives(instance_id, archive_date);

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

    def get_instance_by_id(self, instance_id: str) -> sqlite3.Row | None:
        """按 instance_id 查 instance (单条 SQL 查, 替代 3 处线性扫描 _bucket)。"""
        return self._conn().execute(
            "SELECT * FROM instance_mappings WHERE instance_id = ?", (instance_id,)
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
                        archive_xlog, retention_days, xlog_redundancy_hours,
                        xlog_forward_hours, week_start_day)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(instance_id) DO UPDATE SET
                       archive_full=excluded.archive_full,
                       archive_snapshot=excluded.archive_snapshot,
                       archive_diff=excluded.archive_diff,
                       archive_xlog=excluded.archive_xlog,
                       retention_days=excluded.retention_days,
                       xlog_redundancy_hours=excluded.xlog_redundancy_hours,
                       xlog_forward_hours=excluded.xlog_forward_hours,
                       week_start_day=excluded.week_start_day,
                       updated_at=datetime('now')""",
                (instance_id, int(policy.archive_full), int(policy.archive_snapshot),
                 int(policy.archive_diff), int(policy.archive_xlog),
                 policy.retention_days, policy.xlog_redundancy_hours,
                 policy.xlog_forward_hours, policy.week_start_day),
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
            week_start_day=r["week_start_day"],
        )

    # ─── backup_objects ───
    def upsert_backup_object(self, bo: BackupObject) -> int:
        with self.transaction() as c:
            # P1 修复: obs_last_modified 统一归一为 UTC ISO 带时区, 避免与
            # 链上带 TZ 的 base_full_time 字符串比较时 '+00:00' 与 naive 错位
            lm = bo.obs_last_modified
            if lm.tzinfo is None:
                from datetime import timezone as _tz
                lm = lm.replace(tzinfo=_tz.utc)
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
                 lm.isoformat(),
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
        valid = {"discovered", "queued_for_archive", "archived", "obs_deleted"}
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

    def list_backup_objects_weekly(
        self, instance_id: str, week_start_iso: str, week_end_iso: str,
    ) -> Iterable[BackupObject]:
        """返回本周窗口内的非元数据、非 archive_only 对象。
        - full/diff/snapshot: 按 parent_backup_dir 时间戳 (ts_ms) 落在 [week_start, week_end)
        - xlog: 按 obs_last_modified 落在 [week_start, week_end)
        - metadata / archive_only: 过滤掉 (callers 自己处理 skip 计数)
        """
        sql = """SELECT * FROM backup_objects
                 WHERE instance_id = ?
                   AND status != 'obs_deleted'
                   AND (restore_policy IS NULL OR restore_policy != 'archive_only')
                   AND (
                     (backup_type IN ('full','diff','snapshot')
                      AND backup_timestamp_ms IS NOT NULL
                      AND backup_timestamp_ms >= CAST(? AS BIGINT)
                      AND backup_timestamp_ms <  CAST(? AS BIGINT))
                     OR (backup_type = 'xlog'
                      AND obs_last_modified >= ?
                      AND obs_last_modified <  ?)
                   )
                 ORDER BY backup_type, parent_backup_dir, obs_key"""
        # week_start_iso / week_end_iso are ISO strings; ts_ms comparison needs ms
        from src.utils import ensure_utc_aware
        ws_dt = ensure_utc_aware(datetime_fromisoformat(week_start_iso))
        we_dt = ensure_utc_aware(datetime_fromisoformat(week_end_iso))
        ws_ms = int(ws_dt.timestamp() * 1000)
        we_ms = int(we_dt.timestamp() * 1000)
        params = [instance_id, str(ws_ms), str(we_ms),
                  week_start_iso, week_end_iso]
        for r in self._conn().execute(sql, params):
            yield self._row_to_bo(r)

    def list_backup_objects_daily(
        self, instance_id: str, archive_date: str,
    ) -> Iterable[BackupObject]:
        """返回指定日期窗口内的非元数据、非 archive_only 对象。
        - full/diff/snapshot: 按 backup_date = archive_date
        - xlog: 按 obs_last_modified 落在 [date 00:00, date+1 00:00) UTC
        - metadata / archive_only: 过滤掉
        """
        from datetime import timedelta
        from src.utils import ensure_utc_aware
        day_start = ensure_utc_aware(datetime_fromisoformat(f"{archive_date}T00:00:00+00:00"))
        day_end = day_start + timedelta(days=1)
        day_end_iso = day_end.isoformat()
        sql = """SELECT * FROM backup_objects
                 WHERE instance_id = ?
                   AND status != 'obs_deleted'
                   AND (restore_policy IS NULL OR restore_policy != 'archive_only')
                   AND (
                     (backup_type IN ('full','diff','snapshot')
                      AND backup_date = ?)
                     OR (backup_type = 'xlog'
                      AND obs_last_modified >= ?
                      AND obs_last_modified <  ?)
                   )
                 ORDER BY backup_type, parent_backup_dir, obs_key"""
        day_start_iso = day_start.isoformat()
        for r in self._conn().execute(sql, (instance_id, archive_date, day_start_iso, day_end_iso)):
            yield self._row_to_bo(r)

    def find_pending_daily_dates(
        self, instance_id: str,
    ) -> list[str]:
        """返回该实例所有有待归档对象 (queued_for_archive) 的日期列表, 去重排序。"""
        rows = self._conn().execute(
            """SELECT DISTINCT backup_date
               FROM backup_objects
               WHERE instance_id = ?
                 AND status = 'queued_for_archive'
               ORDER BY backup_date""",
            (instance_id,),
        ).fetchall()
        return [r["backup_date"] for r in rows]

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
            obs_last_modified=datetime_fromisoformat(r["obs_last_modified"]),
            backup_type=r["backup_type"], parent_backup_dir=r["parent_backup_dir"],
            restore_policy=r["restore_policy"], backup_date=r["backup_date"],
            backup_timestamp_ms=r["backup_timestamp_ms"],
            status=r["status"],
            daily_archive_id=r["daily_archive_id"],
            checksum_sha256=r["checksum_sha256"],
            verified_at=datetime_fromisoformat(r["verified_at"]) if r["verified_at"] else None,
            obs_deleted_at=datetime_fromisoformat(r["obs_deleted_at"]) if r["obs_deleted_at"] else None,
            obs_deleted_by=r["obs_deleted_by"],
            obs_etag=r["obs_etag"],
        )

    # ─── daily_archives ───
    def upsert_daily_archive(self, da: DailyArchive) -> int:
        with self.transaction() as c:
            cur = c.execute(
                """INSERT INTO daily_archives
                       (instance_id, archive_date, archive_week_end, archive_type,
                        archive_filename,
                        backup_count, total_size_bytes, compressed_size_bytes,
                        full_count, diff_count, snapshot_count, xlog_count,
                        metadata_skipped_count,
                        full_dirs, diff_dirs, snapshot_dirs,
                        xlog_lsn_start, xlog_lsn_end, xlog_time_start, xlog_time_end,
                        checksum_sha256, status, manifest_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(instance_id, archive_date) DO UPDATE SET
                       archive_week_end=excluded.archive_week_end,
                       archive_type=excluded.archive_type,
                       archive_filename=excluded.archive_filename,
                       backup_count=excluded.backup_count,
                       total_size_bytes=excluded.total_size_bytes,
                       compressed_size_bytes=excluded.compressed_size_bytes,
                       full_count=excluded.full_count, diff_count=excluded.diff_count,
                       snapshot_count=excluded.snapshot_count,
                       xlog_count=excluded.xlog_count,
                       metadata_skipped_count=excluded.metadata_skipped_count,
                       full_dirs=excluded.full_dirs, diff_dirs=excluded.diff_dirs,
                       snapshot_dirs=excluded.snapshot_dirs,
                       xlog_lsn_start=excluded.xlog_lsn_start,
                       xlog_lsn_end=excluded.xlog_lsn_end,
                       xlog_time_start=excluded.xlog_time_start,
                       xlog_time_end=excluded.xlog_time_end,
                       checksum_sha256=excluded.checksum_sha256,
                       status=excluded.status,
                       manifest_json=excluded.manifest_json""",
                (da.instance_id, da.archive_date, da.archive_week_end,
                 da.archive_type,
                 da.archive_filename,
                 da.backup_count, da.total_size_bytes, da.compressed_size_bytes,
                 da.full_count, da.diff_count, da.snapshot_count, da.xlog_count,
                 da.metadata_skipped_count,
                 da.full_dirs, da.diff_dirs, da.snapshot_dirs,
                 da.xlog_lsn_start, da.xlog_lsn_end,
                 da.xlog_time_start, da.xlog_time_end,
                 da.checksum_sha256, da.status, da.manifest_json),
            )
            if cur.lastrowid:
                return cur.lastrowid
            # ON CONFLICT 触发时 lastrowid 可能是 0，重新查
            r = c.execute(
                "SELECT id FROM daily_archives WHERE instance_id = ? AND archive_date = ?",
                (da.instance_id, da.archive_date),
            ).fetchone()
            return r["id"]

    def get_daily_archive(self, da_id: int) -> DailyArchive | None:
        r = self._conn().execute(
            "SELECT * FROM daily_archives WHERE id = ?", (da_id,)
        ).fetchone()
        return self._row_to_da(r) if r else None

    def list_daily_archives_by_status(
        self, status: str, instance_id: str | None = None,
    ) -> Iterable[DailyArchive]:
        sql = "SELECT * FROM daily_archives WHERE status = ?"
        params: list = [status]
        if instance_id:
            sql += " AND instance_id = ?"
            params.append(instance_id)
        sql += " ORDER BY archive_date"
        for r in self._conn().execute(sql, params).fetchall():
            yield self._row_to_da(r)

    def update_daily_archive_status(
        self, da_id: int, new_status: str,
        checksum_sha256: str | None = None,
    ) -> None:
        valid = {"pending", "archived"}
        if new_status not in valid:
            raise CatalogError(f"非法 daily_archive 状态: {new_status}")
        sets = ["status = ?"]
        params: list = [new_status]
        if new_status == "archived":
            sets.append("archived_at = datetime('now')")
        if checksum_sha256 is not None:
            sets.append("checksum_sha256 = ?"); params.append(checksum_sha256)
        params.append(da_id)
        with self.transaction() as c:
            c.execute(f"UPDATE daily_archives SET {', '.join(sets)} WHERE id = ?", params)

    def attach_object_to_daily_archive(self, bo: BackupObject, da_id: int) -> None:
        if bo.id is None:
            raise CatalogError("attach_object_to_daily_archive 要求 bo 已持久化")
        with self.transaction() as c:
            c.execute(
                "UPDATE backup_objects SET daily_archive_id = ?, updated_at = datetime('now') WHERE id = ?",
                (da_id, bo.id),
            )

    def get_objects_by_daily_archive(self, da_id: int) -> Iterable[BackupObject]:
        for r in self._conn().execute(
            "SELECT * FROM backup_objects WHERE daily_archive_id = ? ORDER BY backup_type, obs_key",
            (da_id,),
        ).fetchall():
            yield self._row_to_bo(r)

    def list_weekly_archives_in_range(
        self, instance_id: str, start_iso: str, end_iso: str,
    ) -> Iterable[DailyArchive]:
        """列出 (instance, archive_date) 落在 [start, end) 区间内的 weekly_archives。
        start/end 为 ISO date 字符串 'YYYY-MM-DD'。
        """
        for r in self._conn().execute(
            """SELECT * FROM daily_archives
               WHERE instance_id = ?
                 AND archive_date >= ? AND archive_date < ?
               ORDER BY archive_date""",
            (instance_id, start_iso, end_iso),
        ).fetchall():
            yield self._row_to_da(r)

    def _row_to_da(self, r: sqlite3.Row) -> DailyArchive:
        return DailyArchive(
            id=r["id"], instance_id=r["instance_id"], archive_date=r["archive_date"],
            archive_week_end=r["archive_week_end"],
            archive_type=r["archive_type"] if "archive_type" in r.keys() else "weekly",
            archive_filename=r["archive_filename"], backup_count=r["backup_count"],
            total_size_bytes=r["total_size_bytes"],
            compressed_size_bytes=r["compressed_size_bytes"],
            full_count=r["full_count"], diff_count=r["diff_count"],
            snapshot_count=r["snapshot_count"], xlog_count=r["xlog_count"],
            metadata_skipped_count=r["metadata_skipped_count"],
            full_dirs=r["full_dirs"] or "[]", diff_dirs=r["diff_dirs"] or "[]",
            snapshot_dirs=r["snapshot_dirs"] or "[]",
            xlog_lsn_start=r["xlog_lsn_start"], xlog_lsn_end=r["xlog_lsn_end"],
            xlog_time_start=r["xlog_time_start"], xlog_time_end=r["xlog_time_end"],
            checksum_sha256=r["checksum_sha256"], status=r["status"],
            created_at=datetime_fromisoformat(r["created_at"]) if r["created_at"] else None,
            archived_at=datetime_fromisoformat(r["archived_at"]) if r["archived_at"] else None,
            manifest_json=r["manifest_json"],
        )

    # ─── pitr_chains ───
    def upsert_pitr_chain(
        self, chain_id: str, instance_id: str,
        base_full_dir: str, base_full_time: datetime,
        diff_dirs: list[str], chain_start_time: datetime,
        chain_end_time: datetime | None = None,
        next_chain_id: str | None = None,
    ) -> None:
        with self.transaction() as c:
            c.execute(
                """INSERT INTO pitr_chains
                       (chain_id, instance_id, base_full_dir, base_full_time,
                        diff_dirs, diff_count, next_chain_id,
                        chain_start_time, chain_end_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(chain_id) DO UPDATE SET
                       diff_dirs=excluded.diff_dirs, diff_count=excluded.diff_count,
                       next_chain_id=excluded.next_chain_id,
                       chain_end_time=excluded.chain_end_time""",
                (chain_id, instance_id, base_full_dir,
                 base_full_time.isoformat(),
                 json.dumps(diff_dirs), len(diff_dirs), next_chain_id,
                 chain_start_time.isoformat(),
                 chain_end_time.isoformat() if chain_end_time else None),
            )

    def find_pitr_chain_at(
        self, instance_id: str, target_time: datetime,
    ) -> sqlite3.Row | None:
        return self._conn().execute(
            """SELECT * FROM pitr_chains
               WHERE instance_id = ?
                 AND chain_start_time <= ?
                 AND (chain_end_time IS NULL OR chain_end_time >= ?)
               ORDER BY chain_start_time DESC LIMIT 1""",
            (instance_id, target_time.isoformat(), target_time.isoformat()),
        ).fetchone()

    # ─── restore_sessions ───
    def create_restore_session(
        self, session_id: str, target_time: datetime,
        required_daily_archives: list[int],
        required_full_dir: str | None = None,
        required_diff_dirs: list[str] | None = None,
        xlog_redundancy_hours: float = 6.0,
        xlog_forward_hours: float = 6.0,
    ) -> int:
        with self.transaction() as c:
            cur = c.execute(
                """INSERT INTO restore_sessions
                       (session_id, target_time, required_daily_archives,
                        required_full_dir, required_diff_dirs,
                        xlog_redundancy_hours, xlog_forward_hours)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (session_id, target_time.isoformat(),
                 json.dumps(required_daily_archives),
                 required_full_dir,
                 json.dumps(required_diff_dirs or []),
                 xlog_redundancy_hours, xlog_forward_hours),
            )
            return cur.lastrowid

    def get_restore_session(self, session_id: str) -> sqlite3.Row | None:
        return self._conn().execute(
            "SELECT * FROM restore_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()

    def update_restore_session_status(
        self, session_id: str, new_status: str,
        error_message: str | None = None,
    ) -> None:
        valid = {"retrieving", "extracting", "uploading", "restored",
                 "cleaning", "cleaned", "failed"}
        if new_status not in valid:
            raise CatalogError(f"非法 restore_session 状态: {new_status}")
        ts_col = {
            "restored": "restored_at", "cleaned": "cleaned_at",
            "retrieving": "retrieved_at",
        }.get(new_status)
        with self.transaction() as c:
            if ts_col:
                c.execute(
                    f"""UPDATE restore_sessions
                        SET status = ?, {ts_col} = datetime('now'),
                            error_message = ?
                        WHERE session_id = ?""",
                    (new_status, error_message, session_id),
                )
            else:
                c.execute(
                    "UPDATE restore_sessions SET status = ?, error_message = ? WHERE session_id = ?",
                    (new_status, error_message, session_id),
                )

    # ─── restore_objects ───
    def add_restore_object(
        self, restore_session_id: int, backup_object_id: int | None,
        daily_archive_id: int | None, bucket_name: str, obs_key: str,
        object_size: int | None = None, source_checksum: str | None = None,
        restored_etag: str | None = None,
        restored_last_modified: str | None = None,
    ) -> int:
        with self.transaction() as c:
            cur = c.execute(
                """INSERT INTO restore_objects
                       (restore_session_id, backup_object_id, daily_archive_id,
                        bucket_name, obs_key, object_size, source_checksum,
                        restored_etag, restored_last_modified)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (restore_session_id, backup_object_id, daily_archive_id,
                 bucket_name, obs_key, object_size, source_checksum,
                 restored_etag, restored_last_modified),
            )
            return cur.lastrowid

    def get_restore_object(self, rid: int) -> sqlite3.Row | None:
        return self._conn().execute(
            "SELECT * FROM restore_objects WHERE id = ?", (rid,)
        ).fetchone()

    def list_restore_objects_for_session(
        self, session_id: str, cleanup_status: str | None = None,
    ) -> Iterable[sqlite3.Row]:
        sql = """SELECT ro.* FROM restore_objects ro
                 JOIN restore_sessions rs ON ro.restore_session_id = rs.id
                 WHERE rs.session_id = ?"""
        params: list = [session_id]
        if cleanup_status:
            sql += " AND ro.cleanup_status = ?"
            params.append(cleanup_status)
        sql += " ORDER BY ro.id"
        yield from self._conn().execute(sql, params).fetchall()

    def mark_restore_object_cleaned(
        self, rid: int, note: str = "ok",
    ) -> None:
        with self.transaction() as c:
            c.execute(
                """UPDATE restore_objects
                   SET cleanup_status = 'cleaned', cleaned_at = datetime('now'),
                       error_message = ?
                   WHERE id = ?""",
                (note, rid),
            )

    def update_restore_object_status(
        self, rid: int, restore_status: str,
        restored_etag: str | None = None,
        restored_last_modified: str | None = None,
    ) -> None:
        with self.transaction() as c:
            sets = ["restore_status = ?", "uploaded_at = datetime('now')"]
            params: list = [restore_status]
            if restored_etag:
                sets.append("restored_etag = ?"); params.append(restored_etag)
            if restored_last_modified:
                sets.append("restored_last_modified = ?")
                params.append(restored_last_modified)
            params.append(rid)
            c.execute(
                f"UPDATE restore_objects SET {', '.join(sets)} WHERE id = ?",
                params,
            )
