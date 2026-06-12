"""数据模型 DTO。所有跨模块传递的实体都用 dataclass，不传裸 tuple/dict。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


# ─── 备份对象状态机 ───
BackupObjectStatus = Literal[
    "discovered", "queued_for_archive",
    "archived", "obs_deleted",
]
BackupType = Literal["full", "diff", "snapshot", "xlog", "metadata"]
RestorePolicy = Literal["normal", "archive_only"]

# ─── 周度归档状态 (二态, 与 SQL CHECK 对齐) ───
DailyArchiveStatus = Literal["pending", "archived"]

# ─── 恢复会话状态 ───
RestoreSessionStatus = Literal[
    "retrieving", "extracting", "uploading",
    "restored", "cleaning", "cleaned", "failed",
]


@dataclass(frozen=True)
class Policy:
    """集群级转储策略，对应 cluster_archive_policies 一行。"""
    archive_full: bool
    archive_snapshot: bool
    archive_diff: bool
    archive_xlog: bool
    retention_days: int = 90
    xlog_redundancy_hours: float = 6.0
    xlog_forward_hours: float = 6.0
    # 周度归档起点日 (1=周一..7=周日), 默认 6=周六
    week_start_day: int = 6

    def is_full_pitr_capable(self) -> bool:
        """完整 PITR 能力：full + diff + xlog 全开。"""
        return self.archive_full and self.archive_diff and self.archive_xlog


@dataclass
class InstanceMapping:
    instance_id: str
    alias: str
    display_name: str
    bucket_name: str
    description: str = ""
    enabled: bool = True


@dataclass
class BackupObject:
    obs_key: str
    instance_id: str
    obs_last_modified: datetime
    backup_type: BackupType
    parent_backup_dir: str
    backup_date: str
    obs_size_bytes: int = 0
    restore_policy: RestorePolicy = "normal"
    backup_timestamp_ms: int | None = None
    status: BackupObjectStatus = "discovered"
    id: int | None = None
    daily_archive_id: int | None = None
    checksum_sha256: str | None = None
    verified_at: datetime | None = None
    obs_deleted_at: datetime | None = None
    obs_deleted_by: str | None = None
    obs_etag: str | None = None  # 扫描时记录的 ETag，供 Reaper 二次校验
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass
class DailyArchive:
    instance_id: str
    archive_date: str
    archive_filename: str
    archive_week_end: str | None = None
    backup_count: int = 0
    total_size_bytes: int = 0
    compressed_size_bytes: int = 0
    full_count: int = 0
    diff_count: int = 0
    snapshot_count: int = 0
    xlog_count: int = 0
    metadata_skipped_count: int = 0
    full_dirs: str = "[]"
    diff_dirs: str = "[]"
    snapshot_dirs: str = "[]"
    xlog_lsn_start: str | None = None
    xlog_lsn_end: str | None = None
    xlog_time_start: str | None = None
    xlog_time_end: str | None = None
    checksum_sha256: str | None = None
    status: DailyArchiveStatus = "pending"
    created_at: datetime | None = None
    archived_at: datetime | None = None
    manifest_json: str | None = None
    id: int | None = None


@dataclass
class RestoreSession:
    session_id: str
    target_time: datetime
    required_daily_archives: str  # JSON list
    required_full_dir: str | None = None
    required_diff_dirs: str | None = None
    xlog_redundancy_hours: float = 6.0
    xlog_forward_hours: float = 6.0
    status: RestoreSessionStatus = "retrieving"
    id: int | None = None
    created_at: datetime | None = None
    retrieved_at: datetime | None = None
    restored_at: datetime | None = None
    cleaned_at: datetime | None = None
    error_message: str | None = None
