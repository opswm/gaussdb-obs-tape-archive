"""自定义异常层级。所有项目内异常都应继承 ArchiveError 或其子类。"""
from __future__ import annotations


class ArchiveError(Exception):
    """所有项目内异常的根。"""


class ConfigError(ArchiveError):
    """配置加载或校验失败。"""


class InvalidArchivePolicyError(ArchiveError):
    """违反 1.4.2.1 策略依赖约束。"""


class InvalidWeekStartDayError(InvalidArchivePolicyError):
    """week_start_day 字段非法 (非 1-7)。"""


class ArchiveDirNotFoundError(ArchiveError):
    """archive_dir 配置的目录不存在或不可访问。"""


class CatalogError(ArchiveError):
    """SQLite 读写或约束冲突。"""


class ObsError(ArchiveError):
    """OBS API 调用失败。"""


class UnsafeDeleteError(ArchiveError):
    """Reaper 5 道门禁任一未通过。"""


class RestoreError(ArchiveError):
    """Restorer 流程失败。"""


class SnapshotNotFoundError(RestoreError):
    """Snapshot 独立恢复找不到对应 daily_archive 或未 archived。"""


class PitrNotCapableError(RestoreError):
    """实例策略不支持 PITR (缺 full / diff / xlog)。"""


class CleanupSafetyError(RestoreError):
    """Cleaner 安全门禁未通过（按 restore_objects 清理时）。"""
