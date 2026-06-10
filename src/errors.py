"""自定义异常层级。所有项目内异常都应继承 ArchiveError 或其子类。"""
from __future__ import annotations


class ArchiveError(Exception):
    """所有项目内异常的根。"""


class ConfigError(ArchiveError):
    """配置加载或校验失败。"""


class InvalidArchivePolicyError(ArchiveError):
    """违反 1.4.2.1 策略依赖约束。"""


class CatalogError(ArchiveError):
    """SQLite 读写或约束冲突。"""


class ObsError(ArchiveError):
    """OBS API 调用失败。"""


class TapeWriteError(ArchiveError):
    """磁带写入或回读校验失败。"""

    def __init__(self, msg: str, tape_position: int | None = None) -> None:
        super().__init__(msg)
        self.tape_position = tape_position


class UnsafeDeleteError(ArchiveError):
    """Reaper 6 道门禁任一未通过。"""


class RestoreError(ArchiveError):
    """Restorer 流程失败。"""


class CleanupSafetyError(RestoreError):
    """Cleaner 安全门禁未通过（按 restore_objects 清理时）。"""
