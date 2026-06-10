"""Cleaner: 严格按 restore_objects 清单逐对象清理 OBS 恢复数据。

门禁:
1. session 状态: 不能是 cleaning/cleaned
2. restore_objects 不能为空 (拒绝"误清理整个桶"型事故)
3. uploaded_by_session == 1 (必须是本 session 写入, 拒绝删其他 session 数据)
4. backup_objects.status == 'obs_deleted' (拒绝删现网有效备份)
5. ETag 二次校验 (与 restore_objects.restored_etag 比对, 防止 OBS 数据被覆盖)
"""
from __future__ import annotations

from dataclasses import dataclass, field

from src.catalog import Catalog
from src.errors import CleanupSafetyError
from src.obs_client import ObsClient


@dataclass
class CleanupSummary:
    deleted: int = 0
    skipped: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)


class Cleaner:
    def __init__(self, obs: ObsClient, catalog: Catalog) -> None:
        self.obs = obs
        self.catalog = catalog

    def cleanup(self, session_id: str) -> CleanupSummary:
        sess = self.catalog.get_restore_session(session_id)
        if sess is None:
            raise CleanupSafetyError(f"session {session_id} 不存在")
        if sess["status"] in ("cleaning", "cleaned"):
            raise CleanupSafetyError(
                f"Session {session_id} 已经清理过, 状态: {sess['status']}")

        objs = list(self.catalog.list_restore_objects_for_session(session_id))
        if not objs:
            raise CleanupSafetyError(
                f"Session {session_id} 没有 restore_objects 清单, 拒绝清理")

        self.catalog.update_restore_session_status(session_id, "cleaning")
        summary = CleanupSummary()

        for ro in objs:
            if ro["uploaded_by_session"] != 1:
                raise CleanupSafetyError(
                    f"对象 {ro['obs_key']} 非本 session 写入, 拒绝清理")

            if ro["cleanup_status"] == "cleaned":
                summary.skipped += 1
                continue

            bo = (
                self.catalog.get_backup_object(ro["backup_object_id"])
                if ro["backup_object_id"] else None
            )
            if bo and bo.status != "obs_deleted":
                raise CleanupSafetyError(
                    f"对象 {ro['obs_key']} 在 backup_objects 中状态为 {bo.status}, "
                    f"非 obs_deleted, 拒绝清理 (可能是现网有效备份)")

            meta = self.obs.get_object_metadata(ro["bucket_name"], ro["obs_key"])
            if meta.not_found:
                self.catalog.mark_restore_object_cleaned(
                    ro["id"], note="already_not_found")
                summary.skipped += 1
                continue

            if (ro["restored_etag"] and meta.etag
                    and meta.etag != ro["restored_etag"]):
                raise CleanupSafetyError(
                    f"对象 {ro['obs_key']} ETag 已变化 "
                    f"({meta.etag} vs {ro['restored_etag']}), 拒绝删除")

            try:
                self.obs.delete_object(ro["bucket_name"], ro["obs_key"])
                self.catalog.mark_restore_object_cleaned(ro["id"], note="ok")
                summary.deleted += 1
            except Exception as e:
                summary.failed.append((ro["obs_key"], str(e)))

        self.catalog.update_restore_session_status(session_id, "cleaned")
        self.catalog.log_operation(
            operation="cleanup", run_id=None,
            target=f"session:{session_id}",
            detail=(f'{{"deleted":{summary.deleted},'
                    f'"skipped":{summary.skipped},'
                    f'"failed":{summary.failed}}}'),
        )
        return summary
