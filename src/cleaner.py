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
            # P0 修复: 5 道门禁任一失败 → 跳过 + 记 failed, 不再 raise 中断整 loop
            # 之前 raise 会让 session 状态永远卡 "cleaning" (line 35 的二次清理拒绝)
            # 且 summary.failed 因 loop 提前终止而仅记录极少数 (dead code)
            verdict = self._check_gates(ro)
            if verdict == "skipped":
                summary.skipped += 1
                continue
            if verdict is not None:
                summary.failed.append((ro["obs_key"], verdict))
                continue

            try:
                self.obs.delete_object(ro["bucket_name"], ro["obs_key"])
                self.catalog.mark_restore_object_cleaned(ro["id"], note="ok")
                summary.deleted += 1
            except Exception as e:
                summary.failed.append((ro["obs_key"], str(e)))

        # 终态: 有 failed → "failed" (运维介入); 全清 → "cleaned"
        if summary.failed:
            self.catalog.update_restore_session_status(
                session_id, "failed",
                error_message=f"cleanup 部分失败: {len(summary.failed)} 个对象"
            )
        else:
            self.catalog.update_restore_session_status(session_id, "cleaned")
        self.catalog.log_operation(
            operation="cleanup", run_id=None,
            target=f"session:{session_id}",
            detail=(f'{{"deleted":{summary.deleted},'
                    f'"skipped":{summary.skipped},'
                    f'"failed":{summary.failed}}}'),
        )
        return summary

    def _check_gates(self, ro: dict) -> str | None:
        """5 道门禁: 返回 None = 通过, "skipped" = 已清理过 (调用方记 skipped),
        其他 str = 失败原因 (调用方记 failed)。
        """
        # 门禁 3: uploaded_by_session
        if ro["uploaded_by_session"] != 1:
            return (f"非本 session 写入 (uploaded_by_session="
                    f"{ro['uploaded_by_session']})")
        # 幂等性: 已清理过 → 跳过
        if ro["cleanup_status"] == "cleaned":
            return "skipped"
        # 门禁 4: backup_objects.status == 'obs_deleted'
        bo = (
            self.catalog.get_backup_object(ro["backup_object_id"])
            if ro["backup_object_id"] else None
        )
        if bo and bo.status != "obs_deleted":
            return f"backup_objects 状态为 {bo.status}, 非 obs_deleted"
        # 门禁 5: ETag 二次校验
        meta = self.obs.get_object_metadata(ro["bucket_name"], ro["obs_key"])
        if meta.not_found:
            self.catalog.mark_restore_object_cleaned(
                ro["id"], note="already_not_found")
            return "skipped"
        if (ro["restored_etag"] and meta.etag
                and meta.etag != ro["restored_etag"]):
            return (f"ETag 已变化 ({meta.etag} vs {ro['restored_etag']}), "
                    f"拒绝删除")
        return None
