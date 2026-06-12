"""Reaper: 安全删除 OBS 原始备份。6 道门禁 + ETag 二次校验 + 顺序依赖。"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from src.catalog import Catalog
from src.errors import UnsafeDeleteError
from src.obs_client import ObsClient


@dataclass
class ReapSummary:
    deleted: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)


class Reaper:
    def __init__(self, obs: ObsClient, catalog: Catalog) -> None:
        self.obs = obs
        self.catalog = catalog

    def reap_daily_archive(
        self, daily_archive_id: int, allow_uncovered_types: bool = True,
    ) -> ReapSummary:
        da = self.catalog.get_daily_archive(daily_archive_id)
        if da is None:
            raise UnsafeDeleteError(f"daily_archive {daily_archive_id} 不存在")

        # ─── 门禁 1: daily_archive 状态 (新模型: pending/archived 二态) ───
        if da.status != "archived":
            raise UnsafeDeleteError(
                f"daily_archive {da.archive_date} 状态为 {da.status}, 必须 archived")

        objs = list(self.catalog.get_objects_by_daily_archive(daily_archive_id))

        # ─── 门禁 2: 所有对象已 archived ───
        not_archived = [o for o in objs if o.status != "archived"]
        if not_archived:
            raise UnsafeDeleteError(
                f"存在 {len(not_archived)} 个对象非 archived, 拒绝删除")

        # ─── 门禁 3: 校验和存在 ───
        if not da.checksum_sha256:
            raise UnsafeDeleteError(f"daily_archive {da.archive_date} 无校验记录")
        no_obj_sha = [o for o in objs if not o.checksum_sha256]
        if no_obj_sha:
            raise UnsafeDeleteError(
                f"存在 {len(no_obj_sha)} 个对象无独立 SHA256")

        # ─── 门禁 3.5: 类型覆盖校验 (P1-1 full 依赖) ───
        # 当 allow_uncovered_types=False 时, 缺 full 视为错误;
        # 缺 diff/xlog 视为部分 PITR, 但 plan 文档要求:
        # 进入 diff 阶段前 full 必须存在, 进入 xlog 阶段前 diff 必须存在。
        # 简化: 把缺失的 full 显式检查。
        if not allow_uncovered_types:
            has_full = any(o.backup_type == "full" for o in objs)
            if not has_full:
                raise UnsafeDeleteError(
                    f"顺序门禁: 缺少 full 类型, 拒绝 reap (allow_uncovered_types=False)")
            has_diff = any(o.backup_type == "diff" for o in objs)
            if not has_diff:
                raise UnsafeDeleteError(
                    f"顺序门禁: 缺少 diff 类型, 拒绝 reap (allow_uncovered_types=False)")

        # ─── 门禁 4: 顺序依赖 (P1-1 强化: full → diff → xlog 硬门禁) ───
        # 无论 allow_uncovered_types 是什么, 删除顺序都按 backup_type 严格分层:
        #   1. backup_type='full' / 'snapshot' 先删
        #   2. 上一阶段全部 deleted 后, 再删 backup_type='diff'
        #   3. 再删 backup_type='xlog'
        #   4. metadata 默认不删 (restore_policy='archive_only'), 跳到 log
        # 防止 PITR 链断裂: 若 diff 已被 reap 但 full 还在, 一旦删除对象,
        # 则后续 PITR 找不到基础点, xlog 回放无法对齐。
        order = ["full", "snapshot", "diff", "xlog"]
        ordered_objs: list = []
        for bt in order:
            ordered_objs.extend([o for o in objs if o.backup_type == bt])
        # metadata 跳过 (archive_only)

        run_id = str(uuid.uuid4())
        summary = ReapSummary()
        bucket = self._bucket(da)
        deleted_so_far = 0
        expected_for_stage = {
            "full": sum(1 for o in ordered_objs if o.backup_type == "full"),
            "snapshot": sum(1 for o in ordered_objs if o.backup_type == "snapshot"),
            "diff": sum(1 for o in ordered_objs if o.backup_type == "diff"),
            "xlog": sum(1 for o in ordered_objs if o.backup_type == "xlog"),
        }
        # 阶段累计已删: 用于阻断"前面阶段未删完就进入下阶段"
        cumulative_archived = {
            "full": 0, "snapshot": 0, "diff": 0, "xlog": 0,
        }
        cumulative_total = {
            "full": expected_for_stage["full"],
            "snapshot": expected_for_stage["snapshot"],
            "diff": expected_for_stage["diff"],
            "xlog": expected_for_stage["xlog"],
        }

        for bo in ordered_objs:
            # ── 顺序门禁: 进入 diff 阶段前, full+snapshot 必须全部完成 ──
            if bo.backup_type == "diff":
                if cumulative_archived["full"] + cumulative_archived["snapshot"] < \
                   cumulative_total["full"] + cumulative_total["snapshot"]:
                    raise UnsafeDeleteError(
                        f"顺序门禁: 进入 diff 阶段前, full/snapshot 必须全部完成 "
                        f"(已删 full+snapshot={cumulative_archived['full'] + cumulative_archived['snapshot']}, "
                        f"目标={cumulative_total['full'] + cumulative_total['snapshot']})"
                    )
            # ── 顺序门禁: 进入 xlog 阶段前, diff 必须全部完成 ──
            if bo.backup_type == "xlog":
                if cumulative_archived["diff"] < cumulative_total["diff"]:
                    raise UnsafeDeleteError(
                        f"顺序门禁: 进入 xlog 阶段前, diff 必须全部完成 "
                        f"(已删 diff={cumulative_archived['diff']}, "
                        f"目标={cumulative_total['diff']})"
                    )

            # ─── 门禁 5: ETag 二次校验 (P0 修复: 任何 mismatch/异常 → 硬失败) ───
            # 之前: soft-fail (summary.failed.append + cumulative_archived += 1 + continue)
            # 后果: 累计基准被"已尝试"对象污染, 顺序门禁失去意义, 可能把 full 未删成功的
            #       diff/xlog 继续推进删除 → PITR 链断裂 + 静默数据丢失。
            # 修复: 异常/ETag mismatch 直接 raise; 只有 delete_object 真正成功才累计。
            try:
                meta = self.obs.get_object_metadata(bucket, bo.obs_key)
            except Exception as e:
                raise UnsafeDeleteError(
                    f"门禁 5 失败: 拉取 {bo.obs_key} 元数据异常: {e}"
                ) from e
            # 安全分支: 对象已不在 OBS (并发 reaper 或人工删), 视为已完成
            if meta.not_found:
                self.catalog.mark_backup_object_obs_deleted(bo.id, run_id)
                summary.deleted += 1
                cumulative_archived[bo.backup_type] += 1
                continue
            # ETag 必须严格一致 (任何不一致 = 数据被改, 拒绝删)
            # fail-closed: null obs_etag 视为不可信, 拒绝删除
            if not bo.obs_etag:
                raise UnsafeDeleteError(
                    f"门禁 5 失败: {bo.obs_key} 缺 obs_etag, 拒绝删除"
                )
            if meta.etag and meta.etag != bo.obs_etag:
                raise UnsafeDeleteError(
                    f"门禁 5 失败: {bo.obs_key} ETag 不匹配 "
                    f"(catalog={bo.obs_etag}, OBS={meta.etag})"
                )

            try:
                self.obs.delete_object(bucket, bo.obs_key)
            except Exception as e:
                raise UnsafeDeleteError(
                    f"门禁 5 失败: 删除 {bo.obs_key} 异常: {e}"
                ) from e
            self.catalog.mark_backup_object_obs_deleted(bo.id, run_id)
            summary.deleted += 1
            cumulative_archived[bo.backup_type] += 1

        self.catalog.log_operation(
            operation="delete", run_id=run_id,
            target=f"daily_archive:{daily_archive_id}",
            detail=f'{{"deleted":{summary.deleted},"failed":{summary.failed}}}',
        )
        return summary

    def _bucket(self, da) -> str:
        ins = self.catalog.get_instance_by_id(da.instance_id)
        if ins is None:
            raise UnsafeDeleteError(f"未知 instance: {da.instance_id}")
        return ins["bucket_name"]
