"""Scanner: 周期性扫描 OBS, 发现符合策略的备份对象, 录入 Catalog。
- 动态发现 Log/ 下 cn*/dn* 节点 (不硬编码)
- 分类 full/diff/snapshot/xlog/metadata
- metadata 强制 restore_policy='archive_only'
- 按 parent_backup_dir 解析 backup_date
- P0-5: 使用 policy.retention_days 作为归档门槛
- P1-2: 显式枚举 5 类 xlog 节点元数据分类
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from src.catalog import Catalog
from src.models import BackupObject, Policy
from src.obs_client import ObsClient

# ─── 路径分类常量 ───
_XLOG_FILE_RE = re.compile(
    r"^Log/(?P<node>[A-Za-z0-9_]+)/pg_xlog/tl_\d+/\d+/"
    r"\d{24}_\d{2}_\d{2}_[0-9a-f]{8}_[0-9a-f]{8}_[0-9a-f]{8}$"
)
_LOG_NODE_PREFIXES = ("cn", "dn")
_DIR_NAME_RE = re.compile(r"^\d{13}$")  # unix_ms 13 位数字
_INSTANCE_ROOT_FILES = frozenset({"backup_metadata.cfg", "incr_backup_metadata.cfg"})

# ─── P1-2: 5 类 xlog 节点元数据, 全部 restore_policy='archive_only' ───
LOG_NODE_METADATA_FILES = frozenset({
    "obs_last_clean_record",
    "obs_archive_start_record",       # 文档中也写作 obs_archive_start_end_record
    "obs_archive_start_end_record",
    "cn_build_history",
    "dn_build_history",                # 集中式集群 dn 节点也有
})
RECOVERY_INTERVAL_KEY_PREFIX = "/recovery_interval/"


def classify_log_node_metadata(obs_key: str) -> bool:
    """判断 obs_key 是否属于"归档但默认不恢复"的节点元数据。"""
    base = obs_key.rsplit("/", 1)[-1]
    return base in LOG_NODE_METADATA_FILES


def classify_recovery_interval(obs_key: str) -> bool:
    return RECOVERY_INTERVAL_KEY_PREFIX in obs_key


class Scanner:
    """周期任务入口: scan_instance() 扫一个实例的所有 OBS 备份对象, 落 Catalog。"""

    def __init__(self, obs: ObsClient, catalog: Catalog) -> None:
        self.obs = obs
        self.catalog = catalog

    def scan_instance(
        self, instance_id: str, policy: Policy, bucket: str | None = None,
    ) -> int:
        """扫描单个实例, 录入新发现的 backup_objects。
        - 使用 policy.retention_days 作为 age 门槛 (P0-5)
        - 返回本次新增/刷新的对象数
        """
        if bucket is None:
            ins = self.catalog.get_instance_by_alias(
                next(i["alias"] for i in self.catalog.list_enabled_instances()
                     if i["instance_id"] == instance_id)
            )
            bucket = ins["bucket_name"]

        # P0-5: 必须用 policy.retention_days, 防止 1 天前备份被归档
        age_days = policy.retention_days
        now = datetime.now(timezone.utc)

        count = 0
        # 1. 各顶级类型目录
        if policy.archive_full:
            count += self._scan_dir(bucket, instance_id, "Db/", policy, now, age_days)
        if policy.archive_snapshot:
            count += self._scan_dir(bucket, instance_id, "Snapshot/", policy, now, age_days)
        if policy.archive_diff:
            count += self._scan_dir(bucket, instance_id, "Difference/", policy, now, age_days)
        if policy.archive_xlog:
            count += self._scan_log(bucket, instance_id, now, age_days)

        # 2. 实例级元数据 (总是扫描, restore_policy=archive_only)
        count += self._scan_instance_root(bucket, instance_id, now, age_days)

        # P1-5: 扫描结束 → 重建 PITR 链 (基于 catalog 当前 backup_objects)
        # 幂等, 每次扫完都重算, 不会留下旧 chain 残留
        self._rebuild_pitr_chains(instance_id)
        return count

    def _scan_dir(self, bucket: str, instance_id: str, sub: str,
                  policy: Policy, now: datetime, age_days: int) -> int:
        prefix = f"{instance_id}/{sub}"
        count = 0
        for obj in self.obs.list_objects(bucket, prefix=prefix):
            if not self._passes_age(obj.last_modified, age_days, now):
                continue
            type_, parent, restore_policy = _classify_top_level(obj.key, sub)
            date = _date_from_dir(parent) or obj.last_modified.strftime("%Y-%m-%d")
            ts_ms = int(parent) if parent.isdigit() else None
            self._upsert_with_policy(
                bucket, instance_id, obj, type_, parent, date, ts_ms, restore_policy,
            )
            count += 1
        return count

    def _rebuild_pitr_chains(self, instance_id: str) -> None:
        """P1-5: 扫描结束后统一重建 pitr_chains。
        - 收集该 instance 所有 full base (按 ts_ms 升序)
        - 对每两个相邻 base 之间的区间, 收集 diff
        - 写入 pitr_chains (chain_end_time=NULL 表示当前 open)
        - 幂等: 先清空该 instance 旧 chain
        """
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        conn = self.catalog._conn()
        bases = list(conn.execute(
            """SELECT DISTINCT parent_backup_dir, MAX(backup_timestamp_ms) AS ts_ms
               FROM backup_objects
               WHERE instance_id = ? AND backup_type = 'full'
                 AND parent_backup_dir GLOB '[0-9]*'
               GROUP BY parent_backup_dir
               ORDER BY ts_ms""",
            (instance_id,),
        ).fetchall())
        if not bases:
            return
        all_diffs = list(conn.execute(
            """SELECT DISTINCT parent_backup_dir, MAX(backup_timestamp_ms) AS ts_ms
               FROM backup_objects
               WHERE instance_id = ? AND backup_type = 'diff'
                 AND parent_backup_dir GLOB '[0-9]*'
               GROUP BY parent_backup_dir
               ORDER BY ts_ms""",
            (instance_id,),
        ).fetchall())
        # 幂等: 清空旧 chain
        conn.execute(
            "DELETE FROM pitr_chains WHERE instance_id = ?",
            (instance_id,),
        )
        for i, base in enumerate(bases):
            chain_id = f"{instance_id}_chain_{base['parent_backup_dir']}"
            start_ts = int(base["ts_ms"])
            end_ts = int(bases[i + 1]["ts_ms"]) if i + 1 < len(bases) else None
            # diffs 落在 (start_ts, end_ts] 区间:
            # 当前 base 之后, 下一个 base 之前的差异增量
            in_range = [d for d in all_diffs
                        if start_ts < int(d["ts_ms"])
                        and (end_ts is None or int(d["ts_ms"]) <= end_ts)]
            diff_dirs = [d["parent_backup_dir"] for d in in_range]
            start_dt = _dt.fromtimestamp(start_ts / 1000, tz=_tz.utc)
            end_iso = (_dt.fromtimestamp(end_ts / 1000, tz=_tz.utc).isoformat()
                       if end_ts else None)
            conn.execute(
                """INSERT INTO pitr_chains
                   (chain_id, instance_id, base_full_dir, base_full_time,
                    diff_dirs, diff_count, chain_start_time, chain_end_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (chain_id, instance_id, base["parent_backup_dir"],
                 start_dt.isoformat(), _json.dumps(diff_dirs), len(diff_dirs),
                 start_dt.isoformat(), end_iso),
            )

    def _scan_log(self, bucket: str, instance_id: str,
                  now: datetime, age_days: int) -> int:
        prefix = f"{instance_id}/Log/"
        # 动态发现节点 (不硬编码 cn_5001)
        nodes = [p[len(prefix):].rstrip("/")
                 for p in self.obs.list_common_prefixes(bucket, prefix, "/")]
        nodes = [n for n in nodes if n.startswith(_LOG_NODE_PREFIXES)]
        count = 0
        for node in nodes:
            # 1) pg_xlog/ 下的 xlog 分片
            xlog_prefix = f"{prefix}{node}/pg_xlog/"
            for obj in self.obs.list_objects(bucket, prefix=xlog_prefix):
                if not self._passes_age(obj.last_modified, age_days, now):
                    continue
                # parent_backup_dir 用 LSN 24 位段名
                m = re.search(r"/(\d{24})/", obj.key)
                parent = m.group(1) if m else node
                date = obj.last_modified.strftime("%Y-%m-%d")
                self._upsert_with_policy(
                    bucket, instance_id, obj, "xlog", parent, date, None, "normal",
                )
                count += 1

            # 2) 节点级元数据 (P1-2: 5 类)
            for meta in self.obs.list_objects(bucket, prefix=f"{prefix}{node}/"):
                if not self._passes_age(meta.last_modified, age_days, now):
                    continue
                if "/pg_xlog/" in meta.key or meta.key.endswith("/pg_xlog/"):
                    continue
                if classify_log_node_metadata(meta.key):
                    self._upsert_with_policy(
                        bucket, instance_id, meta, "metadata", node,
                        meta.last_modified.strftime("%Y-%m-%d"), None, "archive_only",
                    )
                    count += 1

        # 3) recovery_interval 整目录 (P1-2)
        ri_prefix = f"{prefix}recovery_interval/"
        for obj in self.obs.list_objects(bucket, prefix=ri_prefix):
            if not self._passes_age(obj.last_modified, age_days, now):
                continue
            base = obj.key.rsplit("/", 1)[-1]
            self._upsert_with_policy(
                bucket, instance_id, obj, "metadata", base,
                obj.last_modified.strftime("%Y-%m-%d"), None, "archive_only",
            )
            count += 1
        return count

    def _scan_instance_root(self, bucket: str, instance_id: str,
                            now: datetime, age_days: int) -> int:
        count = 0
        for obj in self.obs.list_objects(bucket, prefix=f"{instance_id}/"):
            base = obj.key[len(instance_id) + 1:].split("/", 1)[0]
            if base not in _INSTANCE_ROOT_FILES:
                continue
            if not self._passes_age(obj.last_modified, age_days, now):
                continue
            self._upsert_with_policy(
                bucket, instance_id, obj, "metadata", base,
                obj.last_modified.strftime("%Y-%m-%d"), None, "archive_only",
            )
            count += 1
        return count

    def _upsert_with_policy(
        self, bucket: str, instance_id: str, obj, type_: str, parent: str,
        date: str, ts_ms: int | None, restore_policy: str,
    ) -> None:
        bo = BackupObject(
            obs_key=obj.key, instance_id=instance_id,
            obs_size_bytes=obj.size, obs_last_modified=obj.last_modified,
            backup_type=type_, parent_backup_dir=parent, backup_date=date,
            backup_timestamp_ms=ts_ms, restore_policy=restore_policy, obs_etag=obj.etag,
        )
        # 已被标记为 obs_deleted 的, 不重置
        existing = self.catalog.get_backup_object_by_key(obj.key)
        if existing and existing.status == "obs_deleted":
            return
        self.catalog.upsert_backup_object(bo)

    @staticmethod
    def _passes_age(lm: datetime, age_days: int, now: datetime) -> bool:
        """P0-5: age_days 为 policy.retention_days。
        - age_days=0 -> 不过滤
        - age_days>0 -> 只接受 (now - lm).days <= age_days 的对象 (未过保)
        """
        if age_days <= 0:
            return True
        # OBS 返回的 last_modified 可能是 naive; 统一视为 UTC
        if lm.tzinfo is None:
            lm = lm.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return (now - lm).days <= age_days


def _classify_top_level(key: str, sub: str) -> tuple[str, str, str]:
    """key 形如 i1/Db/1780160839955/file_0.rch -> (type, parent_dir, restore_policy)。"""
    parts = key.split("/")
    parent = parts[2] if len(parts) >= 3 else ""
    type_map = {"Db/": "full", "Snapshot/": "snapshot", "Difference/": "diff"}
    return type_map.get(sub, "metadata"), parent, "normal"


def _date_from_dir(parent: str) -> str | None:
    if not _DIR_NAME_RE.match(parent):
        return None
    ts = int(parent) / 1000
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
