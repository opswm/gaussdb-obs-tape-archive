"""配置加载：JSON → dataclass。任何 schema 异常都抛 ConfigError。"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from src.errors import ArchiveDirNotFoundError, ConfigError
from src.models import Policy


_ENV_REF = re.compile(r"^env:([A-Z_][A-Z0-9_]*)$")


def _resolve_env(value):
    """若字符串形如 env:NAME，从环境变量解析；否则原样返回。"""
    if isinstance(value, str):
        m = _ENV_REF.match(value)
        if m:
            return os.environ.get(m.group(1), "")
    return value


@dataclass
class ObsConfig:
    bucket_name: str
    endpoint: str
    access_key: str
    secret_key: str
    concurrency: int = 8
    part_size_mb: int = 10


@dataclass
class InstanceConfig:
    alias: str
    instance_id: str
    display_name: str
    description: str
    enabled: bool
    policy: Policy


@dataclass
class ArchiveDirConfig:
    """归档目录配置: 程序把 weekly tar.gz 写入此目录, 该目录即磁带库映射目录。"""
    path: str


@dataclass
class CatalogConfig:
    path: str
    backup_enabled: bool
    backup_path: str
    backup_retention_days: int


@dataclass
class ArchiveConfig:
    required_manual_confirm_for_delete: bool
    max_concurrent_pack_jobs: int
    daily_archive_format: str
    compression_level: int


@dataclass
class RestoreConfig:
    local_work_retention_hours: int


@dataclass
class AppConfig:
    obs: ObsConfig
    instances: list[InstanceConfig]
    catalog: CatalogConfig
    archive_dir: ArchiveDirConfig
    work_dir: str
    archive: ArchiveConfig
    restore: RestoreConfig


def load_config(path: str) -> AppConfig:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"配置文件不存在: {path}")
    try:
        raw = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        raise ConfigError(f"配置文件不是合法 JSON: {e}") from e

    try:
        obs_raw = raw["obs"]
        obs = ObsConfig(
            bucket_name=obs_raw["bucket_name"],
            endpoint=obs_raw["endpoint"],
            access_key=_resolve_env(obs_raw["access_key"]),
            secret_key=_resolve_env(obs_raw["secret_key"]),
            concurrency=obs_raw.get("concurrency", 8),
            part_size_mb=obs_raw.get("part_size_mb", 10),
        )

        instances: list[InstanceConfig] = []
        for ins in raw["instances"]:
            p_raw = ins["archive_policy"]
            pol = Policy(
                archive_full=bool(p_raw["archive_full"]),
                archive_snapshot=bool(p_raw["archive_snapshot"]),
                archive_diff=bool(p_raw["archive_diff"]),
                archive_xlog=bool(p_raw["archive_xlog"]),
                retention_days=int(p_raw.get("retention_days", 90)),
                xlog_redundancy_hours=float(p_raw.get("xlog_redundancy_hours", 6.0)),
                xlog_forward_hours=float(p_raw.get("xlog_forward_hours", 6.0)),
                week_start_day=int(p_raw.get("week_start_day", 6)),
            )
            instances.append(InstanceConfig(
                alias=ins["alias"], instance_id=ins["instance_id"],
                display_name=ins["display_name"], description=ins.get("description", ""),
                enabled=bool(ins.get("enabled", True)), policy=pol,
            ))

        # 新归档目录 (必需, 替代旧 "tape" 段)
        if "archive_dir" not in raw:
            raise ConfigError(
                "配置缺少必需字段: archive_dir (顶层, 字符串路径或 env:XXX 引用)"
            )
        archive_dir_raw = raw["archive_dir"]
        if isinstance(archive_dir_raw, dict):
            # 允许 { "path": "..." } 或 { "env": "..." } 形式
            if "path" in archive_dir_raw:
                archive_dir_path = _resolve_env(archive_dir_raw["path"])
            elif "env" in archive_dir_raw:
                archive_dir_path = os.environ.get(archive_dir_raw["env"], "")
            else:
                raise ConfigError(
                    "archive_dir 必须是非空字符串, 或含 'path'/'env' 键的对象"
                )
        else:
            archive_dir_path = _resolve_env(archive_dir_raw)
        if not archive_dir_path or not archive_dir_path.strip():
            raise ConfigError(
                f"archive_dir 解析为空字符串, 请检查 env 变量是否设置或路径是否填写"
            )
        archive_dir = ArchiveDirConfig(path=archive_dir_path)

        c = raw["catalog"]
        catalog = CatalogConfig(
            path=c["path"], backup_enabled=bool(c.get("backup_enabled", False)),
            backup_path=c.get("backup_path", ""),
            backup_retention_days=int(c.get("backup_retention_days", 90)),
        )
        a = raw["archive"]
        archive = ArchiveConfig(
            required_manual_confirm_for_delete=bool(a.get("required_manual_confirm_for_delete", True)),
            max_concurrent_pack_jobs=int(a.get("max_concurrent_pack_jobs", 3)),
            daily_archive_format=a.get("daily_archive_format", "tar.gz"),
            compression_level=int(a.get("compression_level", 6)),
        )
        r = raw["restore"]
        restore = RestoreConfig(
            local_work_retention_hours=int(r.get("local_work_retention_hours", 24)),
        )
    except KeyError as e:
        raise ConfigError(f"配置缺少必需字段: {e.args[0]}") from e

    # ─── 校验 instance_id 格式 (P0-2, P1-3) ───
    # instance_id 必须是完整的 {tenant_id}_{instance_id} 字符串
    # (来自 OBS 实际目录名, 长度通常 > 50, 必含下划线)
    # 防止运维误把 alias (如 "ncbs_busi") 当成 instance_id 灌入
    for ins in instances:
        if "_" not in ins.instance_id or len(ins.instance_id) < 30:
            raise ConfigError(
                f"instance_id 格式异常: '{ins.instance_id}' (alias={ins.alias})。"
                f"必须是完整的 '{{tenant_id}}_{{instance_id}}' 字符串, "
                f"长度 ≥ 30 且含下划线。请使用 OBS 实际目录名 (参考 README 集群示例)。"
            )

    return AppConfig(
        obs=obs, instances=instances,
        catalog=catalog, archive_dir=archive_dir,
        work_dir=raw["work_dir"],
        archive=archive, restore=restore,
    )
