"""metadata.json 读写: 周度归档元数据 (含 Beijing time 转换)。"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.utils import format_beijing_short, utc_to_beijing


def build_weekly_manifest(
    instance_alias: str,
    instance_id: str,
    display_name: str,
    bucket_name: str,
    week_start_day: int,
    week_start: date,
    week_end: date,
    full_dirs: list[dict],
    diff_dirs: list[dict],
    snapshot_dirs: list[dict],
    xlog_summary: dict,
    metadata_skipped: int,
    totals: dict,
    checksum_sha256: str | None = None,
) -> dict[str, Any]:
    """构造 metadata.json dict。

    full_dirs/diff_dirs/snapshot_dirs: 每项 = {dir_name, ts_ms, utc, beijing}
    xlog_summary: {count, last_modified_first_utc, last_modified_last_utc,
                   last_modified_first_beijing, last_modified_last_beijing,
                   lsn_start, lsn_end}
    totals: {full_count, diff_count, snapshot_count, xlog_count,
             metadata_skipped_count, total_uncompressed_bytes,
             compressed_tar_bytes}
    """
    return {
        "schema_version": "2.0",
        "archive_type": "weekly",
        "cluster": {
            "alias": instance_alias,
            "instance_id": instance_id,
            "display_name": display_name,
            "bucket": bucket_name,
        },
        "archive_period": {
            "week_start_day": week_start_day,
            "week_start_utc": f"{week_start.isoformat()}T00:00:00+00:00",
            "week_end_utc": f"{week_end.isoformat()}T00:00:00+00:00",
            "week_start_beijing": format_beijing_short(
                datetime.fromisoformat(f"{week_start.isoformat()}T00:00:00+00:00")
            ) + " (UTC+8)",
            "week_end_beijing": format_beijing_short(
                datetime.fromisoformat(f"{week_end.isoformat()}T00:00:00+00:00")
            ) + " (UTC+8)",
        },
        "contents": {
            "full_dirs": full_dirs,
            "diff_dirs": diff_dirs,
            "snapshot_dirs": snapshot_dirs,
            "xlog_summary": xlog_summary,
        },
        "totals": {
            **totals,
            "metadata_skipped_count": metadata_skipped,
        },
        "checksum_sha256": checksum_sha256,
    }


def write_metadata(manifest: dict[str, Any], target: Path) -> None:
    """写 metadata.json 文件, UTF-8 + indent。"""
    target.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))


def build_dir_entry(parent_backup_dir: str, ts_ms: int) -> dict[str, Any]:
    """构造 full/diff/snapshot 目录条目, 含 UTC + Beijing time。"""
    from src.utils import ensure_utc_aware
    dt_utc = ensure_utc_aware(
        datetime.fromtimestamp(ts_ms / 1000)
    )
    return {
        "dir_name": parent_backup_dir,
        "timestamp_ms": ts_ms,
        "utc": dt_utc.isoformat(),
        "beijing": format_beijing_short(utc_to_beijing(dt_utc)),
    }


def build_xlog_summary(
    xlog_obs: list,  # list[BackupObject]
) -> dict[str, Any]:
    """从 xlog 对象列表构造 xlog_summary。"""
    if not xlog_obs:
        return {
            "count": 0,
            "last_modified_first_utc": None,
            "last_modified_last_utc": None,
            "last_modified_first_beijing": None,
            "last_modified_last_beijing": None,
            "lsn_start": None,
            "lsn_end": None,
        }
    lms = [bo.obs_last_modified for bo in xlog_obs]
    lm_first = min(lms)
    lm_last = max(lms)
    # LSN 是 24 字符段名, 格式 (high32, low32) 大写 16 进制.
    # 字符串 min/max 与 WAL 数值序不一致 (例如 '0000000200000000000000A0'
    # 数值 > '000000010000000000000100' 但字典序相反). 必须数值比较.
    def _lsn_key(parent_dir: str) -> tuple[int, int]:
        return (int(parent_dir[:16], 16), int(parent_dir[16:24], 16))

    lsns = [bo.parent_backup_dir for bo in xlog_obs
            if bo.parent_backup_dir and len(bo.parent_backup_dir) == 24]
    lsn_min = min(lsns, key=_lsn_key) if lsns else None
    lsn_max = max(lsns, key=_lsn_key) if lsns else None
    return {
        "count": len(xlog_obs),
        "last_modified_first_utc": lm_first.isoformat(),
        "last_modified_last_utc": lm_last.isoformat(),
        "last_modified_first_beijing": format_beijing_short(
            utc_to_beijing(lm_first)),
        "last_modified_last_beijing": format_beijing_short(
            utc_to_beijing(lm_last)),
        "lsn_start": lsn_min,
        "lsn_end": lsn_max,
    }


def _dir_section(label: str, dirs: list[dict]) -> list[str]:
    """渲染 full/diff/snapshot 目录段, 行数与 N 个目录对应。
    返回 0..N+1 行: header + 每目录一行 ('  - dir_name → Beijing=... (UTC=...)')。"""
    if not dirs:
        return []
    lines = [f"\n{label} ({len(dirs)} 个):"]
    for d in dirs:
        lines.append(
            f"  - {d['dir_name']} → Beijing={d['beijing']} (UTC={d['utc']})"
        )
    return lines


def render_preview(manifest: dict[str, Any]) -> str:
    """把 manifest 渲染为人类可读的 preview 输出。"""
    lines: list[str] = []
    cluster = manifest["cluster"]
    period = manifest["archive_period"]
    lines.append(f"集群: {cluster['alias']} ({cluster['display_name']})")
    lines.append(f"实例: {cluster['instance_id']}")
    lines.append(f"桶: {cluster['bucket']}")
    lines.append(f"周度范围: {period['week_start_beijing']} → {period['week_end_beijing']}")
    lines.append(f"周度范围 (UTC): {period['week_start_utc']} → {period['week_end_utc']}")
    lines.append(f"周起点: {period['week_start_day']} (1=周一..7=周日)")
    lines.append("")
    contents = manifest["contents"]
    lines.extend(_dir_section("全量目录", contents["full_dirs"]))
    lines.extend(_dir_section("差异目录", contents["diff_dirs"]))
    lines.extend(_dir_section("快照目录", contents["snapshot_dirs"]))
    xs = contents["xlog_summary"]
    if xs["count"]:
        lines.append(
            f"\nxlog 文件 ({xs['count']} 个):"
        )
        lines.append(
            f"  - last_modified 范围: {xs['last_modified_first_beijing']} → "
            f"{xs['last_modified_last_beijing']}"
        )
        if xs.get("lsn_start"):
            lines.append(f"  - LSN 范围: {xs['lsn_start']} → {xs['lsn_end']}")
    totals = manifest["totals"]
    if totals.get("metadata_skipped_count", 0) > 0:
        lines.append(
            f"\n元数据 (跳过, archive_only): {totals['metadata_skipped_count']} 个"
        )
    lines.append(
        f"\n合计: full={totals.get('full_count', 0)} "
        f"diff={totals.get('diff_count', 0)} "
        f"snapshot={totals.get('snapshot_count', 0)} "
        f"xlog={totals.get('xlog_count', 0)}"
    )
    return "\n".join(lines)
