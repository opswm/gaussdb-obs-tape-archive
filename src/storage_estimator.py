"""Storage estimation: calculate pending archive sizes and check disk space.

Used by scan (post-scan summary) and pack-all-weeks (find pending weeks).
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from src.catalog import Catalog
from src.compat import date_fromisoformat
from src.week_boundary import compute_week_range


def _format_bytes(n: int) -> str:
    """Human-readable byte size."""
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB", "PB"):
        n /= 1024.0
        if n < 1024:
            return f"{n:.1f} {unit}"
    return f"{n:.1f} PB"


@dataclass
class WeekEstimate:
    cluster_alias: str
    week_start: str       # YYYY-MM-DD
    week_end: str         # YYYY-MM-DD
    total_bytes: int
    total_human: str
    full_count: int = 0
    diff_count: int = 0
    snapshot_count: int = 0
    xlog_count: int = 0


@dataclass
class StorageEstimate:
    total_pending_bytes: int = 0
    total_pending_human: str = "0 B"
    per_week: list[WeekEstimate] = field(default_factory=list)
    disk_free_bytes: int = 0
    disk_free_human: str = "0 B"
    sufficient: bool = True
    warning: str | None = None

    def format_display(self) -> str:
        lines = []
        lines.append("")
        lines.append("=" * 50)
        lines.append("  存储估算")
        lines.append("=" * 50)
        lines.append(f"待归档总大小: {self.total_pending_human} ({self.total_pending_bytes:,} bytes)")
        lines.append(f"archive_dir 可用空间: {self.disk_free_human}")
        lines.append("")

        if self.per_week:
            lines.append("按周分解:")
            for we in self.per_week:
                lines.append(
                    f"  {we.cluster_alias:12s} {we.week_start}~{we.week_end}: "
                    f"{we.total_human:>10s}  "
                    f"({we.full_count} full, {we.diff_count} diff, "
                    f"{we.snapshot_count} snap, {we.xlog_count} xlog)"
                )
            lines.append("")

        if self.sufficient:
            lines.append(f"磁盘空间: 充足 (需要 ~{_format_bytes(self.total_pending_bytes * 2)}, "
                         f"可用 {self.disk_free_human})")
        else:
            lines.append(f"磁盘空间不足! 预估需要 {_format_bytes(self.total_pending_bytes * 2)} "
                         f"(含 staging 临时空间), 可用仅 {self.disk_free_human}。")
            lines.append("请扩展 archive_dir 磁盘或分批执行 pack-weekly。")

        return "\n".join(lines)


def estimate_pending(
    catalog: Catalog,
    instances: list,
    archive_dir: Path,
) -> StorageEstimate:
    """Calculate storage needed for all queued_for_archive objects.

    Groups by (instance_id, week) using each cluster's week_start_day policy.
    Checks disk free space on archive_dir.
    """
    # Collect all queued objects
    rows = catalog._conn().execute(
        """SELECT bo.instance_id, bo.obs_size_bytes, bo.backup_type, bo.backup_date,
                  im.alias
           FROM backup_objects bo
           JOIN instance_mappings im ON bo.instance_id = im.instance_id
           WHERE bo.status = 'queued_for_archive'
           ORDER BY im.alias, bo.backup_date"""
    ).fetchall()

    if not rows:
        free = _disk_free(archive_dir)
        return StorageEstimate(
            total_pending_bytes=0,
            total_pending_human="0 B",
            disk_free_bytes=free,
            disk_free_human=_format_bytes(free),
            sufficient=True,
        )

    # Build instance lookup: alias -> (instance_id, week_start_day)
    inst_map: dict[str, tuple[str, int]] = {}
    for ins in instances:
        inst_map[ins.instance_id] = (ins.alias, ins.policy.week_start_day)

    # Group by (instance_id, week_start)
    week_groups: dict[tuple[str, str], dict] = {}
    for r in rows:
        iid = r["instance_id"]
        alias, wsd = inst_map.get(iid, (iid, 6))
        backup_date = date_fromisoformat(r["backup_date"])
        week_start, week_end = compute_week_range(backup_date, wsd)
        key = (iid, week_start.isoformat())
        if key not in week_groups:
            week_groups[key] = {
                "cluster_alias": alias,
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "total_bytes": 0,
                "full_count": 0,
                "diff_count": 0,
                "snapshot_count": 0,
                "xlog_count": 0,
            }
        g = week_groups[key]
        g["total_bytes"] += r["obs_size_bytes"]
        bt = r["backup_type"]
        if bt in ("full", "diff", "snapshot", "xlog"):
            g[f"{bt}_count"] += 1

    total = sum(g["total_bytes"] for g in week_groups.values())
    free = _disk_free(archive_dir)
    # Need ~2x for staging + compressed tar (rough estimate)
    needed = total * 2

    per_week = sorted(
        [WeekEstimate(
            cluster_alias=g["cluster_alias"],
            week_start=g["week_start"],
            week_end=g["week_end"],
            total_bytes=g["total_bytes"],
            total_human="",
            full_count=g["full_count"],
            diff_count=g["diff_count"],
            snapshot_count=g["snapshot_count"],
            xlog_count=g["xlog_count"],
        ) for g in week_groups.values()],
        key=lambda w: (w.cluster_alias, w.week_start),
    )
    # Human-readable strings
    for we in per_week:
        we.total_human = _format_bytes(we.total_bytes)

    warning = None
    sufficient = free >= needed
    if not sufficient:
        warning = (
            f"磁盘空间不足! 预估需要 {_format_bytes(needed)} "
            f"(含 staging 临时空间), 可用仅 {_format_bytes(free)}。"
        )

    return StorageEstimate(
        total_pending_bytes=total,
        total_pending_human=_format_bytes(total),
        per_week=per_week,
        disk_free_bytes=free,
        disk_free_human=_format_bytes(free),
        sufficient=sufficient,
        warning=warning,
    )


def find_pending_weeks(
    catalog: Catalog,
    instance_id: str,
    week_start_day: int,
) -> list[tuple[date, date]]:
    """Find all distinct weeks that have queued_for_archive objects for an instance.

    Returns sorted list of (week_start, week_end) date pairs.
    """
    rows = catalog._conn().execute(
        """SELECT DISTINCT backup_date
           FROM backup_objects
           WHERE instance_id = ?
             AND status = 'queued_for_archive'
           ORDER BY backup_date""",
        (instance_id,),
    ).fetchall()

    seen: set[tuple[date, date]] = set()
    result: list[tuple[date, date]] = []
    for r in rows:
        backup_date = date_fromisoformat(r["backup_date"])
        ws, we = compute_week_range(backup_date, week_start_day)
        pair = (ws, we)
        if pair not in seen:
            seen.add(pair)
            result.append(pair)
    return result


def _disk_free(path: Path) -> int:
    """Get free disk space on the filesystem containing path. Returns 0 on error."""
    try:
        return shutil.disk_usage(str(path)).free
    except OSError:
        return 0
