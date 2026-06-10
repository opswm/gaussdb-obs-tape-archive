"""manifest.json 读写。"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any


def build_manifest(
    instance_alias: str, instance_display_name: str, instance_id: str,
    archive_date: str, archive_filename: str, contents: dict[str, Any],
    directory_tree: list[str], work_dir: Path,
) -> dict[str, Any]:
    manifest = {
        "archive_date": archive_date,
        "archive_filename": archive_filename,
        "instance_id": instance_id,
        "instance_alias": instance_alias,
        "instance_display_name": instance_display_name,
        "created_at": datetime.now().astimezone().isoformat(),
        "contents": contents,
        "directory_tree": directory_tree,
    }
    return manifest


def write_manifest(manifest: dict[str, Any], target: Path) -> None:
    target.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
