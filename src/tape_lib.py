"""磁带库抽象层。
- 模拟模式: 本地目录, 每个子目录 = 一盘磁带, max_volume_size_gb 强制换卷
- 生产模式 (TODO): 接入 IBM Spectrum Protect (dsmc)
"""
from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TapeWriteResult:
    tape_volume: str
    tape_position: int
    written_size: int
    write_speed_mbps: float
    verify_checksum: str | None  # SHA256 hex


class TapeLibrary(ABC):
    @abstractmethod
    def write_archive(self, archive_file_path: str,
                      archive_id: int) -> TapeWriteResult: ...
    @abstractmethod
    def read_archive(self, tape_volume: str, tape_position: int,
                     size_bytes: int, output_path: str) -> None: ...
    @abstractmethod
    def list_volumes(self) -> list[str]: ...

    @classmethod
    def create_simulated(cls, base_path: str,
                         max_volume_size_gb: int) -> "TapeLibrary":
        return _SimulatedTapeLibrary(base_path, max_volume_size_gb)


class _SimulatedTapeLibrary(TapeLibrary):
    # 排除 volume.meta 后再算 position / used
    _META_NAME = "volume.meta"

    def __init__(self, base_path: str, max_volume_size_gb: int) -> None:
        self.base = Path(base_path)
        self.base.mkdir(parents=True, exist_ok=True)
        # max_volume_size_gb=0 → 强制每次写都换卷 (1 byte 容量)
        if max_volume_size_gb <= 0:
            self.max_bytes = 1
        else:
            self.max_bytes = max_volume_size_gb * 1024 * 1024 * 1024
        self._next_vol_idx = self._scan_existing_volumes() + 1
        self._current_volume = None
        self._current_used = 0

    def _scan_existing_volumes(self) -> int:
        max_idx = 0
        for p in self.base.iterdir():
            if p.is_dir() and p.name.startswith("TAPE"):
                try:
                    idx = int(p.name[4:])
                    if idx > max_idx:
                        max_idx = idx
                except ValueError:
                    pass
        return max_idx

    def _data_files(self, vol_dir: Path) -> list[Path]:
        """卷内数据文件列表 (排除 volume.meta)。"""
        return [f for f in vol_dir.iterdir()
                if f.is_file() and f.name != self._META_NAME]

    def _new_volume(self) -> str:
        vol = f"TAPE{self._next_vol_idx:03d}"
        self._next_vol_idx += 1
        (self.base / vol).mkdir(parents=True, exist_ok=True)
        (self.base / vol / self._META_NAME).write_text(json.dumps({
            "volume_id": vol, "used_bytes": 0, "status": "active",
        }))
        self._current_used = 0
        return vol

    def _ensure_volume_for(self, size: int) -> str:
        if self._current_volume is None or (
            self.max_bytes > 0 and self._current_used + size > self.max_bytes
        ):
            self._current_volume = self._new_volume()
        return self._current_volume

    def write_archive(self, archive_file_path: str,
                      archive_id: int) -> TapeWriteResult:
        src = Path(archive_file_path)
        data = src.read_bytes()
        size = len(data)

        vol = self._ensure_volume_for(size)
        vol_dir = self.base / vol
        # 按 archive_id 命名
        dest = vol_dir / f"archive_{archive_id}_{src.name}"
        dest.write_bytes(data)

        # P0-2 修复: position 只算 data files, 排除 volume.meta
        position = sum(f.stat().st_size for f in self._data_files(vol_dir)
                       if f != dest)
        # P0-4 修复: used = data files 总和 (与 quota 含义一致)
        self._current_used = position + size

        # 更新 volume.meta (加锁模拟后续扩展)
        meta_path = vol_dir / self._META_NAME
        meta = json.loads(meta_path.read_text())
        meta["used_bytes"] = self._current_used
        meta_path.write_text(json.dumps(meta))

        # 回读校验
        sha = hashlib.sha256(dest.read_bytes()).hexdigest()

        return TapeWriteResult(
            tape_volume=vol, tape_position=position,
            written_size=size, write_speed_mbps=0.0,
            verify_checksum=sha,
        )

    def read_archive(self, tape_volume: str, tape_position: int,
                     size_bytes: int, output_path: str) -> None:
        # P0-1 修复: 必须用 tape_volume + tape_position 定位
        # 之前: 按 size 接近度找 (错位, 多个相同 size 文件会选错)
        # 修复: 按 position 定位 (position 是写入时的累计字节偏移)
        vol_dir = self.base / tape_volume
        if not vol_dir.exists():
            raise FileNotFoundError(f"磁带卷 {tape_volume} 不存在")
        # 累计字节定位: 找到第一个 cumulative_size > tape_position 的文件
        cumulative = 0
        target = None
        for f in sorted(self._data_files(vol_dir),
                        key=lambda p: int(p.name.split("_")[1])
                        if p.name.startswith("archive_") else 0):
            fsize = f.stat().st_size
            if cumulative <= tape_position < cumulative + fsize:
                target = f
                break
            cumulative += fsize
        if target is None:
            raise FileNotFoundError(
                f"磁带 {tape_volume} 找不到 position={tape_position} 的 archive "
                f"(卷内累计 {cumulative} bytes)"
            )
        # 校验 size_bytes 与文件实际大小 (防止 catalog 与 tape 不一致)
        actual_size = target.stat().st_size
        if size_bytes > 0 and size_bytes != actual_size:
            raise ValueError(
                f"磁带 {tape_volume} position={tape_position} 大小不匹配: "
                f"catalog 期望 {size_bytes}, 实际 {actual_size}"
            )
        Path(output_path).write_bytes(target.read_bytes())

    def list_volumes(self) -> list[str]:
        return sorted(p.name for p in self.base.iterdir() if p.is_dir() and p.name.startswith("TAPE"))
