"""OBS 客户端抽象。
- 真实实现走 esdk-obs-python (华为云)
- 测试用 MockObsClient (内存模拟)
所有其他模块只能依赖本文件的接口签名, 不可直接 import obs sdk。
"""
from __future__ import annotations

import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO, Iterable


@dataclass
class ObsObject:
    key: str
    size: int
    last_modified: datetime
    etag: str


@dataclass
class ObjectMetadata:
    size: int
    last_modified: datetime
    etag: str
    not_found: bool = False


class ObsClient(ABC):
    @abstractmethod
    def list_objects(self, bucket: str, prefix: str = "",
                     delimiter: str = "") -> Iterable[ObsObject]: ...
    @abstractmethod
    def list_common_prefixes(self, bucket: str, prefix: str,
                             delimiter: str = "/") -> list[str]: ...
    @abstractmethod
    def get_object(self, bucket: str, key: str,
                   out: BinaryIO) -> None: ...
    @abstractmethod
    def get_object_metadata(self, bucket: str, key: str) -> ObjectMetadata: ...
    @abstractmethod
    def put_file(self, bucket: str, key: str,
                 data: BinaryIO, content_length: int) -> ObjectMetadata: ...
    @abstractmethod
    def delete_object(self, bucket: str, key: str) -> None: ...

    @classmethod
    def create_mock(cls, initial_objects: list[tuple] | None = None) -> "ObsClient":
        return _MockObsClient(initial_objects or [])


class _MockObsClient(ObsClient):
    def __init__(self, initial_objects: list[tuple]) -> None:
        # initial_objects: (bucket, key, size, last_modified, etag)
        self._store: dict[tuple[str, str], tuple[int, datetime, str, bytes]] = {}
        for b, k, s, lm, et in initial_objects:
            self._store[(b, k)] = (s, lm, et, b"")

    def list_objects(self, bucket: str, prefix: str = "",
                     delimiter: str = "") -> Iterable[ObsObject]:
        items = []
        for (b, k), (s, lm, et, _) in self._store.items():
            if b != bucket or not k.startswith(prefix):
                continue
            if delimiter and delimiter in k[len(prefix):]:
                # 有 delimiter 时只列出当前层 (不深入下一层)
                rest = k[len(prefix):]
                if rest.count(delimiter) >= 1:
                    continue
            items.append(ObsObject(k, s, lm, et))
        items.sort(key=lambda o: o.key)
        return iter(items)

    def list_common_prefixes(self, bucket: str, prefix: str,
                             delimiter: str = "/") -> list[str]:
        seen: set[str] = set()
        for (b, k), _ in self._store.items():
            if b != bucket or not k.startswith(prefix):
                continue
            rest = k[len(prefix):]
            if delimiter not in rest:
                continue
            top = rest.split(delimiter, 1)[0]
            seen.add(prefix + top + delimiter)
        return sorted(seen)

    def get_object(self, bucket: str, key: str, out: BinaryIO) -> None:
        if (bucket, key) not in self._store:
            from src.errors import ObsError
            raise ObsError(f"对象不存在: {key}")
        out.write(self._store[(bucket, key)][3])

    def get_object_metadata(self, bucket: str, key: str) -> ObjectMetadata:
        if (bucket, key) not in self._store:
            return ObjectMetadata(0, datetime.now(), "", not_found=True)
        s, lm, et, _ = self._store[(bucket, key)]
        return ObjectMetadata(s, lm, et)

    def put_file(self, bucket: str, key: str,
                 data: BinaryIO, content_length: int) -> ObjectMetadata:
        blob = data.read()
        now = datetime.now()
        etag = f"mock-{hash(blob) & 0xffffffff:08x}"
        self._store[(bucket, key)] = (len(blob), now, etag, blob)
        return ObjectMetadata(len(blob), now, etag)

    def delete_object(self, bucket: str, key: str) -> None:
        self._store.pop((bucket, key), None)
