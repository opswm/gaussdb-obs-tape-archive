"""OBS 客户端抽象。
- 真实实现走内置 obs_sdk (华为云 OBS SDK 3.26.2)
- 测试用 MockObsClient (内存模拟)
所有其他模块只能依赖本文件的接口签名, 不可直接 import obs sdk。
"""
from __future__ import annotations

import io
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import BinaryIO, Iterable

from src.compat import datetime_fromisoformat


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

    @classmethod
    def create_real(
        cls,
        access_key: str,
        secret_key: str,
        endpoint: str,
        bucket: str,
        **kwargs,
    ) -> "ObsClient":
        """创建真实华为云 OBS 客户端 (内置 obs_sdk 3.26.2)。

        参数:
        - access_key: OBS Access Key
        - secret_key: OBS Secret Key
        - endpoint: OBS Endpoint (如 https://obs.cn-north-4.myhuaweicloud.com)
        - bucket: 默认桶名
        - kwargs: 传递给 SDK ObsClient 的额外参数 (如 timeout, max_retry_count 等)
        """
        return _RealObsClient(
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint,
            bucket=bucket,
            **kwargs,
        )


class _RealObsClient(ObsClient):
    """华为云 OBS SDK 适配器, 将 SDK 接口映射到本项目的抽象接口。"""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint: str,
        bucket: str,
        **kwargs,
    ) -> None:
        from obs import const
        # 解析 endpoint: https://obs.cn-north-4.myhuaweicloud.com → server
        server = endpoint
        is_secure = True
        if server.startswith("https://"):
            server = server[8:]
        elif server.startswith("http://"):
            server = server[7:]
            is_secure = False

        self._bucket = bucket
        self._sdk = self._create_sdk_client(
            access_key, secret_key, server, is_secure, **kwargs
        )

    def _create_sdk_client(
        self, access_key: str, secret_key: str,
        server: str, is_secure: bool, **kwargs,
    ):
        """延迟 import, 避免 Mock 模式下加载 SDK。"""
        from obs.client import ObsClient as SdkObsClient
        return SdkObsClient(
            access_key_id=access_key,
            secret_access_key=secret_key,
            server=server,
            is_secure=is_secure,
            signature="obs",
            **kwargs,
        )

    def list_objects(
        self, bucket: str = "", prefix: str = "", delimiter: str = "",
    ) -> Iterable[ObsObject]:
        from obs.model import Content
        bucket = bucket or self._bucket
        max_keys = 1000
        marker = None
        while True:
            resp = self._sdk.listObjects(
                bucketName=bucket,
                prefix=prefix,
                marker=marker,
                max_keys=max_keys,
                delimiter=delimiter if delimiter else None,
            )
            for content in resp.body.contents or []:
                if isinstance(content, Content):
                    yield ObsObject(
                        key=content.key,
                        size=content.size,
                        last_modified=_parse_sdk_time(content.lastModified),
                        etag=content.etag or "",
                    )
            if not resp.body.isTruncated:
                break
            marker = resp.body.nextMarker

    def list_common_prefixes(
        self, bucket: str = "", prefix: str = "", delimiter: str = "/",
    ) -> list[str]:
        from obs.model import Content
        bucket = bucket or self._bucket
        resp = self._sdk.listObjects(
            bucketName=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=1000,
        )
        return list(resp.body.commonPrefixs or [])

    def get_object(self, bucket: str, key: str, out: BinaryIO) -> None:
        bucket = bucket or self._bucket
        resp = self._sdk.getObject(bucketName=bucket, objectKey=key)
        # resp.body 是 ObjectStream (类 file-like), 逐块读取
        while True:
            chunk = resp.body.read(65536)
            if not chunk:
                break
            out.write(chunk)

    def get_object_metadata(self, bucket: str, key: str) -> ObjectMetadata:
        bucket = bucket or self._bucket
        try:
            resp = self._sdk.getObjectMetadata(bucketName=bucket, objectKey=key)
            return ObjectMetadata(
                size=int(resp.header.get("content-length", 0)),
                last_modified=_parse_sdk_time(
                    resp.header.get("last-modified", "")
                ),
                etag=resp.header.get("etag", "").strip('"'),
            )
        except Exception:
            # 某些 SDK 异常可能表示对象不存在, 统一返回 not_found
            return ObjectMetadata(0, datetime.now(timezone.utc), "", not_found=True)

    def put_file(
        self, bucket: str, key: str,
        data: BinaryIO, content_length: int,
    ) -> ObjectMetadata:
        bucket = bucket or self._bucket
        # SDK putFile 需要文件路径, 但 data 是 BinaryIO, 先写到临时文件
        fd, tmp_path = tempfile.mkstemp(prefix="obs-upload-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data.read())
            resp = self._sdk.putFile(
                bucketName=bucket, objectKey=key, file_path=tmp_path,
            )
            if resp.status < 300:
                return ObjectMetadata(
                    size=content_length,
                    last_modified=datetime.now(timezone.utc),
                    etag=resp.body.etag if resp.body else "",
                )
            raise ObsError(
                f"putFile 失败: {resp.status} {resp.errorMessage}"
            )
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def delete_object(self, bucket: str, key: str) -> None:
        bucket = bucket or self._bucket
        self._sdk.deleteObject(bucketName=bucket, objectKey=key)


class ObsError(Exception):
    """OBS 操作错误。"""
    pass


def _parse_sdk_time(value: str) -> datetime:
    """解析 SDK 返回的时间字符串 → UTC-aware datetime。"""
    if not value:
        return datetime.now(timezone.utc)
    # 格式: '2026-06-15T10:30:00.000Z' 或 'Mon, 15 Jun 2026 10:30:00 GMT'
    from email.utils import parsedate_to_datetime
    try:
        # 尝试 RFC 2822 (Last-Modified 格式)
        return parsedate_to_datetime(value).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        pass
    try:
        # 尝试 ISO 8601
        s = value.replace("Z", "+00:00")
        return datetime_fromisoformat(s).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


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
