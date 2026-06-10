import io
import datetime as dt
from src.obs_client import ObsClient, ObsObject, ObjectMetadata


def test_mock_list_objects_returns_sorted():
    client = ObsClient.create_mock(initial_objects=[
        ("b1", "k1", 100, dt.datetime(2026, 6, 1, 0, 0, 0), "etag-1"),
        ("b1", "k2", 200, dt.datetime(2026, 6, 2, 0, 0, 0), "etag-2"),
    ])
    objs = list(client.list_objects("b1", prefix=""))
    assert [o.key for o in objs] == ["k1", "k2"]


def test_mock_list_objects_with_prefix_filter():
    client = ObsClient.create_mock(initial_objects=[
        ("b1", "Log/cn_5001/f1", 10, dt.datetime(2026, 6, 1), "e1"),
        ("b1", "Log/dn1/f2", 20, dt.datetime(2026, 6, 1), "e2"),
        ("b1", "Db/file.rch", 30, dt.datetime(2026, 6, 1), "e3"),
    ])
    keys = [o.key for o in client.list_objects("b1", prefix="Log/")]
    assert keys == ["Log/cn_5001/f1", "Log/dn1/f2"]


def test_mock_put_and_get_object():
    client = ObsClient.create_mock()
    client.put_file("b1", "k1", io.BytesIO(b"hello"), content_length=5)
    out = io.BytesIO()
    client.get_object("b1", "k1", out)
    assert out.getvalue() == b"hello"


def test_mock_get_object_metadata():
    client = ObsClient.create_mock(initial_objects=[
        ("b1", "k1", 100, dt.datetime(2026, 6, 1, 12, 0, 0), "etag-abc"),
    ])
    meta = client.get_object_metadata("b1", "k1")
    assert meta.size == 100
    assert meta.etag == "etag-abc"


def test_mock_delete_object():
    client = ObsClient.create_mock(initial_objects=[
        ("b1", "k1", 1, dt.datetime(2026, 6, 1), "e1"),
    ])
    client.delete_object("b1", "k1")
    assert list(client.list_objects("b1", prefix="")) == []


def test_mock_list_common_prefixes():
    """带 delimiter='/' 时，返回顶层 '目录'。"""
    client = ObsClient.create_mock(initial_objects=[
        ("b1", "Log/cn_5001/f1", 10, dt.datetime(2026, 6, 1), "e1"),
        ("b1", "Log/cn_5001/f2", 20, dt.datetime(2026, 6, 1), "e2"),
        ("b1", "Log/dn1/f1", 30, dt.datetime(2026, 6, 1), "e3"),
        ("b1", "Db/dir1/f1", 40, dt.datetime(2026, 6, 1), "e4"),
    ])
    prefixes = client.list_common_prefixes("b1", prefix="Log/", delimiter="/")
    assert set(prefixes) == {"Log/cn_5001/", "Log/dn1/"}
