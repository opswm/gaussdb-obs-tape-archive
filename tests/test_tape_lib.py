import hashlib
from pathlib import Path
from src.tape_lib import TapeLibrary, TapeWriteResult


def test_simulated_write_and_read_back(tmp_path: Path):
    lib = TapeLibrary.create_simulated(
        base_path=str(tmp_path), max_volume_size_gb=1,
    )
    src = tmp_path / "archive.tar.gz"
    src.write_bytes(b"hello world" * 1000)

    result = lib.write_archive(str(src), archive_id=42)
    assert result.tape_volume.startswith("TAPE")
    assert result.written_size > 0
    assert result.verify_checksum  # 模拟模式会做 read-back 校验


def test_simulated_read_back_content_matches(tmp_path: Path):
    lib = TapeLibrary.create_simulated(base_path=str(tmp_path), max_volume_size_gb=1)
    src = tmp_path / "a.tar.gz"
    payload = b"the-quick-brown-fox" * 500
    src.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()

    res = lib.write_archive(str(src), archive_id=1)
    out = tmp_path / "restored.tar.gz"
    lib.read_archive(res.tape_volume, res.tape_position, res.written_size, str(out))

    assert hashlib.sha256(out.read_bytes()).hexdigest() == expected


def test_simulated_volume_rollover(tmp_path: Path):
    """超过单卷容量应自动切换到新卷。"""
    lib = TapeLibrary.create_simulated(
        base_path=str(tmp_path), max_volume_size_gb=0,  # 0 = 1 byte 强制切换
    )
    src = tmp_path / "big.tar.gz"
    src.write_bytes(b"x" * 100)
    r1 = lib.write_archive(str(src), archive_id=1)
    r2 = lib.write_archive(str(src), archive_id=2)
    assert r1.tape_volume != r2.tape_volume


def test_list_volumes(tmp_path: Path):
    lib = TapeLibrary.create_simulated(base_path=str(tmp_path), max_volume_size_gb=0)
    src = tmp_path / "x.bin"
    src.write_bytes(b"y" * 10)
    lib.write_archive(str(src), 1)
    lib.write_archive(str(src), 2)
    vols = lib.list_volumes()
    assert len(vols) >= 2
