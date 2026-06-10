import pytest
from pathlib import Path


@pytest.fixture
def tmp_work_dir(tmp_path: Path) -> Path:
    """临时工作目录（每个测试自动隔离）。"""
    return tmp_path


@pytest.fixture
def tmp_catalog_path(tmp_work_dir: Path) -> Path:
    """临时 SQLite 路径。"""
    return tmp_work_dir / "catalog.db"
