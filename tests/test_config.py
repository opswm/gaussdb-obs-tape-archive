# tests/test_config.py
import json
import pytest
from src.config import load_config
from src.errors import ConfigError


def test_load_minimal_config(tmp_path):
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps({
        "obs": {"bucket_name": "b", "endpoint": "https://obs.example.com",
                "access_key": "env:AK", "secret_key": "env:SK",
                "concurrency": 4, "part_size_mb": 10},
        "instances": [{
            "alias": "ncbs_busi",
            "instance_id": "2c61167d2f1f42858bc2a719a1275eae_in00aaaaaaaaaaaaaaaaaaaaaain00",
            "display_name": "核心数据库集群",
            "description": "core",
            "enabled": True,
            "archive_policy": {
                "archive_full": True, "archive_snapshot": True,
                "archive_diff": True, "archive_xlog": True,
                "retention_days": 90,
                "xlog_redundancy_hours": 6.0, "xlog_forward_hours": 6.0,
            }
        }],
        "tape": {"mode": "simulated", "simulated_path": str(tmp_path / "tapes"),
                 "max_volume_size_gb": 100, "verify_after_write": True},
        "archive_dir": str(tmp_path / "tape_mapping"),
        "catalog": {"path": str(tmp_path / "cat.db"), "backup_enabled": False,
                    "backup_path": "", "backup_retention_days": 90},
        "work_dir": str(tmp_path / "work"),
        "archive": {"required_manual_confirm_for_delete": True,
                    "max_concurrent_pack_jobs": 3,
                    "daily_archive_format": "tar.gz", "compression_level": 6},
        "restore": {"local_work_retention_hours": 24},
    }))
    cfg = load_config(str(cfg_path))
    assert cfg.instances[0].alias == "ncbs_busi"
    assert cfg.instances[0].policy.archive_xlog is True
    assert cfg.tape.mode == "simulated"
    assert cfg.archive_dir.path == str(tmp_path / "tape_mapping")


def test_load_missing_file_raises():
    with pytest.raises(ConfigError):
        load_config("/nonexistent.json")
