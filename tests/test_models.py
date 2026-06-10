from datetime import datetime
from src.models import BackupObject, DailyArchive, InstanceMapping, Policy, RestoreSession


def test_policy_full_combination():
    p = Policy(
        archive_full=True,
        archive_snapshot=True,
        archive_diff=True,
        archive_xlog=True,
    )
    assert p.is_full_pitr_capable() is True


def test_policy_no_xlog_not_pitr():
    p = Policy(
        archive_full=True, archive_snapshot=True,
        archive_diff=True, archive_xlog=False,
    )
    assert p.is_full_pitr_capable() is False


def test_backup_object_defaults():
    bo = BackupObject(
        obs_key="inst/Db/1780160839955/file_0.rch",
        instance_id="inst",
        obs_last_modified=datetime(2026, 6, 1, 0, 0, 0),
        backup_type="full",
        parent_backup_dir="1780160839955",
        backup_date="2026-06-01",
    )
    assert bo.status == "discovered"
    assert bo.restore_policy == "normal"
    assert bo.backup_timestamp_ms is None


def test_restore_session_status_enum():
    s = RestoreSession(
        session_id="uuid-1",
        target_time=datetime(2026, 6, 9, 14, 30),
        required_daily_archives="[42]",
    )
    assert s.status == "retrieving"
