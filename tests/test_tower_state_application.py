from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.tower.migrations import apply_tower_state
from nonebot_plugin_xiuxian_2.features.tower.state_application import TowerStateApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedClock:
    def __init__(self, now=datetime(2026, 7, 14, 20, 0, tzinfo=timezone.utc)):
        self._now = now

    def now(self):
        return self._now


def test_feature_tower_state_application_initializes_once_with_injected_clock(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_tower_state(uow)
    application = TowerStateApplication(database, clock=FixedClock())

    state = application.get("user")
    duplicate = application.get("user")

    assert state == duplicate == {
        "current_floor": 0,
        "max_floor": 0,
        "score": 0,
        "weekly_purchases": {"_last_reset": "2026-07-14"},
    }
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all(
            "SELECT operation_id,kind,period_key FROM tower_state_operations ORDER BY operation_id"
        )
    assert [(row["operation_id"], row["kind"], row["period_key"]) for row in operations] == [
        ("tower-state-init:user", "initialize", "2026-W29"),
    ]


def test_tower_limit_defaults_to_feature_state_application():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_tower"
    source = (root / "tower_limit.py").read_text(encoding="utf-8")

    assert "TowerStateApplication" in source
    assert "TowerStateService" not in source
    assert "state_application.get(user_id)" in source


def test_feature_tower_state_path_uses_uow_clock_and_migration_owned_schema():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    repository = (root / "features" / "tower" / "state_repository.py").read_text(encoding="utf-8")
    application = (root / "features" / "tower" / "state_application.py").read_text(encoding="utf-8")
    limit = (root / "xiuxian" / "xiuxian_tower" / "tower_limit.py").read_text(encoding="utf-8")

    assert "DatabaseUnitOfWork" in repository
    assert all(token not in repository for token in ("xiuxian_utils", "db_backend", "date.today", "datetime.now", "CREATE TABLE"))
    assert "self.clock.now().date()" in application
    assert "lock=_state_lock" in limit
    assert "TowerStateService" not in limit


def test_feature_tower_state_application_rolls_weekly_purchases_on_new_iso_week(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_tower_state(uow)
    TowerStateApplication(
        database,
        clock=FixedClock(datetime(2020, 12, 31, 20, 0, tzinfo=timezone.utc)),
    ).get("user")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "UPDATE tower SET current_floor=?,max_floor=?,score=?,weekly_purchases=? WHERE user_id=?",
            (5, 9, 90, '{"_last_reset":"2020-12-31","7":2}', "user"),
        )

    state = TowerStateApplication(
        database,
        clock=FixedClock(datetime(2021, 1, 4, 20, 0, tzinfo=timezone.utc)),
    ).get("user")

    assert state == {
        "current_floor": 5,
        "max_floor": 9,
        "score": 90,
        "weekly_purchases": {"_last_reset": "2021-01-04"},
    }
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all(
            "SELECT operation_id,kind,period_key FROM tower_state_operations ORDER BY operation_id"
        )
    assert [(row["operation_id"], row["kind"], row["period_key"]) for row in operations] == [
        ("tower-state-init:user", "initialize", "2020-W53"),
        ("tower-state-week:user:2021-W01", "week", "2021-W01"),
    ]


def test_tower_global_floor_reset_preserves_state_and_replay_does_not_clear_new_progress(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_tower_state(uow)
        uow.execute(
            "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
            "VALUES('one',4,12,80,'{\"_last_reset\":\"2026-07-14\",\"7\":2}')"
        )
        uow.execute(
            "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
            "VALUES('two',0,6,30,'{\"_last_reset\":\"2026-07-14\"}')"
        )

    application = TowerStateApplication(database, clock=FixedClock())
    first = application.reset_all_floors(
        operation_id="tower-reset:weekly:2026-W29",
        source="scheduler",
        period_key="2026-W29",
    )
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("UPDATE tower SET current_floor=9 WHERE user_id='one'")
    replay = application.reset_all_floors(
        operation_id="tower-reset:weekly:2026-W29",
        source="scheduler",
        period_key="2026-W29",
    )
    conflict = application.reset_all_floors(
        operation_id="tower-reset:weekly:2026-W29",
        source="admin",
        period_key="2026-W29",
    )

    assert (first.status, first.total, first.changed) == ("applied", 2, 1)
    assert (replay.status, replay.total, replay.changed) == ("duplicate", 2, 1)
    assert conflict.status == "operation_conflict"
    with DatabaseUnitOfWork(database) as uow:
        rows = uow.query_all(
            "SELECT user_id,current_floor,max_floor,score,weekly_purchases FROM tower ORDER BY user_id"
        )
    assert [
        (
            row["user_id"], row["current_floor"], row["max_floor"],
            row["score"], row["weekly_purchases"],
        )
        for row in rows
    ] == [
        ("one", 9, 12, 80, '{"_last_reset":"2026-07-14","7":2}'),
        ("two", 0, 6, 30, '{"_last_reset":"2026-07-14"}'),
    ]


def test_tower_global_floor_reset_fails_closed_without_schema(tmp_path):
    missing = tmp_path / "missing.db"
    absent_schema = tmp_path / "empty.db"
    with DatabaseUnitOfWork(absent_schema):
        pass

    for database in (missing, absent_schema):
        result = TowerStateApplication(database).reset_all_floors(
            operation_id="tower-reset:admin:one",
            source="admin",
            period_key="2026-W29",
        )
        assert result.status == "schema_missing"
    assert not missing.exists()
    with DatabaseUnitOfWork(absent_schema) as uow:
        assert uow.query_all("SELECT name FROM sqlite_master WHERE type='table'") == []


def test_tower_global_floor_reset_rolls_back_when_receipt_insert_fails(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_tower_state(uow)
        uow.execute(
            "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
            "VALUES('one',4,12,80,'{}')"
        )
        uow.execute(
            "CREATE TRIGGER fail_global_tower_reset BEFORE INSERT ON tower_state_operations "
            "WHEN NEW.kind='reset_all_floors' BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
        )

    try:
        TowerStateApplication(database).reset_all_floors(
            operation_id="tower-reset:admin:one",
            source="admin",
            period_key="2026-W29",
        )
    except Exception as exc:
        assert "receipt failed" in str(exc)
    else:
        raise AssertionError("reset must fail when its receipt cannot be persisted")
    with DatabaseUnitOfWork(database) as uow:
        row = uow.query_one("SELECT current_floor FROM tower WHERE user_id='one'")
        receipt = uow.query_one(
            "SELECT operation_id FROM tower_state_operations WHERE operation_id='tower-reset:admin:one'"
        )
    assert int(row["current_floor"]) == 4
    assert receipt is None


def test_tower_global_floor_reset_rejects_malformed_receipts_without_mutating_state(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_tower_state(uow)
        uow.execute(
            "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
            "VALUES('one',4,12,80,'{}')"
        )
        uow.execute(
            "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot) "
            "VALUES('bad-list','0','reset_all_floors','2026-W29','[]')"
        )
        uow.execute(
            "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot) "
            "VALUES('bad-count','0','reset_all_floors','2026-W29',?)",
            ('{"request":{"source":"admin","period_key":"2026-W29"},'
             '"result":{"total":"many","changed":1}}',),
        )

    application = TowerStateApplication(database)
    for operation_id in ("bad-list", "bad-count"):
        result = application.reset_all_floors(
            operation_id=operation_id,
            source="admin",
            period_key="2026-W29",
        )
        assert result.status == "operation_conflict"
    with DatabaseUnitOfWork(database) as uow:
        row = uow.query_one("SELECT current_floor FROM tower WHERE user_id='one'")
    assert int(row["current_floor"]) == 4


def test_tower_rankings_query_only_top_fifty_with_stable_tie_order(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_tower_state(uow)
        uow.executemany(
            "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
            "VALUES(?, ?, 0, ?, '{}')",
            [(f"user-{index:03}", index % 5, index % 5) for index in range(60)],
        )

    application = TowerStateApplication(database)
    actual = application.ranking("current_floor", limit=500)
    expected = sorted(
        [(f"user-{index:03}", index % 5) for index in range(60)],
        key=lambda row: (-row[1], row[0]),
    )[:50]
    assert actual == expected
    assert application.ranking("score", limit=0) == []


def test_tower_global_floor_reset_scheduler_and_admin_entries_have_stable_identity():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2" / "xiuxian"
    limit = (root / "xiuxian_tower" / "tower_limit.py").read_text(encoding="utf-8")
    facade = (root / "xiuxian_tower" / "__init__.py").read_text(encoding="utf-8")
    scheduler = (root / "xiuxian_scheduler" / "__init__.py").read_text(encoding="utf-8")
    admin = (root / "xiuxian_admin" / "__init__.py").read_text(encoding="utf-8")

    assert "PlayerDataManager" not in limit
    assert "update_all_records" not in limit
    assert "await asyncio.to_thread(" in facade
    assert 'f"tower-reset:weekly:{period_key}"' in scheduler
    assert "business_date = _scheduler_business_date()" in scheduler
    assert 'f"tower-reset:admin:{operator_id}:{message_id}"' in admin
    assert "if not result.succeeded:" in admin
