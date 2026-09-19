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
    assert "lock=_player_data_manager().lock" in limit
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
