from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.training.migrations import apply_training_state
from nonebot_plugin_xiuxian_2.features.training.state_application import TrainingStateApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedClock:
    def __init__(self, now=datetime(2026, 7, 14, 20, 0, tzinfo=timezone.utc)):
        self._now = now

    def now(self):
        return self._now


def test_feature_training_state_application_initializes_once_with_injected_clock(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_training_state(uow)
    application = TrainingStateApplication(database, clock=FixedClock())

    state = application.get("user")
    duplicate = application.get("user")

    assert state == duplicate == {
        "progress": 0,
        "last_time": None,
        "points": 0,
        "completed": 0,
        "max_progress": 0,
        "last_event": "",
        "weekly_purchases": {"_last_reset": "2026-07-14"},
    }
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all(
            "SELECT operation_id,kind,period_key FROM training_state_operations ORDER BY operation_id"
        )
    assert [(row["operation_id"], row["kind"], row["period_key"]) for row in operations] == [
        ("training-state-init:user", "initialize", "2026-W29"),
    ]


def test_training_limit_defaults_to_feature_state_application():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_training"
    source = (root / "training_limit.py").read_text(encoding="utf-8")

    assert "TrainingStateApplication" in source
    assert "TrainingStateService" not in source
    assert "state_application.get(user_id)" in source


def test_feature_training_state_application_rolls_weekly_purchases_on_new_iso_week(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_training_state(uow)
    TrainingStateApplication(
        database,
        clock=FixedClock(datetime(2020, 12, 31, 20, 0, tzinfo=timezone.utc)),
    ).get("user")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "UPDATE training SET progress=?,last_time=?,points=?,completed=?,max_progress=?,last_event=?,weekly_purchases=? WHERE user_id=?",
            (5, "2020-12-31 20:30:00", 90, 4, 9, "event", '{"_last_reset":"2020-12-31","7":2}', "user"),
        )

    state = TrainingStateApplication(
        database,
        clock=FixedClock(datetime(2021, 1, 4, 20, 0, tzinfo=timezone.utc)),
    ).get("user")

    assert state["progress"] == 5
    assert state["points"] == 90
    assert state["weekly_purchases"] == {"_last_reset": "2021-01-04"}
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all(
            "SELECT operation_id,kind,period_key FROM training_state_operations ORDER BY operation_id"
        )
    assert [(row["operation_id"], row["kind"], row["period_key"]) for row in operations] == [
        ("training-state-init:user", "initialize", "2020-W53"),
        ("training-state-week:user:2021-W01", "week", "2021-W01"),
    ]
