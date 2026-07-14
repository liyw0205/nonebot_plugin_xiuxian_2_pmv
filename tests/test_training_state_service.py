from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training.state_service import (
    TRAINING_FIELDS,
    TrainingStateService,
)
from tests.test_db_backend import db_backend


def _operation_rows(database: Path):
    with db_backend.connection(database) as conn:
        return conn.execute(
            "SELECT operation_id,kind,period_key,snapshot "
            "FROM training_state_operations ORDER BY operation_id"
        ).fetchall()


def test_missing_user_is_initialized_once_with_a_complete_snapshot(tmp_path):
    database = tmp_path / "player.db"
    service = TrainingStateService(database)

    state = service.get("user", date(2026, 7, 14))
    duplicate = service.get("user", date(2026, 7, 14))

    assert state == duplicate == {
        "progress": 0,
        "last_time": None,
        "points": 0,
        "completed": 0,
        "max_progress": 0,
        "last_event": "",
        "weekly_purchases": {"_last_reset": "2026-07-14"},
    }
    with db_backend.connection(database) as conn:
        row = conn.execute(
            "SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases "
            "FROM training WHERE user_id=%s",
            ("user",),
        ).fetchone()
        columns = {column[1] for column in conn.execute("PRAGMA table_info(training)").fetchall()}
    assert columns.issuperset({"user_id", *TRAINING_FIELDS})
    assert tuple(row[:6]) == ("0", None, "0", "0", "0", "")
    assert json.loads(row[6]) == {"_last_reset": "2026-07-14"}
    operations = _operation_rows(database)
    assert [(row[0], row[1], row[2]) for row in operations] == [
        ("training-state-init:user", "initialize", "2026-W29")
    ]
    assert json.loads(operations[0][3]) == state


def test_same_iso_week_across_calendar_year_does_not_reset(tmp_path):
    database = tmp_path / "player.db"
    service = TrainingStateService(database)
    service.get("user", date(2020, 12, 31))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE training SET progress=%s,last_time=%s,points=%s,completed=%s,"
            "max_progress=%s,last_event=%s,weekly_purchases=%s WHERE user_id=%s",
            (
                5,
                "2020-12-31 20:30:00",
                90,
                4,
                9,
                "event",
                json.dumps({"_last_reset": "2020-12-31", "7": 2}),
                "user",
            ),
        )

    same_week = service.get("user", date(2021, 1, 1))

    assert same_week["weekly_purchases"] == {"_last_reset": "2020-12-31", "7": 2}
    assert len(_operation_rows(database)) == 1


def test_new_iso_week_resets_only_purchases_and_is_idempotent(tmp_path):
    database = tmp_path / "player.db"
    service = TrainingStateService(database)
    service.get("user", date(2020, 12, 31))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE training SET progress=5,last_time=%s,points=90,completed=4,"
            "max_progress=9,last_event='event',weekly_purchases=%s WHERE user_id='user'",
            ("2020-12-31 20:30:00", json.dumps({"_last_reset": "2020-12-31", "7": 2})),
        )

    state = service.get("user", date(2021, 1, 4))
    duplicate = service.get("user", date(2021, 1, 4))

    assert state == duplicate
    assert state == {
        "progress": 5,
        "last_time": datetime(2020, 12, 31, 20, 30),
        "points": 90,
        "completed": 4,
        "max_progress": 9,
        "last_event": "event",
        "weekly_purchases": {"_last_reset": "2021-01-04"},
    }
    operations = _operation_rows(database)
    assert [(row[0], row[1], row[2]) for row in operations] == [
        ("training-state-init:user", "initialize", "2020-W53"),
        ("training-state-week:user:2021-W01", "week", "2021-W01"),
    ]


@pytest.mark.parametrize(
    "stored",
    ["{broken", json.dumps({"_last_reset": "2026-07-14", "1": "2", "bad": "x"})],
)
def test_invalid_weekly_snapshot_is_repaired(stored, tmp_path):
    database = tmp_path / "player.db"
    service = TrainingStateService(database)
    service.get("user", date(2026, 7, 14))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE training SET weekly_purchases=%s WHERE user_id=%s",
            (stored, "user"),
        )

    state = service.get("user", date(2026, 7, 14))

    expected = {"_last_reset": "2026-07-14"}
    if stored != "{broken":
        expected["1"] = 2
    assert state["weekly_purchases"] == expected
    with db_backend.connection(database) as conn:
        stored_weekly = conn.execute(
            "SELECT weekly_purchases FROM training WHERE user_id=%s", ("user",)
        ).fetchone()[0]
    assert json.loads(stored_weekly) == expected
    assert len(_operation_rows(database)) == 2


def test_existing_partial_row_is_completed_in_the_same_transaction(tmp_path):
    database = tmp_path / "player.db"
    with db_backend.transaction(database) as conn:
        conn.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,progress TEXT)")
        conn.execute("INSERT INTO training VALUES(%s,%s)", ("user", 3))

    state = TrainingStateService(database).get("user", date(2026, 7, 14))

    assert state["progress"] == 3
    assert state["points"] == state["completed"] == state["max_progress"] == 0
    assert state["weekly_purchases"] == {"_last_reset": "2026-07-14"}
    with db_backend.connection(database) as conn:
        columns = {column[1] for column in conn.execute("PRAGMA table_info(training)").fetchall()}
    assert columns.issuperset({"user_id", *TRAINING_FIELDS})


def test_operation_failure_rolls_back_initialization(tmp_path):
    database = tmp_path / "player.db"
    with db_backend.transaction(database) as conn:
        conn.execute(
            "CREATE TABLE training_state_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,created_at TIMESTAMP)"
        )
        conn.execute(
            "CREATE TRIGGER fail_training_state BEFORE INSERT ON training_state_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(db_backend.IntegrityError):
        TrainingStateService(database).get("user", date(2026, 7, 14))

    with db_backend.connection(database) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='training'"
        ).fetchone() is None


def test_operation_failure_rolls_back_week_switch(tmp_path):
    database = tmp_path / "player.db"
    service = TrainingStateService(database)
    service.get("user", date(2026, 7, 14))
    previous = {"_last_reset": "2026-07-14", "1": 4}
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE training SET weekly_purchases=%s WHERE user_id=%s",
            (json.dumps(previous), "user"),
        )
        conn.execute(
            "CREATE TRIGGER fail_training_week BEFORE INSERT ON training_state_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(db_backend.IntegrityError):
        service.get("user", date(2026, 7, 21))

    with db_backend.connection(database) as conn:
        stored = conn.execute(
            "SELECT weekly_purchases FROM training WHERE user_id=%s", ("user",)
        ).fetchone()[0]
    assert json.loads(stored) == previous
    assert len(_operation_rows(database)) == 1


def test_production_facade_has_no_per_field_write_bypass():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_training"
    facade = (root / "training_limit.py").read_text(encoding="utf-8")
    service = (root / "state_service.py").read_text(encoding="utf-8")

    assert "TrainingStateService(" in facade
    assert "update_or_write_data" not in facade
    assert "save_user_training_info" not in facade
    assert "update_weekly_purchase" not in facade
    assert "BEGIN IMMEDIATE" in service
    assert "training_state_operations" in service
