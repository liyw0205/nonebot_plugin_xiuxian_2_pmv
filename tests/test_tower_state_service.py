from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tower.transaction_service import (
    TOWER_FIELDS,
    TowerStateService,
)
from tests.test_db_backend import db_backend


def _operations(database: Path):
    with db_backend.connection(database) as conn:
        return conn.execute(
            "SELECT operation_id,kind,period_key,snapshot "
            "FROM tower_state_operations ORDER BY operation_id"
        ).fetchall()


def test_missing_user_is_initialized_once_with_complete_state(tmp_path):
    database = tmp_path / "player.db"
    service = TowerStateService(database)

    state = service.get("user", date(2026, 7, 14))
    duplicate = service.get("user", date(2026, 7, 14))

    assert state == duplicate == {
        "current_floor": 0,
        "max_floor": 0,
        "score": 0,
        "weekly_purchases": {"_last_reset": "2026-07-14"},
    }
    with db_backend.connection(database) as conn:
        row = conn.execute(
            "SELECT current_floor,max_floor,score,weekly_purchases "
            "FROM tower WHERE user_id=%s",
            ("user",),
        ).fetchone()
        columns = {column[1] for column in conn.execute("PRAGMA table_info(tower)").fetchall()}
    assert columns.issuperset({"user_id", *TOWER_FIELDS})
    assert tuple(row[:3]) == ("0", "0", "0")
    assert json.loads(row[3]) == {"_last_reset": "2026-07-14"}
    operations = _operations(database)
    assert [(row[0], row[1], row[2]) for row in operations] == [
        ("tower-state-init:user", "initialize", "2026-W29")
    ]
    assert json.loads(operations[0][3]) == state


def test_same_iso_week_across_calendar_year_does_not_reset(tmp_path):
    database = tmp_path / "player.db"
    service = TowerStateService(database)
    service.get("user", date(2020, 12, 31))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE tower SET current_floor=5,max_floor=9,score=90,weekly_purchases=%s "
            "WHERE user_id=%s",
            (json.dumps({"_last_reset": "2020-12-31", "7": 2}), "user"),
        )

    state = service.get("user", date(2021, 1, 1))

    assert state["weekly_purchases"] == {"_last_reset": "2020-12-31", "7": 2}
    assert len(_operations(database)) == 1


def test_new_iso_week_resets_only_purchases_and_is_idempotent(tmp_path):
    database = tmp_path / "player.db"
    service = TowerStateService(database)
    service.get("user", date(2020, 12, 31))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE tower SET current_floor=5,max_floor=9,score=90,weekly_purchases=%s "
            "WHERE user_id=%s",
            (json.dumps({"_last_reset": "2020-12-31", "7": 2}), "user"),
        )

    state = service.get("user", date(2021, 1, 4))
    duplicate = service.get("user", date(2021, 1, 4))

    assert state == duplicate == {
        "current_floor": 5,
        "max_floor": 9,
        "score": 90,
        "weekly_purchases": {"_last_reset": "2021-01-04"},
    }
    assert [(row[0], row[1], row[2]) for row in _operations(database)] == [
        ("tower-state-init:user", "initialize", "2020-W53"),
        ("tower-state-week:user:2021-W01", "week", "2021-W01"),
    ]


@pytest.mark.parametrize(
    "stored,expected",
    [
        ("{broken", {"_last_reset": "2026-07-14"}),
        (
            json.dumps({"_last_reset": "2026-07-14", "1": "2", "bad": "x"}),
            {"_last_reset": "2026-07-14", "1": 2},
        ),
    ],
)
def test_invalid_weekly_snapshot_is_repaired(stored, expected, tmp_path):
    database = tmp_path / "player.db"
    service = TowerStateService(database)
    service.get("user", date(2026, 7, 14))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE tower SET weekly_purchases=%s WHERE user_id=%s",
            (stored, "user"),
        )

    state = service.get("user", date(2026, 7, 14))

    assert state["weekly_purchases"] == expected
    with db_backend.connection(database) as conn:
        stored_weekly = conn.execute(
            "SELECT weekly_purchases FROM tower WHERE user_id=%s", ("user",)
        ).fetchone()[0]
    assert json.loads(stored_weekly) == expected
    assert len(_operations(database)) == 2


def test_existing_partial_row_is_completed_in_same_transaction(tmp_path):
    database = tmp_path / "player.db"
    with db_backend.transaction(database) as conn:
        conn.execute("CREATE TABLE tower(user_id TEXT PRIMARY KEY,current_floor TEXT)")
        conn.execute("INSERT INTO tower VALUES(%s,%s)", ("user", 3))

    state = TowerStateService(database).get("user", date(2026, 7, 14))

    assert state == {
        "current_floor": 3,
        "max_floor": 0,
        "score": 0,
        "weekly_purchases": {"_last_reset": "2026-07-14"},
    }
    with db_backend.connection(database) as conn:
        columns = {column[1] for column in conn.execute("PRAGMA table_info(tower)").fetchall()}
    assert columns.issuperset({"user_id", *TOWER_FIELDS})


def test_operation_failure_rolls_back_initialization(tmp_path):
    database = tmp_path / "player.db"
    with db_backend.transaction(database) as conn:
        conn.execute(
            "CREATE TABLE tower_state_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,created_at TIMESTAMP)"
        )
        conn.execute(
            "CREATE TRIGGER fail_tower_state BEFORE INSERT ON tower_state_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(db_backend.IntegrityError):
        TowerStateService(database).get("user", date(2026, 7, 14))

    with db_backend.connection(database) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tower'"
        ).fetchone() is None


def test_operation_failure_rolls_back_week_switch(tmp_path):
    database = tmp_path / "player.db"
    service = TowerStateService(database)
    service.get("user", date(2026, 7, 14))
    previous = {"_last_reset": "2026-07-14", "1": 4}
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE tower SET weekly_purchases=%s WHERE user_id=%s",
            (json.dumps(previous), "user"),
        )
        conn.execute(
            "CREATE TRIGGER fail_tower_week BEFORE INSERT ON tower_state_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(db_backend.IntegrityError):
        service.get("user", date(2026, 7, 21))

    with db_backend.connection(database) as conn:
        stored = conn.execute(
            "SELECT weekly_purchases FROM tower WHERE user_id=%s", ("user",)
        ).fetchone()[0]
    assert json.loads(stored) == previous
    assert len(_operations(database)) == 1


def test_production_facade_has_no_per_field_write_bypass():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_tower"
    facade = (root / "tower_limit.py").read_text(encoding="utf-8")
    service = (root / "transaction_service.py").read_text(encoding="utf-8")

    assert "TowerStateService(" in facade
    assert "update_or_write_data" not in facade
    assert "save_user_tower_info" not in facade
    assert "update_weekly_purchase" not in facade
    assert "BEGIN IMMEDIATE" in service
    assert "tower_state_operations" in service
