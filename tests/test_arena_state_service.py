from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena.transaction_service import (
    ARENA_FIELDS,
    ArenaStateService,
)
from tests.test_db_backend import db_backend


def _operations(database: Path):
    with db_backend.connection(database) as conn:
        return conn.execute(
            "SELECT operation_id,kind,period_key,snapshot "
            "FROM arena_state_operations ORDER BY operation_id"
        ).fetchall()


def _default_state(day="2026-07-14"):
    return {
        "score": 1000,
        "total_wins": 0,
        "total_losses": 0,
        "daily_challenges_used": 0,
        "daily_extra_challenges": 0,
        "daily_challenge_buys": 0,
        "last_reset_date": day,
        "last_buy_date": day,
        "last_challenge_time": "",
        "win_streak": 0,
        "max_win_streak": 0,
        "rank": "青铜",
        "honor_points": 0,
        "total_honor_earned": 0,
        "weekly_purchases": {"_last_reset": day},
    }


def test_missing_user_is_initialized_once_with_all_fields(tmp_path):
    database = tmp_path / "player.db"
    service = ArenaStateService(database)

    state = service.get("user", date(2026, 7, 14))
    duplicate = service.get("user", date(2026, 7, 14))

    assert state == duplicate == _default_state()
    with db_backend.connection(database) as conn:
        row = conn.execute(
            f"SELECT {','.join(ARENA_FIELDS)} FROM arena WHERE user_id=%s",
            ("user",),
        ).fetchone()
        columns = {column[1] for column in conn.execute("PRAGMA table_info(arena)").fetchall()}
    assert columns.issuperset({"user_id", *ARENA_FIELDS})
    assert tuple(map(int, row[:6])) == (1000, 0, 0, 0, 0, 0)
    assert json.loads(row[-1]) == {"_last_reset": "2026-07-14"}
    operations = _operations(database)
    assert [(row[0], row[1], row[2]) for row in operations] == [
        ("arena-state-init:user", "initialize", "2026-07-14")
    ]
    assert json.loads(operations[0][3]) == state


def test_new_day_resets_only_purchase_allowance(tmp_path):
    database = tmp_path / "player.db"
    service = ArenaStateService(database)
    service.get("user", date(2026, 7, 13))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE arena SET score=1800,total_wins=5,daily_challenges_used=7,"
            "daily_extra_challenges=2,daily_challenge_buys=2,last_buy_date=%s,"
            "weekly_purchases=%s WHERE user_id=%s",
            (
                "2026-07-13",
                json.dumps({"_last_reset": "2026-07-13", "1": 3}),
                "user",
            ),
        )

    state = service.get("user", date(2026, 7, 14))
    duplicate = service.get("user", date(2026, 7, 14))

    assert state == duplicate
    assert (
        state["score"],
        state["total_wins"],
        state["daily_challenges_used"],
        state["daily_extra_challenges"],
        state["daily_challenge_buys"],
        state["last_buy_date"],
    ) == (1800, 5, 7, 0, 0, "2026-07-14")
    assert state["weekly_purchases"] == {"_last_reset": "2026-07-13", "1": 3}
    assert [(row[1], row[2]) for row in _operations(database)] == [
        ("day", "2026-07-14"),
        ("initialize", "2026-07-13"),
    ]


def test_same_iso_week_across_calendar_year_preserves_purchases(tmp_path):
    database = tmp_path / "player.db"
    service = ArenaStateService(database)
    service.get("user", date(2020, 12, 31))
    weekly = {"_last_reset": "2020-12-31", "7": 2}
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE arena SET weekly_purchases=%s WHERE user_id=%s",
            (json.dumps(weekly), "user"),
        )

    state = service.get("user", date(2021, 1, 1))

    assert state["weekly_purchases"] == weekly
    assert not any(row[1] == "week" for row in _operations(database))


def test_new_iso_week_resets_only_weekly_purchases(tmp_path):
    database = tmp_path / "player.db"
    service = ArenaStateService(database)
    service.get("user", date(2020, 12, 31))
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE arena SET score=1900,total_wins=8,honor_points=50,weekly_purchases=%s "
            "WHERE user_id=%s",
            (json.dumps({"_last_reset": "2020-12-31", "7": 2}), "user"),
        )

    state = service.get("user", date(2021, 1, 4))

    assert (state["score"], state["total_wins"], state["honor_points"]) == (1900, 8, 50)
    assert state["weekly_purchases"] == {"_last_reset": "2021-01-04"}
    assert any(
        row[0] == "arena-state-week:user:2021-W01" and row[1] == "week"
        for row in _operations(database)
    )


def test_partial_and_invalid_state_is_normalized_atomically(tmp_path):
    database = tmp_path / "player.db"
    with db_backend.transaction(database) as conn:
        conn.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,score TEXT,rank TEXT)")
        conn.execute("INSERT INTO arena VALUES(%s,%s,%s)", ("user", "bad", None))

    state = ArenaStateService(database).get("user", date(2026, 7, 14))

    assert state == _default_state()
    with db_backend.connection(database) as conn:
        columns = {column[1] for column in conn.execute("PRAGMA table_info(arena)").fetchall()}
        stored = conn.execute("SELECT score,rank,last_buy_date FROM arena").fetchone()
    assert columns.issuperset({"user_id", *ARENA_FIELDS})
    assert tuple(stored) == ("1000", "青铜", "2026-07-14")
    assert {row[1] for row in _operations(database)} == {"normalize", "week"}


def test_operation_failure_rolls_back_initialization(tmp_path):
    database = tmp_path / "player.db"
    with db_backend.transaction(database) as conn:
        conn.execute(
            "CREATE TABLE arena_state_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,created_at TIMESTAMP)"
        )
        conn.execute(
            "CREATE TRIGGER fail_arena_state BEFORE INSERT ON arena_state_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(db_backend.IntegrityError):
        ArenaStateService(database).get("user", date(2026, 7, 14))

    with db_backend.connection(database) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='arena'"
        ).fetchone() is None


def test_operation_failure_rolls_back_day_and_week_switch(tmp_path):
    database = tmp_path / "player.db"
    service = ArenaStateService(database)
    service.get("user", date(2026, 7, 14))
    previous = {"_last_reset": "2026-07-14", "1": 4}
    with db_backend.transaction(database) as conn:
        conn.execute(
            "UPDATE arena SET daily_challenge_buys=2,daily_extra_challenges=2,"
            "weekly_purchases=%s WHERE user_id=%s",
            (json.dumps(previous), "user"),
        )
        conn.execute(
            "CREATE TRIGGER fail_arena_period BEFORE INSERT ON arena_state_operations "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(db_backend.IntegrityError):
        service.get("user", date(2026, 7, 21))

    with db_backend.connection(database) as conn:
        row = conn.execute(
            "SELECT daily_challenge_buys,daily_extra_challenges,last_buy_date,"
            "weekly_purchases FROM arena WHERE user_id=%s",
            ("user",),
        ).fetchone()
    assert tuple(map(int, row[:2])) == (2, 2)
    assert row[2] == "2026-07-14"
    assert json.loads(row[3]) == previous
    assert len(_operations(database)) == 1


def test_production_facade_has_no_legacy_write_bypass():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_arena"
    facade = (root / "arena_limit.py").read_text(encoding="utf-8")
    service = (root / "transaction_service.py").read_text(encoding="utf-8")
    handler = (root / "__init__.py").read_text(encoding="utf-8")

    assert "ArenaStateService(" in facade
    assert "update_or_write_data" not in facade
    assert "update_arena_data" not in facade
    assert "update_weekly_purchase" not in facade
    assert "buy_challenge_count" not in facade
    assert "reset_daily_challenges" not in facade
    assert "BEGIN IMMEDIATE" in service
    assert "arena_state_operations" in service
    assert 'arena_info["last_buy_date"]' in handler
