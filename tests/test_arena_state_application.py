from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.state_application import ArenaStateApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedClock:
    def __init__(self, now=datetime(2026, 7, 14, 20, 0, tzinfo=timezone.utc)):
        self._now = now

    def now(self):
        return self._now


class TrackingLock:
    def __init__(self) -> None:
        self.entered = 0

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, _type, _value, _traceback):
        return False


def test_feature_state_application_initializes_once_with_injected_clock(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
    application = ArenaStateApplication(database, clock=FixedClock())

    state = application.get("user")
    duplicate = application.get("user")

    assert state == duplicate
    assert state["score"] == 1000
    assert state["rank"] == "青铜"
    assert state["last_buy_date"] == "2026-07-14"
    assert state["weekly_purchases"] == {"_last_reset": "2026-07-14"}
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all(
            "SELECT operation_id,kind,period_key FROM arena_state_operations ORDER BY operation_id"
        )
    assert [(row["operation_id"], row["kind"], row["period_key"]) for row in operations] == [
        ("arena-state-init:user", "initialize", "2026-07-14"),
    ]


def test_feature_state_application_serializes_get_with_injected_lock(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
    lock = TrackingLock()
    application = ArenaStateApplication(database, clock=FixedClock(), lock=lock)

    application.get("user")

    assert lock.entered == 1


def test_arena_limit_defaults_to_feature_state_application():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_arena"
    source = (root / "arena_limit.py").read_text(encoding="utf-8")

    assert "ArenaStateApplication" in source
    assert "ArenaStateService" not in source
    assert "state_application.get(user_id)" in source


def test_feature_state_application_reads_descending_ranking(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
        uow.executemany(
            "INSERT INTO arena(user_id,score,rank) VALUES(?,?,?)",
            (("low", 1200, "青铜"), ("high", 3200, "王者"), ("middle", 1900, "黄金")),
        )

    ranking = ArenaStateApplication(database, clock=FixedClock()).ranking(limit=2)

    assert ranking == (("high", 3200), ("middle", 1900))


def test_arena_limit_ranking_defaults_to_feature_state_application():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_arena"
    source = (root / "arena_limit.py").read_text(encoding="utf-8")

    assert "state_application.ranking(limit)" in source
    assert "get_all_field_data(self.table_name, \"score\")" not in source


def test_feature_state_application_rolls_purchase_allowance_on_new_day(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
    day_one = ArenaStateApplication(
        database,
        clock=FixedClock(datetime(2026, 7, 13, 20, 0, tzinfo=timezone.utc)),
    )
    day_one.get("user")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "UPDATE arena SET score=?,total_wins=?,daily_challenges_used=?,"
            "daily_extra_challenges=?,daily_challenge_buys=?,last_buy_date=?,"
            "weekly_purchases=? WHERE user_id=?",
            (1800, 5, 7, 2, 2, "2026-07-13", '{"_last_reset":"2026-07-13","1":3}', "user"),
        )

    state = ArenaStateApplication(
        database,
        clock=FixedClock(datetime(2026, 7, 14, 20, 0, tzinfo=timezone.utc)),
    ).get("user")

    assert (
        state["score"], state["total_wins"], state["daily_challenges_used"],
        state["daily_extra_challenges"], state["daily_challenge_buys"], state["last_buy_date"],
    ) == (1800, 5, 7, 0, 0, "2026-07-14")
    assert state["weekly_purchases"] == {"_last_reset": "2026-07-13", "1": 3}
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all("SELECT kind,period_key FROM arena_state_operations ORDER BY operation_id")
    assert [(row["kind"], row["period_key"]) for row in operations] == [
        ("day", "2026-07-14"), ("initialize", "2026-07-13")
    ]


def test_feature_state_application_rolls_weekly_purchases_on_new_iso_week(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
    application = ArenaStateApplication(
        database,
        clock=FixedClock(datetime(2020, 12, 31, 20, 0, tzinfo=timezone.utc)),
    )
    application.get("user")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "UPDATE arena SET score=?,total_wins=?,honor_points=?,weekly_purchases=? WHERE user_id=?",
            (1900, 8, 50, '{"_last_reset":"2020-12-31","7":2}', "user"),
        )

    state = ArenaStateApplication(
        database,
        clock=FixedClock(datetime(2021, 1, 4, 20, 0, tzinfo=timezone.utc)),
    ).get("user")

    assert (state["score"], state["total_wins"], state["honor_points"]) == (1900, 8, 50)
    assert state["weekly_purchases"] == {"_last_reset": "2021-01-04"}
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all("SELECT operation_id,kind,period_key FROM arena_state_operations ORDER BY operation_id")
    assert any(
        row["operation_id"] == "arena-state-week:user:2021-W01"
        and row["kind"] == "week"
        and row["period_key"] == "2021-W01"
        for row in operations
    )


def test_feature_state_application_preserves_purchases_within_cross_year_iso_week(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
    ArenaStateApplication(
        database,
        clock=FixedClock(datetime(2020, 12, 31, 20, 0, tzinfo=timezone.utc)),
    ).get("user")
    weekly = {"_last_reset": "2020-12-31", "7": 2}
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "UPDATE arena SET weekly_purchases=? WHERE user_id=?",
            ('{"_last_reset":"2020-12-31","7":2}', "user"),
        )

    state = ArenaStateApplication(
        database,
        clock=FixedClock(datetime(2021, 1, 1, 20, 0, tzinfo=timezone.utc)),
    ).get("user")

    assert state["weekly_purchases"] == weekly
    with DatabaseUnitOfWork(database) as uow:
        operations = uow.query_all("SELECT kind FROM arena_state_operations ORDER BY operation_id")
    assert all(row["kind"] != "week" for row in operations)


def test_feature_state_application_normalizes_malformed_weekly_members_in_current_iso_week(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
        uow.execute(
            "INSERT INTO arena(user_id,weekly_purchases) VALUES(?,?)",
            ("user", '{"_last_reset":"2026-07-14","1":"2","bad":-1,"flag":true}'),
        )

    state = ArenaStateApplication(database, clock=FixedClock()).get("user")

    assert state["weekly_purchases"] == {"_last_reset": "2026-07-14", "1": 2, "flag": 1}
    with DatabaseUnitOfWork(database) as uow:
        stored = uow.query_one("SELECT weekly_purchases FROM arena WHERE user_id=?", ("user",))
        operations = uow.query_all("SELECT kind,period_key FROM arena_state_operations ORDER BY operation_id")
    assert stored["weekly_purchases"] == '{"1": 2, "_last_reset": "2026-07-14", "flag": 1}'
    assert [(row["kind"], row["period_key"]) for row in operations] == [
        ("normalize", "2026-07-14"),
        ("week", "2026-W29"),
    ]


def test_feature_state_application_normalizes_invalid_legacy_values_atomically(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_arena_state(uow)
        uow.execute(
            "INSERT INTO arena(user_id,score,rank,weekly_purchases) VALUES(?,?,?,?)",
            ("user", "bad", None, "not-json"),
        )

    state = ArenaStateApplication(database, clock=FixedClock()).get("user")

    assert state["score"] == 1000
    assert state["rank"] == "青铜"
    assert state["last_buy_date"] == "2026-07-14"
    assert state["weekly_purchases"] == {"_last_reset": "2026-07-14"}
    with DatabaseUnitOfWork(database) as uow:
        stored = uow.query_one("SELECT score,rank,last_buy_date FROM arena WHERE user_id=?", ("user",))
        operations = uow.query_all("SELECT kind FROM arena_state_operations ORDER BY kind")
    assert (stored["score"], stored["rank"], stored["last_buy_date"]) == (1000, "青铜", "2026-07-14")
    assert [row["kind"] for row in operations] == ["normalize", "week"]
