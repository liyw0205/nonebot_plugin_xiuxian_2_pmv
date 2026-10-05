from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..event_context_repository import TrainingEventContextSqlRepository
from ..event_repository import TrainingEventSqlRepository
from ..event_planner import build_training_event_plan
from ..leaderboard_repository import TrainingLeaderboardSqlRepository
from ..migrations import (
    apply_training_event_operations,
    apply_training_event_player,
    apply_training_event_resolutions,
    apply_training_leaderboard_indexes,
)


class _NothingRandom:
    def __init__(self) -> None:
        self._choices = iter(("nothing", "worldly"))

    def choices(self, values, weights=None):
        return [next(self._choices)]

    def choice(self, values):
        return values[0]


def _databases(tmp_path: Path, *, users: int = 1) -> tuple[Path, Path]:
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,level TEXT,"
            "stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)"
        )
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
            "PRIMARY KEY(user_id,goods_id))"
        )
        uow.execute(
            "CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,sub_buff INTEGER DEFAULT 0,"
            "faqi_buff INTEGER DEFAULT 0,armor_buff INTEGER DEFAULT 0)"
        )
        for index in range(users):
            user_id = "u" if index == 0 else f"u{index}"
            uow.execute(
                "INSERT INTO user_xiuxian VALUES (?,?,?,?,?,?,?)",
                (user_id, f"道友{index}", "筑基初期", 100, 200, 50, 3),
            )
        apply_training_event_operations(uow)
        apply_training_event_resolutions(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_training_event_player(uow)
        for index in range(users):
            user_id = "u" if index == 0 else f"u{index}"
            uow.execute(
                "INSERT INTO training(user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
                "VALUES (?,0,NULL,0,0,0,'','{}')",
                (user_id,),
            )
    return game, player


def _plan() -> dict:
    return {
        "expected_state": {
            "progress": 0,
            "last_time": None,
            "points": 0,
            "completed": 0,
            "max_progress": 0,
            "last_event": "",
            "weekly_purchases": {},
        },
        "state": {
            "progress": 1,
            "last_time": "2026-10-06 12:00:00",
            "points": 0,
            "completed": 0,
            "max_progress": 1,
            "last_event": "已结算事件",
            "weekly_purchases": {},
        },
        "expected_user": {"stone": 100, "exp": 200, "hp": 50, "mp": 3},
        "stone_delta": 10,
        "exp_delta": 0,
        "hp_delta": 0,
        "items": [{"id": 9, "name": "灵草", "type": "药材", "amount": 1}],
        "max_goods_num": 10,
        "message": "已结算事件",
    }


def test_frozen_event_plan_survives_failed_settlement_and_resumes_without_reroll(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    repository = TrainingEventSqlRepository(game, player)
    plan_calls = 0

    def make_plan():
        nonlocal plan_calls
        plan_calls += 1
        return {"status": "ready", **_plan()}

    with patch.object(TrainingEventSqlRepository, "_apply_item", side_effect=RuntimeError("late")):
        with pytest.raises(RuntimeError, match="late"):
            repository.run_event("event-op", "u", make_plan)

    assert repository.get_plan("event-op", "u") == {"status": "frozen", "plan": _plan()}
    assert repository.resume_event("event-op", "u") == {"status": "applied", "message": "已结算事件"}
    assert plan_calls == 1
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0] == 110
        assert connection.execute("SELECT COUNT(*) FROM training_event_resolutions").fetchone()[0] == 0
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT progress FROM training WHERE user_id='u'").fetchone()[0] == 1

    assert repository.run_event("event-op", "u", lambda: pytest.fail("must replay")) == {
        "status": "duplicate",
        "message": "已结算事件",
    }


def test_event_context_treats_missing_buff_row_as_zero_without_inserting(tmp_path: Path) -> None:
    game, _ = _databases(tmp_path)
    repository = TrainingEventContextSqlRepository(game)

    context = repository.read("u")

    assert context["status"] == "ready"
    assert context["buff_info"] == {"sub_buff": 0, "faqi_buff": 0, "armor_buff": 0}
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT COUNT(*) FROM BuffInfo WHERE user_id='u'").fetchone()[0] == 0


def test_event_planner_resolves_nothing_outcome_into_state_plan() -> None:
    state = {
        "progress": 4,
        "last_time": None,
        "points": 10,
        "completed": 2,
        "max_progress": 4,
        "last_event": "old",
        "weekly_purchases": {"_last_reset": "2026-10-06"},
    }

    plan = build_training_event_plan(
        user_id="u",
        training_state=state,
        context={
            "user": {"user_id": "u", "level": "筑基初期", "exp": 200, "stone": 100, "hp": 50, "mp": 3},
            "buff_info": {"sub_buff": 0, "faqi_buff": 0, "armor_buff": 0},
            "inventory": [],
        },
        now=datetime(2026, 10, 6, 12, 0),
        items=object(),
        random_source=_NothingRandom(),
        max_goods_num=99,
    )

    assert state["progress"] == 4
    assert plan["expected_state"]["progress"] == 4
    assert plan["state"]["progress"] == 5
    assert plan["state"]["last_time"] == "2026-10-06 12:00:00"
    assert plan["state"]["last_event"].endswith("心境略有提升")
    assert plan["stone_delta"] == plan["exp_delta"] == plan["hp_delta"] == 0
    assert plan["items"] == []


def test_concurrent_duplicate_event_operation_replays_committed_result(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    repository = TrainingEventSqlRepository(game, player)
    barrier = Barrier(2)

    def run_once():
        def make_plan():
            barrier.wait(timeout=5)
            return {"status": "ready", **_plan()}

        return repository.run_event("same-operation", "u", make_plan)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: run_once(), range(2)))

    assert sorted(outcome["status"] for outcome in outcomes) == ["applied", "duplicate"]
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT stone FROM user_xiuxian WHERE user_id='u'").fetchone()[0] == 110


def test_leaderboard_returns_bounded_numeric_top_and_caches_for_ttl(tmp_path: Path) -> None:
    game, player = _databases(tmp_path, users=75)
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE training SET points=CAST(substr(user_id,2) AS INTEGER)")
        uow.execute("UPDATE training SET points=74 WHERE user_id='u'")
        apply_training_leaderboard_indexes(uow)
    now = [0.0]
    repository = TrainingLeaderboardSqlRepository(
        game, player, clock=lambda: now[0], cache_ttl=45.0
    )

    top = repository.top("points", limit=500)
    assert len(top) == 50
    assert top[0]["value"] == 74
    assert all(top[index]["value"] >= top[index + 1]["value"] for index in range(49))

    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE training SET points=100 WHERE user_id='u1'")
    assert repository.top("points")[0]["user_id"] == "u"

    now[0] = 46.0
    assert repository.top("points")[0]["user_id"] == "u1"

    with DatabaseUnitOfWork(player, read_only=True) as uow:
        plan = uow.query_all(
            "EXPLAIN QUERY PLAN SELECT user_id FROM training "
            "ORDER BY CAST(COALESCE(points,0) AS INTEGER) DESC,rowid ASC LIMIT 50"
        )
    assert any("training_points_rank_idx" in str(row["detail"]) for row in plan)
