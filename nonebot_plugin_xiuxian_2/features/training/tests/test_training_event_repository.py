from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from ..event_repository import TrainingEventSqlRepository
from ..migrations import apply_training_event_operations, apply_training_event_player
from ..application import TrainingApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


def _databases(tmp_path: Path) -> tuple[Path, Path]:
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian("
            "user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)"
        )
        uow.execute(
            "CREATE TABLE back("
            "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,"
            "create_time TEXT,update_time TEXT,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
        )
        uow.execute("INSERT INTO user_xiuxian VALUES ('u',100,200,50,3)")
        apply_training_event_operations(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_training_event_player(uow)
        uow.execute(
            "INSERT INTO training(user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
            "VALUES ('u',1,'2026-01-01',2,0,1,'old','{}')"
        )
    return game, player


def _request(repo: TrainingEventSqlRepository, operation_id: str = "op") -> dict:
    return {
        "operation_id": operation_id,
        "user_id": "u",
        "expected_state": {
            "progress": 1,
            "last_time": "2026-01-01",
            "points": 2,
            "completed": 0,
            "max_progress": 1,
            "last_event": "old",
            "weekly_purchases": {},
        },
        "state": {
            "progress": 2,
            "last_time": "2026-01-02",
            "points": 2,
            "completed": 0,
            "max_progress": 2,
            "last_event": "new",
            "weekly_purchases": {},
        },
        "expected_user": {"stone": 100, "exp": 200, "hp": 50, "mp": 3},
        "stone_delta": 10,
        "items": ({"id": 7, "name": "草药", "type": "药材", "amount": 1},),
        "max_goods_num": 99,
    }


def test_event_apply_is_atomic_and_replayable(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    repo = TrainingEventSqlRepository(game, player)
    request = _request(repo)

    assert repo.apply(**request) == {"status": "applied", "message": "new"}
    assert repo.apply(**request) == {"status": "duplicate", "message": "new"}
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT stone FROM user_xiuxian").fetchone()[0] == 110
        assert connection.execute("SELECT goods_num,bind_num FROM back").fetchone() == (1, 1)
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT progress FROM training").fetchone()[0] == 2
        assert connection.execute('SELECT "历练次数" FROM statistics').fetchone()[0] == 1


def test_event_apply_rejects_conflict_and_changed_state(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    repo = TrainingEventSqlRepository(game, player)
    request = _request(repo)
    request = {**request, "operation_id": "conflict"}
    assert repo.apply(**request)["status"] == "applied"
    changed = dict(request)
    changed["state"] = {**request["state"], "points": 3}
    assert repo.apply(**changed)["status"] == "operation_conflict"
    changed = _request(repo, "changed-state")
    changed["expected_state"] = {**changed["expected_state"], "points": 99}
    assert repo.apply(**changed)["status"] == "state_changed"


@pytest.mark.parametrize(
    ("field", "value", "status"),
    [
        ("items", ({"id": 7, "name": "草药", "type": "药材", "amount": -1},), "item_missing"),
        ("items", ({"id": 7, "name": "草药", "type": "药材", "amount": 1},), "inventory_full"),
        ("stone_delta", -101, "resource_missing"),
    ],
)
def test_event_apply_rejects_missing_resources(tmp_path: Path, field: str, value: object, status: str) -> None:
    game, player = _databases(tmp_path)
    repo = TrainingEventSqlRepository(game, player)
    request = _request(repo, status)
    if status == "inventory_full":
        request["max_goods_num"] = 0
    request[field] = value
    assert repo.apply(**request)["status"] == status


def test_late_sql_failure_rolls_back_both_databases(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    repo = TrainingEventSqlRepository(game, player)
    request = _request(repo, "late-failure")
    with patch.object(TrainingEventSqlRepository, "_apply_item", side_effect=RuntimeError("late")):
        with pytest.raises(RuntimeError, match="late"):
            repo.apply(**request)
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT stone FROM user_xiuxian").fetchone()[0] == 100
        assert connection.execute("SELECT COUNT(*) FROM training_event_operations").fetchone()[0] == 0
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT progress FROM training").fetchone()[0] == 1
        assert connection.execute('SELECT COUNT(*) FROM statistics').fetchone()[0] == 0


def test_legacy_operation_rows_are_read_without_request_ddl(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    repo = TrainingEventSqlRepository(game, player)
    request = _request(repo, "legacy")
    rewards = ((7, "草药", "药材", 1),)
    payload = repo._payload(
        "u", request["expected_state"], request["state"], request["expected_user"],
        request["stone_delta"], request.get("exp_delta", 0), request.get("hp_delta", 0), rewards,
        request["max_goods_num"],
    )
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("DROP TABLE training_event_operations")
        uow.execute("CREATE TABLE training_event_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
        uow.execute("INSERT INTO training_event_operations VALUES (?,?)", ("legacy", payload))
    with sqlite3.connect(game) as connection:
        before = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='training_event_operations'"
        ).fetchone()[0]
    assert repo.apply(**request) == {"status": "duplicate", "message": "new"}
    with sqlite3.connect(game) as connection:
        after = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='training_event_operations'"
        ).fetchone()[0]
    assert before == after
    fresh = {**request, "operation_id": "legacy-new"}
    assert repo.apply(**fresh) == {"status": "applied", "message": "new"}


def test_application_routes_event_apply_to_feature_repository(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    request = _request(TrainingEventSqlRepository(game, player), "application")
    request.pop("operation_id")
    request.pop("user_id")
    outcome = TrainingApplication(game, player).execute(
        operation_id="application",
        user_id="u",
        payload={"action": "event_apply", **request},
    )
    assert outcome.status == "applied"
    assert outcome.data["status"] == "applied"


def test_training_migrations_are_routed_to_their_own_databases() -> None:
    migrations = build_migrations()
    assert [item.version for item in migrations_for_database(migrations, "game_db") if item.version.startswith("training.")] == ["training.001"]
    assert [item.version for item in migrations_for_database(migrations, "player_db") if item.version.startswith("training.")] == ["training.002"]
