from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database

from ..application import TrainingApplication
from ..migrations import apply_training_event_player, apply_training_purchase_operations
from ..purchase_repository import TrainingPurchaseSqlRepository


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 27, 12, 34, 56, tzinfo=timezone.utc)


def _databases(tmp_path: Path) -> tuple[Path, Path]:
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER DEFAULT 0)"
        )
        uow.execute(
            "CREATE TABLE back("
            "user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,"
            "create_time TEXT,update_time TEXT,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
        )
        uow.execute("INSERT INTO user_xiuxian(user_id) VALUES ('u')")
        apply_training_purchase_operations(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_training_event_player(uow)
        uow.execute(
            "INSERT INTO training(user_id,points,weekly_purchases) VALUES (?,?,?)",
            ("u", 100, json.dumps({"_last_reset": "2026-09-27", "7": 1})),
        )
    return game, player


def _request(**overrides: object) -> dict[str, object]:
    request: dict[str, object] = {
        "item_id": 7,
        "item_name": "草药",
        "item_type": "药材",
        "quantity": 2,
        "unit_cost": 10,
        "weekly_limit": 5,
        "expected_points": 100,
        "expected_weekly_purchases": {"_last_reset": "2026-09-27", "7": 1},
        "max_goods_num": 99,
        "bind_flag": 1,
        "today": "2026-09-27",
    }
    request.update(overrides)
    return request


def _repo(tmp_path: Path) -> tuple[TrainingPurchaseSqlRepository, Path, Path]:
    game, player = _databases(tmp_path)
    return TrainingPurchaseSqlRepository(game, player, clock=FixedClock()), game, player


def test_purchase_updates_points_inventory_and_replays(tmp_path: Path) -> None:
    repo, game, player = _repo(tmp_path)
    first = repo.purchase("purchase", "u", **_request())
    duplicate = repo.purchase("purchase", "u", **_request())
    conflict = repo.purchase("purchase", "u", **_request(quantity=1))

    assert (first.status, first.cost, first.points, first.purchased, first.inventory) == (
        "applied", 20, 80, 3, 2
    )
    assert duplicate.status == "duplicate"
    assert conflict.status == "state_changed"
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT goods_num,bind_num FROM back").fetchone() == (2, 2)
        assert connection.execute("SELECT update_time FROM back").fetchone()[0].startswith(
            "2026-09-27 12:34:56"
        )
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT points,weekly_purchases FROM training").fetchone() == (
            80,
            '{"7":3,"_last_reset":"2026-09-27"}',
        )


@pytest.mark.parametrize(
    ("overrides", "status"),
    [
        ({"unit_cost": 60}, "points_insufficient"),
        ({"quantity": 5}, "limit_reached"),
        ({"max_goods_num": 1}, "inventory_full"),
        ({"expected_points": 99}, "state_changed"),
    ],
)
def test_purchase_rejections_leave_both_databases_unchanged(
    tmp_path: Path, overrides: dict[str, object], status: str
) -> None:
    repo, game, player = _repo(tmp_path)
    result = repo.purchase("reject", "u", **_request(**overrides))
    assert result.status == status
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT COUNT(*) FROM back").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM training_purchase_operations"
        ).fetchone()[0] == 0
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT points FROM training").fetchone()[0] == 100


def test_late_sql_failure_rolls_back_cross_database_state(tmp_path: Path) -> None:
    repo, game, player = _repo(tmp_path)
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "CREATE TRIGGER fail_training_purchase BEFORE INSERT ON "
            "training_purchase_operations BEGIN SELECT RAISE(ABORT, 'failed'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="failed"):
        repo.purchase("rollback", "u", **_request())
    with sqlite3.connect(game) as connection:
        assert connection.execute("SELECT COUNT(*) FROM back").fetchone()[0] == 0
    with sqlite3.connect(player) as connection:
        assert connection.execute("SELECT points FROM training").fetchone()[0] == 100


def test_legacy_payload_is_read_without_request_ddl(tmp_path: Path) -> None:
    repo, game, _ = _repo(tmp_path)
    request = _request()
    payload = repo._payload(
        "u", 7, "草药", "药材", 2, 10, 5, 100,
        {"_last_reset": "2026-09-27", "7": 1}, 99, 1,
    )
    with DatabaseUnitOfWork(game) as uow:
        uow.execute(
            "INSERT INTO training_purchase_operations"
            "(operation_id,payload,quantity,cost,points,purchased,inventory) "
            "VALUES(?,?,?,?,?,?,?)",
            ("legacy", json.dumps(json.loads(payload), ensure_ascii=False, indent=2), 2, 20, 80, 3, 2),
        )
    with sqlite3.connect(game) as connection:
        before = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='training_purchase_operations'"
        ).fetchone()[0]
    assert repo.purchase("legacy", "u", **request).status == "duplicate"
    with sqlite3.connect(game) as connection:
        after = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='training_purchase_operations'"
        ).fetchone()[0]
    assert before == after


def test_missing_operation_schema_is_rejected_without_request_ddl(tmp_path: Path) -> None:
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES ('u')")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,points INTEGER,weekly_purchases TEXT)")
        uow.execute("INSERT INTO training VALUES ('u',100,'{}')")
    result = TrainingPurchaseSqlRepository(game, player).purchase(
        "missing", "u", **_request(expected_weekly_purchases={})
    )
    assert result.status == "schema_missing"
    with sqlite3.connect(game) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name='training_purchase_operations'"
        ).fetchone() is None


def test_application_default_purchase_uses_feature_repository(tmp_path: Path) -> None:
    game, player = _databases(tmp_path)
    outcome = TrainingApplication(game, player, clock=FixedClock()).execute(
        operation_id="application-purchase",
        user_id="u",
        payload={"action": "purchase", **_request()},
    )
    assert outcome.status == "applied"
    assert outcome.data["status"] == "applied"


def test_training_purchase_migration_is_game_only() -> None:
    migrations = build_migrations()
    assert [item.version for item in migrations_for_database(migrations, "game_db") if item.version.startswith("training.")] == [
        "training.001", "training.003", "training.004"
    ]
    assert [item.version for item in migrations_for_database(migrations, "player_db") if item.version.startswith("training.")] == [
        "training.002"
    ]


def test_purchase_migration_backfills_legacy_operation_columns(tmp_path: Path) -> None:
    game = tmp_path / "game.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE training_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
        apply_training_purchase_operations(uow)
        columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(training_purchase_operations)")
        }
    assert {"operation_id", "payload", "quantity", "cost", "points", "purchased", "inventory"}.issubset(columns)
