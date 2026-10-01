from __future__ import annotations

from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..migrations import apply_base_root_reroll_operations
from ..root_reroll_repository import BaseRootRerollSqlRepository


@pytest.fixture
def root_reroll_db(tmp_path: Path) -> Path:
    database = tmp_path / "game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian("
            "user_id TEXT,root TEXT,root_type TEXT,root_level INTEGER,level TEXT,"
            "exp INTEGER,power INTEGER,stone INTEGER)"
        )
        uow.execute(
            "INSERT INTO user_xiuxian VALUES('u','旧根','木灵根',0,'练气一层',123,100,100)"
        )
        apply_base_root_reroll_operations(uow)
    return database


def _snapshot(*, stone: int = 100) -> dict[str, object]:
    return {
        "root": "旧根",
        "root_type": "木灵根",
        "root_level": 0,
        "level": "练气一层",
        "exp": 123,
        "power": 100,
        "stone": stone,
    }


def test_reroll_updates_root_power_and_stone_once(root_reroll_db: Path) -> None:
    repository = BaseRootRerollSqlRepository(root_reroll_db)

    first = repository.reroll(
        "reroll-1", "u", _snapshot(), "新根", "火灵根", 25, 2.5, 1.25
    )
    with DatabaseUnitOfWork(root_reroll_db, immediate=True) as uow:
        apply_base_root_reroll_operations(uow)
    replay = repository.reroll(
        "reroll-1", "u", _snapshot(), "新根", "火灵根", 25, 2.5, 1.25
    )

    assert (first.status, first.root, first.root_type, first.power, first.stone) == (
        "applied", "新根", "火灵根", 384, 75
    )
    assert (replay.status, replay.root, replay.power, replay.stone) == (
        "duplicate", "新根", 384, 75
    )
    assert repository.get_result("reroll-1", "u").status == "duplicate"
    with DatabaseUnitOfWork(root_reroll_db, read_only=True) as uow:
        assert uow.query_one("SELECT COUNT(*) AS count FROM player_root_reroll_operations")["count"] == 1


def test_reroll_rejects_operation_reuse_and_stale_snapshot(root_reroll_db: Path) -> None:
    repository = BaseRootRerollSqlRepository(root_reroll_db)
    repository.reroll("reroll-1", "u", _snapshot(), "新根", "火灵根", 25, 2, 1)

    conflict = repository.reroll("reroll-1", "u", _snapshot(), "别根", "水灵根", 25, 2, 1)
    with DatabaseUnitOfWork(root_reroll_db, immediate=True) as uow:
        uow.execute("UPDATE user_xiuxian SET exp=124 WHERE user_id='u'")
    changed = repository.reroll(
        "reroll-2", "u", _snapshot(), "别根", "水灵根", 25, 2, 1
    )

    assert conflict.status == "operation_conflict"
    assert changed.status == "state_changed"
    with DatabaseUnitOfWork(root_reroll_db, read_only=True) as uow:
        row = uow.query_one("SELECT root,stone FROM user_xiuxian WHERE user_id='u'")
    assert (row["root"], row["stone"]) == ("新根", 75)


def test_reroll_rejects_insufficient_stone_and_non_finite_rates(root_reroll_db: Path) -> None:
    with DatabaseUnitOfWork(root_reroll_db, immediate=True) as uow:
        uow.execute("UPDATE user_xiuxian SET stone=10 WHERE user_id='u'")
    repository = BaseRootRerollSqlRepository(root_reroll_db)

    insufficient = repository.reroll(
        "reroll-low", "u", _snapshot(stone=10), "新根", "火灵根", 25, 2, 1
    )
    invalid = repository.reroll(
        "reroll-nan", "u", _snapshot(stone=10), "新根", "火灵根", 1, float("nan"), 1
    )

    assert insufficient.status == "stone_insufficient"
    assert invalid.status == "invalid"
    with DatabaseUnitOfWork(root_reroll_db, read_only=True) as uow:
        assert uow.query_one("SELECT COUNT(*) AS count FROM player_root_reroll_operations")["count"] == 0


def test_missing_startup_schema_fails_closed_without_request_ddl(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian("
            "user_id TEXT,root TEXT,root_type TEXT,root_level INTEGER,level TEXT,"
            "exp INTEGER,power INTEGER,stone INTEGER)"
        )
        uow.execute(
            "INSERT INTO user_xiuxian VALUES('u','旧根','木灵根',0,'练气一层',123,100,100)"
        )

    result = BaseRootRerollSqlRepository(database).reroll(
        "reroll-1", "u", _snapshot(), "新根", "火灵根", 25, 2, 1
    )

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='player_root_reroll_operations'"
        ) is None
        assert uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"] == 100


def test_root_reroll_migration_is_game_only() -> None:
    migrations = build_migrations()
    game = {item.version for item in migrations_for_database(migrations, "game_db")}
    player = {item.version for item in migrations_for_database(migrations, "player_db")}
    assert "base.008" in game
    assert "base.008" not in player
