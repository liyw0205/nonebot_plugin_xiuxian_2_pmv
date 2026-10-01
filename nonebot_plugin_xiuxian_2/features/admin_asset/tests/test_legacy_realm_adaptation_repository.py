from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..legacy_realm_adaptation_repository import AdminLegacyRealmAdaptationSqlRepository
from ..migrations import apply_admin_realm_changes


@pytest.fixture
def realm_adaptation_db(tmp_path: Path) -> Path:
    database = tmp_path / "game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,level TEXT)")
        uow.execute("INSERT INTO user_xiuxian VALUES('legacy','搬血境初期')")
        uow.execute("INSERT INTO user_xiuxian VALUES('current','感气境')")
        uow.execute("INSERT INTO user_xiuxian VALUES('duplicate','洞天境中期')")
        uow.execute("INSERT INTO user_xiuxian VALUES('duplicate','other')")
        apply_admin_realm_changes(uow)
    return database


def _mapping() -> dict[str, str]:
    return {"搬血境": "感气境", "洞天境": "练气境"}


def test_adaptation_preserves_stages_and_first_duplicate_user_semantics(
    realm_adaptation_db: Path,
) -> None:
    repository = AdminLegacyRealmAdaptationSqlRepository(realm_adaptation_db)

    result = repository.adapt("adapt-1", "admin", _mapping())

    assert (result.status, result.adapted_count, result.failed_count, result.success_count) == (
        "applied", 2, 0, 2
    )
    with DatabaseUnitOfWork(realm_adaptation_db, read_only=True) as uow:
        rows = uow.query_all("SELECT user_id,level FROM user_xiuxian ORDER BY rowid")
    assert [(row["user_id"], row["level"]) for row in rows] == [
        ("legacy", "感气境初期"),
        ("current", "感气境"),
        ("duplicate", "练气境中期"),
        ("duplicate", "练气境中期"),
    ]


def test_adaptation_replay_returns_first_result_and_rejects_operation_conflict(
    realm_adaptation_db: Path,
) -> None:
    repository = AdminLegacyRealmAdaptationSqlRepository(realm_adaptation_db)
    first = repository.adapt("adapt-1", "admin", _mapping())
    replay = repository.adapt("adapt-1", "admin", _mapping())
    conflict = repository.adapt("adapt-1", "admin", {"搬血境": "新境界"})

    assert replay.status == "duplicate"
    assert (replay.adapted_count, replay.success_count) == (
        first.adapted_count,
        first.success_count,
    )
    assert conflict.status == "operation_conflict"
    with DatabaseUnitOfWork(realm_adaptation_db, read_only=True) as uow:
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM admin_level_change_operations"
        )["count"] == 1


def test_adaptation_fails_closed_without_existing_operation_schema(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,level TEXT)")
        uow.execute("INSERT INTO user_xiuxian VALUES('legacy','搬血境')")

    result = AdminLegacyRealmAdaptationSqlRepository(database).adapt(
        "adapt-1", "admin", _mapping()
    )

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT level FROM user_xiuxian WHERE user_id='legacy'")["level"] == "搬血境"
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='admin_level_change_operations'"
        ) is None


def test_receipt_failure_rolls_back_all_level_updates(realm_adaptation_db: Path) -> None:
    with DatabaseUnitOfWork(realm_adaptation_db, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER fail_realm_adaptation_receipt "
            "BEFORE INSERT ON admin_level_change_operations "
            "BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="receipt failed"):
        AdminLegacyRealmAdaptationSqlRepository(realm_adaptation_db).adapt(
            "adapt-1", "admin", _mapping()
        )

    with DatabaseUnitOfWork(realm_adaptation_db, read_only=True) as uow:
        assert uow.query_one("SELECT level FROM user_xiuxian WHERE user_id='legacy'")["level"] == "搬血境初期"
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM admin_level_change_operations"
        )["count"] == 0
