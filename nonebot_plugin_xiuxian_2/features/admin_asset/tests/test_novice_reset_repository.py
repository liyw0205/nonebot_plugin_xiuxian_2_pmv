from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..novice_reset_repository import AdminNoviceResetSqlRepository


@pytest.fixture
def novice_reset_db(tmp_path: Path) -> Path:
    database = tmp_path / "game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,is_novice INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('claimed',1)")
        uow.execute("INSERT INTO user_xiuxian VALUES('fresh',0)")
        uow.execute("INSERT INTO user_xiuxian VALUES('claimed',1)")
        OperationLedger().ensure_schema(uow)
    return database


def test_reset_is_atomic_and_replayable(novice_reset_db: Path) -> None:
    repository = AdminNoviceResetSqlRepository(novice_reset_db)

    first = repository.reset("novice-reset-1", "admin")
    replay = repository.reset("novice-reset-1", "admin")

    assert (first.status, first.reset_count) == ("applied", 2)
    assert (replay.status, replay.reset_count) == ("duplicate", 2)
    with DatabaseUnitOfWork(novice_reset_db, read_only=True) as uow:
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM user_xiuxian WHERE is_novice <> 0"
        )["count"] == 0
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM operation_audit WHERE operation_id=?",
            ("novice-reset-1",),
        )["count"] == 1


def test_reset_rejects_operation_conflict(novice_reset_db: Path) -> None:
    repository = AdminNoviceResetSqlRepository(novice_reset_db)
    repository.reset("novice-reset-1", "admin")

    conflict = repository.reset("novice-reset-1", "other-admin")

    assert conflict.status == "operation_conflict"


def test_reset_fails_closed_without_platform_schema(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,is_novice INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('claimed',1)")

    result = AdminNoviceResetSqlRepository(database).reset("novice-reset-1", "admin")

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT is_novice FROM user_xiuxian")["is_novice"] == 1
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='operation_ledger'"
        ) is None


def test_reset_fails_closed_for_incomplete_platform_schema(tmp_path: Path) -> None:
    database = tmp_path / "partial-ledger.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,is_novice INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('claimed',1)")
        uow.execute("CREATE TABLE operation_ledger(operation_id TEXT,action TEXT)")

    result = AdminNoviceResetSqlRepository(database).reset("novice-reset-1", "admin")

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT is_novice FROM user_xiuxian")["is_novice"] == 1


def test_reset_rolls_back_when_audit_write_fails(novice_reset_db: Path) -> None:
    with DatabaseUnitOfWork(novice_reset_db, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER fail_novice_reset_audit "
            "BEFORE INSERT ON operation_audit "
            "BEGIN SELECT RAISE(ABORT,'audit failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="audit failed"):
        AdminNoviceResetSqlRepository(novice_reset_db).reset("novice-reset-1", "admin")

    with DatabaseUnitOfWork(novice_reset_db, read_only=True) as uow:
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM user_xiuxian WHERE is_novice <> 0"
        )["count"] == 2
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM operation_ledger"
        )["count"] == 0
