from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..admin_refresh_reset_repository import WorkAdminRefreshResetSqlRepository


@pytest.fixture
def work_reset_db(tmp_path: Path) -> Path:
    database = tmp_path / "game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,work_num INTEGER)")
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES(?,?)",
            (("duplicate", 1), ("duplicate", 0), ("same", 5), ("null", None)),
        )
        OperationLedger().ensure_schema(uow)
    return database


def test_reset_updates_all_historical_rows_atomically_and_replays(work_reset_db: Path) -> None:
    repository = WorkAdminRefreshResetSqlRepository(work_reset_db)

    first = repository.reset_all("work-reset-1", "admin", 5)
    replay = repository.reset_all("work-reset-1", "admin", 5)

    assert (first.status, first.affected_rows) == ("applied", 3)
    assert (replay.status, replay.affected_rows) == ("duplicate", 3)
    with DatabaseUnitOfWork(work_reset_db, read_only=True) as uow:
        assert [row["work_num"] for row in uow.query_all("SELECT work_num FROM user_xiuxian ORDER BY rowid")] == [5, 5, 5, 5]
        assert uow.query_one(
            "SELECT COUNT(*) AS count FROM operation_audit WHERE operation_id=?",
            ("work-reset-1",),
        )["count"] == 1


def test_reset_rejects_operation_payload_conflict(work_reset_db: Path) -> None:
    repository = WorkAdminRefreshResetSqlRepository(work_reset_db)
    repository.reset_all("work-reset-1", "admin", 5)

    conflict = repository.reset_all("work-reset-1", "other-admin", 6)

    assert conflict.status == "operation_conflict"


def test_reset_fails_closed_without_platform_schema(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,work_num INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('player',1)")

    result = WorkAdminRefreshResetSqlRepository(database).reset_all("work-reset-1", "admin", 5)

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT work_num FROM user_xiuxian")["work_num"] == 1
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='operation_ledger'"
        ) is None


def test_reset_fails_closed_for_incomplete_platform_schema(tmp_path: Path) -> None:
    database = tmp_path / "partial-ledger.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,work_num INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('player',1)")
        uow.execute("CREATE TABLE operation_ledger(operation_id TEXT,action TEXT)")

    result = WorkAdminRefreshResetSqlRepository(database).reset_all("work-reset-1", "admin", 5)

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT work_num FROM user_xiuxian")["work_num"] == 1


def test_reset_rolls_back_when_audit_write_fails(work_reset_db: Path) -> None:
    with DatabaseUnitOfWork(work_reset_db, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER fail_work_reset_audit "
            "BEFORE INSERT ON operation_audit "
            "BEGIN SELECT RAISE(ABORT,'audit failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="audit failed"):
        WorkAdminRefreshResetSqlRepository(work_reset_db).reset_all("work-reset-1", "admin", 5)

    with DatabaseUnitOfWork(work_reset_db, read_only=True) as uow:
        assert [row["work_num"] for row in uow.query_all("SELECT work_num FROM user_xiuxian ORDER BY rowid")] == [1, 0, 5, None]
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_ledger")["count"] == 0
