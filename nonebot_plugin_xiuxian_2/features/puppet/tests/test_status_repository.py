from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..migrations import apply_puppet_status
from ..status_repository import PuppetStatusSqlRepository


@pytest.fixture
def puppet_status_db(tmp_path: Path) -> Path:
    database = tmp_path / "game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,puppet_status INTEGER)")
        uow.executemany(
            "INSERT INTO user_xiuxian VALUES(?,?)",
            (("duplicate", 0), ("duplicate", 1), ("first", 1), ("off", 0)),
        )
        OperationLedger().ensure_schema(uow)
    return database


def test_puppet_status_migration_is_idempotent_and_preserves_existing_values(tmp_path: Path) -> None:
    database = tmp_path / "game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,stone INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('player',5)")
        apply_puppet_status(uow)
        apply_puppet_status(uow)

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        columns = {row["name"] for row in uow.query_all('PRAGMA main.table_info("user_xiuxian")')}
        assert "puppet_status" in columns
        row = uow.query_one("SELECT user_id,stone,puppet_status FROM user_xiuxian")
        assert (row["user_id"], row["stone"], row["puppet_status"]) == ("player", 5, 0)


def test_puppet_status_migration_skips_database_before_player_table_exists(tmp_path: Path) -> None:
    database = tmp_path / "new-game.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        apply_puppet_status(uow)
        assert uow.query_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='user_xiuxian'"
        ) is None


def test_status_toggle_updates_duplicate_rows_replays_and_reads_first_row(puppet_status_db: Path) -> None:
    repository = PuppetStatusSqlRepository(puppet_status_db)
    assert repository.get_status("duplicate") == 0

    first = repository.set_enabled("puppet-status-1", "duplicate", True)
    replay = repository.set_enabled("puppet-status-1", "duplicate", True)

    assert (first.status, first.enabled, first.affected_rows) == ("applied", 1, 1)
    assert (replay.status, replay.enabled, replay.affected_rows) == ("duplicate", 1, 1)
    assert repository.get_status("duplicate") == 1
    with DatabaseUnitOfWork(puppet_status_db, read_only=True) as uow:
        assert [row["puppet_status"] for row in uow.query_all(
            "SELECT puppet_status FROM user_xiuxian WHERE user_id='duplicate' ORDER BY rowid"
        )] == [1, 1]
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_audit")["count"] == 1


def test_status_toggle_rejects_operation_conflict(puppet_status_db: Path) -> None:
    repository = PuppetStatusSqlRepository(puppet_status_db)
    repository.set_enabled("puppet-status-1", "first", True)

    conflict = repository.set_enabled("puppet-status-1", "first", False)

    assert conflict.status == "operation_conflict"
    assert repository.get_status("first") == 1


def test_enabled_users_are_read_in_bounded_rowid_pages(puppet_status_db: Path) -> None:
    repository = PuppetStatusSqlRepository(puppet_status_db)
    high_watermark = repository.enabled_user_high_watermark()
    with DatabaseUnitOfWork(puppet_status_db, immediate=True) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES('late',1)")

    first_page = repository.list_enabled_users(0, high_watermark, limit=1)
    second_page = repository.list_enabled_users(first_page[-1].row_id, high_watermark, limit=1)

    assert [(row.row_id, row.user_id) for row in first_page] == [(2, "duplicate")]
    assert [(row.row_id, row.user_id) for row in second_page] == [(3, "first")]
    assert repository.list_enabled_users(second_page[-1].row_id, high_watermark, limit=1) == []


def test_status_requests_fail_closed_without_migrated_column(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.sqlite3"
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
        uow.execute("INSERT INTO user_xiuxian VALUES('player')")
        OperationLedger().ensure_schema(uow)

    result = PuppetStatusSqlRepository(database).set_enabled("puppet-status-1", "player", True)

    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        columns = {row["name"] for row in uow.query_all('PRAGMA main.table_info("user_xiuxian")')}
        assert "puppet_status" not in columns
        assert uow.query_one("SELECT user_id FROM user_xiuxian")["user_id"] == "player"
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_ledger")["count"] == 0


def test_status_toggle_rolls_back_when_audit_write_fails(puppet_status_db: Path) -> None:
    with DatabaseUnitOfWork(puppet_status_db, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER fail_puppet_status_audit "
            "BEFORE INSERT ON operation_audit "
            "BEGIN SELECT RAISE(ABORT,'audit failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="audit failed"):
        PuppetStatusSqlRepository(puppet_status_db).set_enabled(
            "puppet-status-1", "duplicate", True
        )

    with DatabaseUnitOfWork(puppet_status_db, read_only=True) as uow:
        assert [row["puppet_status"] for row in uow.query_all(
            "SELECT puppet_status FROM user_xiuxian WHERE user_id='duplicate' ORDER BY rowid"
        )] == [0, 1]
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_ledger")["count"] == 0
