from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..application import MixelixirApplication
from ..daily_reset_repository import MixelixirDailyCountResetSqlRepository


def _ready_database(path: Path) -> Path:
    with DatabaseUnitOfWork(path, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('active',3),('clear',0),('legacy-null',NULL)")
        OperationLedger().ensure_schema(uow)
    return path


def test_daily_reset_is_atomic_and_same_day_replay_does_not_clear_new_counts(tmp_path: Path) -> None:
    database = _ready_database(tmp_path / "game.db")
    application = MixelixirApplication(database, tmp_path / "player.db")

    first = application.reset_daily_count("2026-10-03")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("UPDATE user_xiuxian SET mixelixir_num=7 WHERE user_id='active'")
    replay = application.reset_daily_count("2026-10-03")
    next_day = application.reset_daily_count("2026-10-04")

    assert (first.status, first.reset_count) == ("applied", 2)
    assert (replay.status, replay.reset_count) == ("duplicate", 2)
    assert (next_day.status, next_day.reset_count) == ("applied", 1)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT mixelixir_num FROM user_xiuxian WHERE user_id='active'")["mixelixir_num"] == 0
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_audit")["count"] == 2


def test_daily_reset_conflict_and_missing_schema_fail_closed(tmp_path: Path) -> None:
    database = _ready_database(tmp_path / "game.db")
    repository = MixelixirDailyCountResetSqlRepository(database)
    repository.reset("daily-reset", "2026-10-03")

    conflict = repository.reset("daily-reset", "2026-10-04")
    assert conflict.status == "operation_conflict"

    missing = tmp_path / "missing-ledger.db"
    with DatabaseUnitOfWork(missing, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('active',3)")
    result = MixelixirDailyCountResetSqlRepository(missing).reset("daily-reset", "2026-10-03")
    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(missing, read_only=True) as uow:
        assert uow.query_one("SELECT mixelixir_num FROM user_xiuxian")["mixelixir_num"] == 3
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='operation_ledger'"
        ) is None


def test_daily_reset_rolls_back_counter_and_ledger_when_audit_write_fails(tmp_path: Path) -> None:
    database = _ready_database(tmp_path / "game.db")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER fail_mixelixir_reset_audit BEFORE INSERT ON operation_audit "
            "BEGIN SELECT RAISE(ABORT,'audit failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="audit failed"):
        MixelixirDailyCountResetSqlRepository(database).reset("daily-reset", "2026-10-03")

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT mixelixir_num FROM user_xiuxian WHERE user_id='active'")["mixelixir_num"] == 3
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_ledger")["count"] == 0
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_audit")["count"] == 0
