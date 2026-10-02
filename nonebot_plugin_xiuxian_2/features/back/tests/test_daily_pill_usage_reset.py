from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..daily_pill_usage_reset_application import DailyPillUsageResetApplication
from ..daily_pill_usage_reset_repository import DailyPillUsageResetSqlRepository


def _ready_database(path: Path) -> Path:
    with DatabaseUnitOfWork(path, immediate=True) as uow:
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_type TEXT,day_num INTEGER)"
        )
        uow.execute(
            "INSERT INTO back VALUES"
            "('u1',1,'丹药',3),('u2',2,'丹药',0),('u3',3,'丹药',NULL),('u4',4,'药材',8)"
        )
        OperationLedger().ensure_schema(uow)
    return path


def test_daily_pill_usage_reset_is_replayable_without_clearing_new_usage(tmp_path: Path) -> None:
    database = _ready_database(tmp_path / "game.db")
    application = DailyPillUsageResetApplication(database)

    first = application.reset("2026-10-03")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute("UPDATE back SET day_num=5 WHERE user_id='u1'")
    replay = application.reset("2026-10-03")
    next_day = application.reset("2026-10-04")

    assert (first.status, first.affected_rows) == ("applied", 2)
    assert (replay.status, replay.affected_rows) == ("duplicate", 2)
    assert (next_day.status, next_day.affected_rows) == ("applied", 1)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT day_num FROM back WHERE user_id='u1'")["day_num"] == 0
        assert uow.query_one("SELECT day_num FROM back WHERE user_id='u4'")["day_num"] == 8
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_audit")["count"] == 2


def test_daily_pill_usage_reset_conflict_and_missing_schema_fail_closed(tmp_path: Path) -> None:
    database = _ready_database(tmp_path / "game.db")
    repository = DailyPillUsageResetSqlRepository(database)
    repository.reset("pill-reset", "2026-10-03")

    conflict = repository.reset("pill-reset", "2026-10-04")
    assert conflict.status == "operation_conflict"

    missing = tmp_path / "missing-ledger.db"
    with DatabaseUnitOfWork(missing, immediate=True) as uow:
        uow.execute("CREATE TABLE back(goods_type TEXT,day_num INTEGER)")
        uow.execute("INSERT INTO back VALUES('丹药',3)")
    result = DailyPillUsageResetSqlRepository(missing).reset("pill-reset", "2026-10-03")
    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(missing, read_only=True) as uow:
        assert uow.query_one("SELECT day_num FROM back")["day_num"] == 3
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='operation_ledger'"
        ) is None

    missing_audit = tmp_path / "missing-audit.db"
    with DatabaseUnitOfWork(missing_audit, immediate=True) as uow:
        uow.execute("CREATE TABLE back(goods_type TEXT,day_num INTEGER)")
        uow.execute("INSERT INTO back VALUES('丹药',3)")
        OperationLedger().ensure_schema(uow)
        uow.execute("DROP TABLE operation_audit")
    result = DailyPillUsageResetSqlRepository(missing_audit).reset(
        "pill-reset", "2026-10-03"
    )
    assert result.status == "schema_missing"
    with DatabaseUnitOfWork(missing_audit, read_only=True) as uow:
        assert uow.query_one("SELECT day_num FROM back")["day_num"] == 3
        assert uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='operation_audit'"
        ) is None


def test_daily_pill_usage_reset_rolls_back_when_audit_write_fails(tmp_path: Path) -> None:
    database = _ready_database(tmp_path / "game.db")
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER fail_pill_reset_audit BEFORE INSERT ON operation_audit "
            "BEGIN SELECT RAISE(ABORT,'audit failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="audit failed"):
        DailyPillUsageResetSqlRepository(database).reset("pill-reset", "2026-10-03")

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("SELECT day_num FROM back WHERE user_id='u1'")["day_num"] == 3
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_ledger")["count"] == 0
        assert uow.query_one("SELECT COUNT(*) AS count FROM operation_audit")["count"] == 0
