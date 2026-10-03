from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import AdminApplication
from ..migrations import apply_admin_player_status_batch_reset
from ..player_status_reset_repository import AdminPlayerStatusResetSqlRepository


def _database(path: Path) -> None:
    with DatabaseUnitOfWork(path) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,"
            "mp INTEGER,atk INTEGER,user_stamina INTEGER)"
        )
        uow.execute("INSERT INTO user_xiuxian VALUES('u',101,1,2,3,4)")
        apply_admin_player_status_batch_reset(uow)


def _state(path: Path) -> tuple[int, int, int, int, int]:
    with DatabaseUnitOfWork(path, read_only=True) as uow:
        row = uow.query_one(
            "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian WHERE user_id='u'"
        )
    return tuple(row.values())


def test_single_reset_is_atomic_idempotent_and_feature_owned(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _database(database)
    app = AdminApplication(database)

    assert app.player_status_snapshot("u") == (101, 1, 2, 3, 4)
    first = app.reset_player_status(
        "reset", "admin", "u", (101, 1, 2, 3, 4), 20, target_name="道友", force=True
    )
    duplicate = app.reset_player_status(
        "reset", "admin", "u", (101, 50, 101, 10, 20), 20, force=True
    )

    assert (first.status, duplicate.status) == ("reset", "duplicate")
    assert first.previous_state == (101, 1, 2, 3, 4)
    assert first.final_state == (101, 50, 101, 10, 20)
    assert _state(database) == (101, 50, 101, 10, 20)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        count = uow.query_one(
            "SELECT COUNT(*) AS count FROM admin_player_status_reset_operations"
        )
    assert count["count"] == 1


def test_single_reset_preserves_conflict_cas_and_missing_user_results(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _database(database)
    repository = AdminPlayerStatusResetSqlRepository(database)

    stale = repository.reset("stale", "admin", "u", (101, 0, 2, 3, 4), 20)
    reset = repository.reset("conflict", "admin", "u", None, 20)
    force_conflict = repository.reset(
        "conflict", "admin", "u", None, 20, force=True
    )
    conflict = repository.reset("conflict", "admin", "u", None, 30)
    missing = repository.reset("missing", "admin", "absent", None, 20)

    assert stale.status == "state_changed"
    assert reset.status == "reset"
    assert force_conflict.status == "operation_conflict"
    assert conflict.status == "operation_conflict"
    assert missing.status == "user_missing"
    assert repository.snapshot("absent") is None


def test_single_reset_fails_closed_without_request_schema_creation(tmp_path: Path) -> None:
    database = tmp_path / "unmigrated.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,hp INTEGER,"
            "mp INTEGER,atk INTEGER,user_stamina INTEGER)"
        )
        uow.execute("INSERT INTO user_xiuxian VALUES('u',101,1,2,3,4)")

    repository = AdminPlayerStatusResetSqlRepository(database)
    assert repository.reset("op", "admin", "u", None, 20).status == "schema_missing"
    with pytest.raises(RuntimeError, match="schema_missing"):
        repository.snapshot("u")
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE name='admin_player_status_reset_operations'"
        )
    assert table is None


def test_single_reset_late_receipt_failure_rolls_back_player_update(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _database(database)
    repository = AdminPlayerStatusResetSqlRepository(database)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TRIGGER reject_status_reset_receipt BEFORE INSERT ON "
            "admin_player_status_reset_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="failed"):
        repository.reset("failed", "admin", "u", (101, 1, 2, 3, 4), 20, force=True)

    assert _state(database) == (101, 1, 2, 3, 4)


def test_single_reset_keeps_oversized_numeric_receipt_values(tmp_path: Path) -> None:
    database = tmp_path / "game.db"
    _database(database)
    exp = 2**80
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("UPDATE user_xiuxian SET exp=? WHERE user_id='u'", (str(exp),))
    repository = AdminPlayerStatusResetSqlRepository(database)
    expected = repository.snapshot("u")

    result = repository.reset("large", "admin", "u", expected, 20, force=True)

    assert result.status == "reset"
    assert result.final_state == (exp, exp // 2, exp, exp // 10, 20)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        receipt = uow.query_one(
            "SELECT previous_state,final_state FROM "
            "admin_player_status_reset_operations WHERE operation_id='large'"
        )
    assert json.loads(receipt["previous_state"])[0] == str(exp)
    assert json.loads(receipt["final_state"])[0] == str(exp)


def test_default_single_handler_routes_snapshot_and_reset_through_application() -> None:
    source_path = Path(__file__).parents[3] / "xiuxian" / "xiuxian_admin" / "__init__.py"
    source = source_path.read_text(encoding="utf-8")
    start = source.index("async def restate_")
    handler = source[start:source.index("@set_xiuxian.handle", start)]
    single = handler[handler.index("if give_qq:"):]
    assert "get_user_profile_by_name(nick_name)" in handler
    assert "admin_application.player_status_snapshot(" in single
    assert "admin_application.reset_player_status(" in single
    assert "_admin_player_status_reset_service().snapshot(" not in single
    assert "_sql_message().get_user_info_with_name(nick_name)" not in handler
    assert "schema_missing" in single
