from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest

from nonebot_plugin_xiuxian_2.features.title.application import TitleApplication
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


@pytest.fixture
def unlock(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_title_schema(uow)
        OperationLedger().ensure_schema(uow)
        uow.execute("INSERT INTO title VALUES('u','[\"1\"]','1')")
        uow.execute("INSERT INTO title VALUES('other','[\"9\"]','9')")
        uow.execute("CREATE TABLE isolated_unlock_effect(value INTEGER)")
    return SimpleNamespace(database=database, app=TitleApplication(database))


def sql(fixture, statement):
    with sqlite3.connect(fixture.database) as conn:
        conn.execute(statement)


def snapshot(fixture):
    with sqlite3.connect(fixture.database) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        return tuple(conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() for table in (
            "title", "title_transaction_operations", "operation_ledger", "operation_audit", "isolated_unlock_effect"))


def execute(fixture, operation="unlock"):
    return fixture.app.unlock_batch(operation_id=operation, user_id="u", expected_unlocked=["1"], title_ids=["2", "3"])


WRITE_FAULTS = [
    (False, "BEFORE INSERT ON title", "SELECT RAISE(IGNORE)", "state_write_rejected"),
    (True, "BEFORE UPDATE ON title", "SELECT RAISE(IGNORE)", "state_write_rejected"),
    (False, "AFTER INSERT ON title", "UPDATE title SET unlocked='[]' WHERE user_id=NEW.user_id", "state_projection_mismatch"),
    (True, "AFTER UPDATE ON title", "UPDATE title SET equipped='changed' WHERE user_id=NEW.user_id", "state_projection_mismatch"),
    (True, "BEFORE INSERT ON title_transaction_operations", "SELECT RAISE(IGNORE)", "receipt_write_rejected"),
    (True, "AFTER INSERT ON title_transaction_operations", "UPDATE title_transaction_operations SET payload='[]' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
    (True, "AFTER INSERT ON title_transaction_operations", "UPDATE title SET unlocked='[]' WHERE user_id='u'", "state_projection_mismatch"),
    (True, "AFTER INSERT ON operation_audit", "UPDATE title SET equipped='changed' WHERE user_id='u'", "state_projection_mismatch"),
    (True, "AFTER INSERT ON operation_audit", "UPDATE title_transaction_operations SET title_id='wrong' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
    (True, "AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET result_json='{}' WHERE operation_id=NEW.operation_id", "ledger_projection_mismatch"),
    (True, "AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET request_hash='wrong' WHERE operation_id=NEW.operation_id", "ledger_projection_mismatch"),
    (True, "AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET result_json='not-json' WHERE operation_id=NEW.operation_id", "ledger_projection_mismatch"),
]


@pytest.mark.parametrize("existing,timing,body,suffix", WRITE_FAULTS)
def test_unlock_rejection_rolls_back_and_recovers_same_request(unlock, existing, timing, body, suffix):
    if not existing:
        sql(unlock, "DELETE FROM title WHERE user_id='u'")
    expected = ["1"] if existing else []
    before = snapshot(unlock)
    sql(unlock, f"CREATE TRIGGER fault {timing} BEGIN INSERT INTO isolated_unlock_effect VALUES(1); {body}; END")

    def action():
        return unlock.app.unlock_batch(operation_id="unlock", user_id="u", expected_unlocked=expected, title_ids=["2", "3"])

    try:
        rejected = action()
        assert (rejected.status, rejected.code) == ("failed", f"title_batch_unlock_{suffix}")
        assert rejected.data["subject"] == "2,3"
        assert rejected.data["reason"] == rejected.code
        assert snapshot(unlock) == before
    finally:
        sql(unlock, "DROP TRIGGER fault")
    assert action().ok
    state = unlock.app.get_state("u")
    assert json.loads(state["unlocked"]) == [*expected, "2", "3"]
    assert state["equipped"] == ("1" if existing else "")
    committed = snapshot(unlock)
    assert action().replayed
    assert snapshot(unlock) == committed


@pytest.mark.parametrize("timing,condition,exception,phrase", [
    ("BEFORE UPDATE ON title", "1", sqlite3.IntegrityError, "isolated title unlock abort"),
    ("BEFORE INSERT ON title_transaction_operations", "1", sqlite3.IntegrityError, "isolated title unlock abort"),
    ("BEFORE INSERT ON operation_audit", "NEW.status='applied'", sqlite3.IntegrityError, "isolated title unlock abort"),
    ("BEFORE INSERT ON operation_ledger", "NEW.status='started'", RuntimeError, "operation ledger finish write rejected"),
    ("BEFORE UPDATE ON operation_ledger", "NEW.status='applied'", RuntimeError, "operation ledger finish write rejected"),
    ("BEFORE INSERT ON operation_audit", "NEW.status='applied'", RuntimeError, "operation audit finish write rejected"),
])
def test_unexpected_database_error_keeps_original_failure_and_retry_hash(unlock, timing, condition, exception, phrase):
    before = snapshot(unlock)
    body = "SELECT RAISE(ABORT,'isolated title unlock abort')" if exception is sqlite3.IntegrityError else "SELECT RAISE(IGNORE)"
    sql(unlock, f"CREATE TRIGGER fault {timing} WHEN {condition} BEGIN INSERT INTO isolated_unlock_effect VALUES(1); {body}; END")
    with pytest.raises(exception, match=phrase):
        execute(unlock)
    failed = snapshot(unlock)
    assert (failed[0], failed[1], failed[4]) == (before[0], before[1], before[4])
    assert len(failed[2]) == len(failed[3]) == 1
    failure_audit = failed[3][0]
    assert failed[2][0][3] == "failed"
    sql(unlock, "DROP TRIGGER fault")
    recovered = execute(unlock)
    assert recovered.ok, recovered
    assert failure_audit in snapshot(unlock)[3]
    committed = snapshot(unlock)
    assert execute(unlock).replayed
    assert snapshot(unlock) == committed
