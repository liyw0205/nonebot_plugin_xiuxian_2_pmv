from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest

from nonebot_plugin_xiuxian_2.features.title.application import TitleApplication
from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


@pytest.fixture
def grant(tmp_path):
    database = tmp_path / "player.db"
    with DatabaseUnitOfWork(database) as uow:
        apply_title_schema(uow)
        OperationLedger().ensure_schema(uow)
        uow.execute("INSERT INTO title VALUES('other','[\"9\"]','9')")
        uow.execute("CREATE TABLE isolated_grant_effect(value INTEGER)")
    return SimpleNamespace(database=database, app=TitleApplication(database))


def sql(fixture, statement):
    with sqlite3.connect(fixture.database) as conn:
        conn.execute(statement)


def snapshot(fixture):
    with sqlite3.connect(fixture.database) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        return tuple(conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() for table in (
            "title", "title_transaction_operations", "operation_ledger", "operation_audit", "isolated_grant_effect"))


def execute(fixture, operation="grant"):
    return fixture.app.grant(operation_id=operation, user_id="u", expected_unlocked=[], title_id="30101")


WRITE_FAULTS = [
    ("BEFORE INSERT ON title", "SELECT RAISE(IGNORE)", "state_write_rejected"),
    ("AFTER INSERT ON title", "UPDATE title SET unlocked='[]' WHERE user_id=NEW.user_id", "state_projection_mismatch"),
    ("BEFORE INSERT ON title_transaction_operations", "SELECT RAISE(IGNORE)", "receipt_write_rejected"),
    ("AFTER INSERT ON title_transaction_operations", "UPDATE title_transaction_operations SET payload='[]' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
    ("AFTER INSERT ON title_transaction_operations", "UPDATE title SET equipped='wrong' WHERE user_id='u'", "state_projection_mismatch"),
    ("AFTER INSERT ON operation_audit", "UPDATE title SET unlocked='[]' WHERE user_id='u'", "state_projection_mismatch"),
    ("AFTER INSERT ON operation_audit", "UPDATE title_transaction_operations SET title_id='wrong' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch"),
    ("AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET request_hash='wrong' WHERE operation_id=NEW.operation_id", "ledger_projection_mismatch"),
    ("AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET result_json='not-json' WHERE operation_id=NEW.operation_id", "ledger_projection_mismatch"),
]


@pytest.mark.parametrize("timing,body,suffix", WRITE_FAULTS)
def test_known_grant_write_fault_rolls_back_and_recovers_same_request(grant, timing, body, suffix):
    before = snapshot(grant)
    sql(grant, f"CREATE TRIGGER fault {timing} BEGIN INSERT INTO isolated_grant_effect VALUES(1); {body}; END")
    rejected = execute(grant)
    assert (rejected.status, rejected.code) == ("failed", f"title_grant_{suffix}")
    assert rejected.data["reason"] == rejected.code and rejected.data["subject"] == "30101"
    assert snapshot(grant) == before
    sql(grant, "DROP TRIGGER fault")
    recovered = execute(grant)
    assert recovered.ok, recovered
    state = grant.app.get_state("u")
    assert json.loads(state["unlocked"]) == ["30101"] and state["equipped"] == ""
    committed = snapshot(grant)
    assert execute(grant).replayed
    assert snapshot(grant) == committed


@pytest.mark.parametrize("timing,condition", [
    ("BEFORE INSERT ON title", "NEW.user_id='u'"),
    ("BEFORE INSERT ON title_transaction_operations", "NEW.operation_id='grant'"),
    ("BEFORE INSERT ON operation_audit", "NEW.status='applied'"),
])
def test_grant_abort_keeps_original_error_and_retry_hash(grant, timing, condition):
    before = snapshot(grant)
    sql(grant, f"CREATE TRIGGER fault {timing} WHEN {condition} BEGIN SELECT RAISE(ABORT,'isolated grant abort'); END")
    with pytest.raises(sqlite3.IntegrityError, match="isolated grant abort"):
        execute(grant)
    failed = snapshot(grant)
    assert (failed[0], failed[1], failed[4]) == (before[0], before[1], before[4])
    assert len(failed[2]) == len(failed[3]) == 1 and failed[2][0][3] == "failed"
    sql(grant, "DROP TRIGGER fault")
    assert execute(grant).ok
    committed = snapshot(grant)
    assert execute(grant).replayed and snapshot(grant) == committed
