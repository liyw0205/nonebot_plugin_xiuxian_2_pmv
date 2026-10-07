from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pytest

from ....core.result import OperationOutcome
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from .. import command_repository as command_repository_module
from ..command_repository import BegCommandRepository, BegCommandSchemaError
from ..repository import BegRepository


@pytest.fixture
def reader(tmp_path):
    database = tmp_path / "game.db"
    with DatabaseUnitOfWork(database) as uow:
        OperationLedger().ensure_schema(uow)
        BegRepository.ensure_schema(uow)
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,create_time TEXT,stone INTEGER,sect_id INTEGER,root_type TEXT,level TEXT,is_beg INTEGER,is_novice INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u','2026-10-07 10:00:00',100,NULL,'root','level',NULL,0)")
    return BegCommandRepository(database)


def store(reader, *, action="daily_settle", user="u", status="applied", data=None):
    ledger = OperationLedger()
    with DatabaseUnitOfWork(reader.database) as uow:
        ledger.begin(uow, "event", f"beg.{action}", {"user_id": user})
        if status == "started":
            return
        if data is None:
            data = {"status": status, "stone_reward": 10, "stone": 110}
        outcome = OperationOutcome(
            status=status, operation_id="event", action=f"beg.{action}", data=data,
            code=data.get("status") if status != "applied" else None,
        )
        ledger.finish(uow, outcome)


@pytest.mark.parametrize("action", ["daily_settle", "novice_claim"])
def test_successful_ledger_receipt_replays_original_result_before_legacy_tables(reader, action):
    store(reader, action=action)
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("DROP TABLE beg_daily_reward_operations")
        uow.execute("DROP TABLE novice_gift_claim_operations")

    result = reader.receipt(operation_id="event", user_id="u", action=action)

    assert result["status"] == "duplicate"
    assert result["action"] == action
    assert result["replayed"] is True
    assert result["stone"] == 110


def test_durable_rejection_is_not_transformed_into_success(reader):
    store(reader, status="rejected", data={"status": "already_claimed", "stone_reward": 0, "stone": 0})
    result = reader.receipt(operation_id="event", user_id="u", action="daily_settle")
    assert result["status"] == "already_claimed"
    assert result["replayed"] is True


@pytest.mark.parametrize("user,action", [("other", "daily_settle"), ("u", "novice_claim")])
def test_ledger_rejects_different_user_or_action(reader, user, action):
    store(reader)
    assert reader.receipt(operation_id="event", user_id=user, action=action)["status"] == "operation_conflict"


@pytest.mark.parametrize("status,expected", [("started", "operation_pending"), ("needs_reconcile", "operation_failed")])
def test_incomplete_ledger_does_not_fall_through_to_a_legacy_success(reader, status, expected):
    store(reader, status=status)
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("INSERT INTO beg_daily_reward_operations(operation_id,payload,stone_reward,stone) VALUES('event','[\"u\"]',10,110)")
    assert reader.receipt(operation_id="event", user_id="u", action="daily_settle")["status"] == expected


def test_rolled_back_failure_can_retry_with_original_identity_without_mutating_the_failure_receipt(reader):
    ledger = OperationLedger()
    ledger.record_failure(reader.database, "retry", "beg.daily_settle", {"user_id": "u"}, "injected failure")
    with DatabaseUnitOfWork(reader.database, read_only=True) as uow:
        before = uow.query_all("SELECT * FROM operation_ledger")
        audit_before = uow.query_all("SELECT * FROM operation_audit")

    assert reader.receipt(operation_id="retry", user_id="u", action="daily_settle") is None
    assert reader.receipt(operation_id="retry", user_id="other", action="daily_settle")["status"] == "operation_conflict"
    with DatabaseUnitOfWork(reader.database, read_only=True) as uow:
        assert uow.query_all("SELECT * FROM operation_ledger") == before
        assert uow.query_all("SELECT * FROM operation_audit") == audit_before
    with DatabaseUnitOfWork(reader.database) as uow:
        assert ledger.begin(uow, "retry", "beg.daily_settle", {"user_id": "u"}) is None
        ledger.finish(uow, OperationOutcome.applied(
            "retry", "beg.daily_settle", data={"status": "applied", "stone_reward": 10, "stone": 110},
        ))
    assert reader.receipt(operation_id="retry", user_id="u", action="daily_settle")["status"] == "duplicate"


def test_retryable_failure_still_rejects_conflicting_old_receipt(reader):
    OperationLedger().record_failure(reader.database, "retry", "beg.daily_settle", {"user_id": "u"}, "failed")
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("INSERT INTO beg_daily_reward_operations(operation_id,payload,stone_reward,stone) VALUES('retry','[\"other\"]',10,110)")
    assert reader.receipt(operation_id="retry", user_id="u", action="daily_settle")["status"] == "operation_conflict"


def test_unrecognized_failure_result_is_not_allowed_to_retry(reader):
    store(reader, status="failed")
    assert reader.receipt(operation_id="event", user_id="u", action="daily_settle")["status"] == "receipt_invalid"


@pytest.mark.parametrize("with_failed_receipt", [False, True])
def test_new_or_retryable_write_requires_complete_audit_schema_without_repair(reader, with_failed_receipt):
    if with_failed_receipt:
        OperationLedger().record_failure(reader.database, "retry", "beg.daily_settle", {"user_id": "u"}, "failed")
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("DROP TABLE operation_audit")
        uow.execute("CREATE TABLE operation_audit(operation_id TEXT)")
    with pytest.raises(BegCommandSchemaError, match="audit schema"):
        reader.receipt(operation_id="retry", user_id="u", action="daily_settle")
    with DatabaseUnitOfWork(reader.database, read_only=True) as uow:
        assert [row["name"] for row in uow.query_all("PRAGMA table_info(operation_audit)")] == ["operation_id"]
        assert uow.query_all("SELECT * FROM operation_audit") == []


def test_explicit_successful_receipt_does_not_require_audit_schema(reader):
    store(reader)
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("DROP TABLE operation_audit")
    assert reader.receipt(operation_id="event", user_id="u", action="daily_settle")["status"] == "duplicate"


@pytest.mark.parametrize("corruption", ["bad_json", "wrong_action", "missing_status", "contradictory_status"])
def test_malformed_or_inconsistent_ledger_result_is_rejected(reader, corruption):
    store(reader)
    with DatabaseUnitOfWork(reader.database) as uow:
        row = uow.query_one("SELECT result_json FROM operation_ledger WHERE operation_id='event'")
        result = json.loads(row["result_json"])
        if corruption == "wrong_action":
            result["action"] = "beg.novice_claim"
        elif corruption == "missing_status":
            result["data"].pop("status")
        elif corruption == "contradictory_status":
            result["status"] = "rejected"
        encoded = "broken" if corruption == "bad_json" else json.dumps(result)
        uow.execute("UPDATE operation_ledger SET result_json=? WHERE operation_id='event'", (encoded,))
    assert reader.receipt(operation_id="event", user_id="u", action="daily_settle")["status"] == "receipt_invalid"


@pytest.mark.parametrize("action,table,fields,values", [
    ("daily_settle", "beg_daily_reward_operations", "stone_reward,stone", "10,110"),
    ("novice_claim", "novice_gift_claim_operations", "stone", "1000"),
])
def test_legacy_success_survives_without_ledger_but_original_payload_identity_is_required(reader, action, table, fields, values):
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("DROP TABLE operation_ledger")
        uow.execute(f"INSERT INTO {table}(operation_id,payload,{fields}) VALUES('legacy','[\"u\"]',{values})")
    assert reader.receipt(operation_id="legacy", user_id="u", action=action)["status"] == "duplicate"
    assert reader.receipt(operation_id="legacy", user_id="other", action=action)["status"] == "operation_conflict"
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute(f"UPDATE {table} SET payload='{{}}' WHERE operation_id='legacy'")
    assert reader.receipt(operation_id="legacy", user_id="u", action=action)["status"] == "receipt_invalid"


def test_profile_and_absent_receipt_use_read_only_connections_and_preserve_storage(reader):
    original = command_repository_module.DatabaseUnitOfWork
    before = reader.database.read_bytes()
    with patch.object(command_repository_module, "DatabaseUnitOfWork", wraps=original) as opened:
        assert reader.receipt(operation_id="new", user_id="u", action="daily_settle") is None
        profile = reader.profile("u")
        assert reader.profile("missing") is None
    assert profile == {
        "user_id": "u", "create_time": "2026-10-07 10:00:00", "stone": 100,
        "sect_id": None, "root_type": "root", "level": "level", "is_beg": 0, "is_novice": 0,
    }
    assert all(call.kwargs.get("read_only") is True for call in opened.call_args_list)
    assert reader.database.read_bytes() == before


def test_missing_database_and_schema_are_never_created(tmp_path):
    database = tmp_path / "missing.db"
    reader = BegCommandRepository(database)
    with pytest.raises(BegCommandSchemaError):
        reader.receipt(operation_id="event", user_id="u", action="daily_settle")
    with pytest.raises(BegCommandSchemaError):
        reader.profile("u")
    assert not database.exists()
    with sqlite3.connect(database):
        pass
    with pytest.raises(BegCommandSchemaError):
        reader.receipt(operation_id="event", user_id="u", action="daily_settle")
    with pytest.raises(BegCommandSchemaError):
        reader.profile("u")
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []


def test_partial_ledger_schema_fails_closed_without_repair(reader):
    with DatabaseUnitOfWork(reader.database) as uow:
        uow.execute("DROP TABLE operation_ledger")
        uow.execute("CREATE TABLE operation_ledger(operation_id TEXT)")
    with pytest.raises(BegCommandSchemaError, match="ledger schema"):
        reader.receipt(operation_id="event", user_id="u", action="daily_settle")
    with DatabaseUnitOfWork(reader.database, read_only=True) as uow:
        assert [row["name"] for row in uow.query_all("PRAGMA table_info(operation_ledger)")] == ["operation_id"]
