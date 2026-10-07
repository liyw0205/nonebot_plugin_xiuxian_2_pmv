from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..clock import reset_bank_clock, set_bank_clock
from ..command_application import BankCommandApplication
from ..migrations import apply_bank_accounts


NOW = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
SAVED_AT = datetime(2026, 10, 7, 10, tzinfo=timezone.utc).isoformat()


@pytest.fixture
def command(tmp_path):
    database = tmp_path / "game.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',1000)")
        apply_bank_accounts(uow)
        uow.execute("INSERT INTO bank_accounts VALUES('u',100,'1',?)", (SAVED_AT,))
    levels = {
        "1": {"savemax": 10000, "interest": 0.1, "levelup": 100},
        "2": {"savemax": 20000, "interest": 0.2, "levelup": 0},
    }
    return BankCommandApplication(database, levels, clock=SimpleNamespace(now=lambda: NOW))


def rows(command, sql, values=()):
    with DatabaseUnitOfWork(command.database, read_only=True) as uow:
        return uow.query_all(sql, values)


@pytest.mark.parametrize("mode,argument,action,wallet,saved", [
    ("存灵石", "10", "deposit", 1010, 110),
    ("取灵石", "10", "withdrawal", 1030, 90),
    ("结算", None, "interest", 1020, 100),
])
def test_money_commands_settle_elapsed_interest_and_replay_before_current_configuration(command, mode, argument, action, wallet, saved):
    result = command.execute(operation_id="once", user_id="u", mode=mode, argument=argument)
    assert result["status"] == "applied"
    assert result["action"] == action
    assert result["interest"] == 20
    assert result["wallet_stone"] == wallet
    assert result["saved_stone"] == saved
    command.bank_levels.clear()
    with patch.object(command.info, "get_info", side_effect=AssertionError("replay read current account")):
        replay = command.execute(operation_id="once", user_id="u", mode=mode, argument=argument)
    assert replay["status"] == "duplicate"
    assert replay["interest"] == 20
    assert rows(command, "SELECT stone FROM user_xiuxian") == [{"stone": wallet}]
    assert len(rows(command, "SELECT * FROM bank_account_operations")) == 1


def test_upgrade_replay_does_not_upgrade_again_or_require_current_level_configuration(command):
    first = command.execute(operation_id="upgrade", user_id="u", mode="升级会员", argument=None)
    assert (first["status"], first["bank_level"], first["cost"]) == ("applied", "2", 100)
    command.bank_levels.clear()
    with patch.object(command.info, "get_info", side_effect=AssertionError("upgrade replay read account")):
        replay = command.execute(operation_id="upgrade", user_id="u", mode="升级会员", argument="")
    assert (replay["status"], replay["bank_level"], replay["cost"]) == ("duplicate", "2", 100)
    assert rows(command, "SELECT stone FROM user_xiuxian") == [{"stone": 900}]


@pytest.mark.parametrize("user,mode,argument", [("other", "存灵石", "10"), ("u", "取灵石", "10"), ("u", "存灵石", "11")])
def test_same_operation_rejects_changed_user_action_or_original_amount(command, user, mode, argument):
    command.execute(operation_id="once", user_id="u", mode="存灵石", argument="10")
    with patch.object(command.info, "get_info", side_effect=AssertionError("conflict read account")):
        result = command.execute(operation_id="once", user_id=user, mode=mode, argument=argument)
    assert result["status"] == "operation_conflict"


@pytest.mark.parametrize("mode,argument,field,value", [
    ("存灵石", "10", "saved_stone", 200),
    ("取灵石", "10", "updated_at", NOW.isoformat()),
    ("结算", None, "bank_level", "2"),
])
def test_stale_interest_snapshot_cannot_be_paid_after_concurrent_account_change(command, mode, argument, field, value):
    original = command.info.get_info

    def change_after_read(**kwargs):
        snapshot = original(**kwargs)
        with DatabaseUnitOfWork(command.database) as uow:
            uow.execute(f"UPDATE bank_accounts SET {field}=? WHERE user_id='u'", (value,))
        return snapshot

    with patch.object(command.info, "get_info", side_effect=change_after_read):
        result = command.execute(operation_id="stale", user_id="u", mode=mode, argument=argument)
    assert result["status"] == "state_changed"
    assert rows(command, "SELECT stone FROM user_xiuxian") == [{"stone": 1000}]
    assert rows(command, "SELECT * FROM bank_account_operations") == []


def test_two_commands_sharing_an_interest_snapshot_only_pay_once(command):
    competing = BankCommandApplication(command.database, command.bank_levels, clock=command.clock)
    original = command.info.get_info

    def settle_after_read(**kwargs):
        snapshot = original(**kwargs)
        result = competing.execute(operation_id="winner", user_id="u", mode="结算", argument=None)
        assert result["status"] == "applied"
        return snapshot

    with patch.object(command.info, "get_info", side_effect=settle_after_read):
        result = command.execute(operation_id="stale", user_id="u", mode="结算", argument=None)
    assert result["status"] == "state_changed"
    assert rows(command, "SELECT stone FROM user_xiuxian") == [{"stone": 1020}]
    assert rows(command, "SELECT operation_id FROM bank_account_operations") == [{"operation_id": "winner"}]


def test_default_command_clock_uses_context_at_execution_time(command):
    application = BankCommandApplication(command.database, command.bank_levels)
    token = set_bank_clock(command.clock)
    try:
        result = application.execute(operation_id="context", user_id="u", mode="结算", argument=None)
    finally:
        reset_bank_clock(token)
    assert result["status"] == "applied"
    assert result["interest"] == 20
    assert rows(command, "SELECT updated_at FROM bank_accounts") == [{"updated_at": NOW.isoformat()}]


@pytest.mark.parametrize("mode,argument,table,payload,columns,values,field,expected", [
    ("存灵石", "10", "bank_deposit_operations", ["u", 10], "deposited,interest,wallet_stone,saved_stone,saved_at", (10, 2, 992, 110, SAVED_AT), "deposited", 10),
    ("取灵石", "10", "bank_withdrawal_operations", ["u", 10], "withdrawn,interest,wallet_stone,saved_stone,saved_at", (10, 2, 1012, 90, SAVED_AT), "withdrawn", 10),
    ("升级会员", None, "bank_upgrade_operations", ["u", "2", 100], "cost,wallet_stone,bank_level", (100, 900, "2"), "bank_level", "2"),
    ("结算", None, "bank_interest_operations", ["u"], "interest,wallet_stone,saved_at", (2, 1002, SAVED_AT), "interest", 2),
])
def test_legacy_receipts_validate_payload_and_replay_without_account_or_configuration(command, mode, argument, table, payload, columns, values, field, expected):
    with DatabaseUnitOfWork(command.database) as uow:
        uow.execute(f"CREATE TABLE {table}(operation_id TEXT PRIMARY KEY,payload TEXT,{columns})")
        uow.execute(f"INSERT INTO {table} VALUES({','.join('?' for _ in range(len(values) + 2))})", ("legacy", json.dumps(payload), *values))
    command.bank_levels.clear()
    with patch.object(command.info, "get_info", side_effect=AssertionError("legacy replay read account")):
        result = command.execute(operation_id="legacy", user_id="u", mode=mode, argument=argument)
        wrong_user = command.execute(operation_id="legacy", user_id="other", mode=mode, argument=argument)
    assert result["status"] == "duplicate"
    assert result[field] == expected
    assert wrong_user["status"] == "operation_conflict"


@pytest.mark.parametrize("payload,deposited", [("broken", 10), ('["u",10]', 11)])
def test_invalid_legacy_receipt_is_not_guessed_success_or_reexecuted(command, payload, deposited):
    with DatabaseUnitOfWork(command.database) as uow:
        uow.execute("CREATE TABLE bank_deposit_operations(operation_id TEXT PRIMARY KEY,payload TEXT,deposited INTEGER,interest INTEGER,wallet_stone INTEGER,saved_stone INTEGER,saved_at TEXT)")
        uow.execute("INSERT INTO bank_deposit_operations VALUES('bad',?,?,0,990,110,?)", (payload, deposited, SAVED_AT))
    with patch.object(command.info, "get_info", side_effect=AssertionError("bad receipt fell through")):
        result = command.execute(operation_id="bad", user_id="u", mode="存灵石", argument="10")
    assert result["status"] == "receipt_invalid"
    assert rows(command, "SELECT * FROM bank_account_operations") == []


def test_missing_accounts_are_read_only_defaults_then_first_deposit_uses_full_iso_time(command):
    with DatabaseUnitOfWork(command.database) as uow:
        uow.execute("DELETE FROM bank_accounts")
    info = command.execute(operation_id="info", user_id="u", mode="信息", argument=None)
    assert (info["status"], info["wallet_stone"], info["saved_stone"], info["bank_level"]) == ("account_missing", 1000, 0, "1")
    assert rows(command, "SELECT * FROM bank_accounts") == []
    result = command.execute(operation_id="first", user_id="u", mode="存灵石", argument="10")
    assert result["status"] == "applied"
    assert result["interest"] == 0
    assert rows(command, "SELECT updated_at FROM bank_accounts") == [{"updated_at": NOW.isoformat()}]


def test_information_does_not_read_unrelated_operation_receipts(command):
    with patch.object(command.receipts, "find", side_effect=AssertionError("information read operation receipts")):
        result = command.execute(operation_id="info", user_id="u", mode="信息", argument=None)
    assert result["status"] == "ok"
    assert result["wallet_stone"] == 1000
    assert result["saved_stone"] == 100
    assert rows(command, "SELECT * FROM bank_account_operations") == []


@pytest.mark.parametrize("mode,argument", [("存灵石", "9" * 5000), ("取灵石", "-1"), ("存灵石", "0"), ("信息", "extra"), (None, "garbage")])
def test_invalid_arguments_are_bounded_and_do_not_open_a_database(tmp_path, mode, argument):
    database = tmp_path / "absent.db"
    result = BankCommandApplication(database, {}).execute(operation_id="bad", user_id="u", mode=mode, argument=argument)
    assert result["status"] == "invalid_argument"
    assert not database.exists()


def test_help_missing_database_and_missing_schema_do_not_create_tables(tmp_path):
    database = tmp_path / "absent.db"
    application = BankCommandApplication(database, {})
    assert application.execute(operation_id="help", user_id="u", mode=None, argument="帮助")["status"] == "help"
    assert application.execute(operation_id="missing", user_id="u", mode="信息", argument=None)["status"] == "schema_missing"
    assert not database.exists()
    with sqlite3.connect(database):
        pass
    assert application.execute(operation_id="empty", user_id="u", mode="存灵石", argument="10")["status"] == "schema_missing"
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
