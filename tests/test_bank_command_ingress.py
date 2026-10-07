from __future__ import annotations

import __future__
import ast
import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.core.errors import ConflictError
from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.account_info_application import BankAccountInfoApplication
from nonebot_plugin_xiuxian_2.features.bank.account_interest_application import BankInterestApplication
from nonebot_plugin_xiuxian_2.features.bank.account_upgrade_application import BankUpgradeApplication
from nonebot_plugin_xiuxian_2.features.bank.account_withdrawal_application import BankWithdrawalApplication
from nonebot_plugin_xiuxian_2.features.bank.command_application import BankCommandApplication
from nonebot_plugin_xiuxian_2.features.bank.command_replies import render_bank_reply
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


FACADE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_bank/__init__.py"
NOW = datetime(2026, 10, 7, 12)
DEPOSIT = "\u5b58\u7075\u77f3"
WITHDRAW = "\u53d6\u7075\u77f3"
UPGRADE = "\u5347\u7ea7\u4f1a\u5458"
INTEREST = "\u7ed3\u7b97"
INFO = "\u4fe1\u606f"
SUCCESS = "\u6210\u529f"
REPLAY = "\u5df2\u7ecf\u5904\u7406"


def _query(database, sql, parameters=()):
    with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
        return connection.execute(sql, parameters).fetchall()


def _context(tmp_path, *, account=True):
    database = tmp_path / "game.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',1000)")
        apply_bank_accounts(uow)
        if account:
            uow.execute("INSERT INTO bank_accounts VALUES('u',100,'1','2026-10-07 10:00:00')")
    levels = {
        "1": {"level": "L1", "savemax": 10000, "levelup": 50, "interest": 0.01},
        "2": {"level": "L2", "savemax": 20000, "levelup": 100, "interest": 0.02},
    }
    clock = SimpleNamespace(now=lambda: NOW)
    return SimpleNamespace(database=database, levels=levels, clock=clock,
                           application=BankCommandApplication(database, levels, clock=clock))


def _state(context):
    return (
        _query(context.database, "SELECT user_id,stone FROM user_xiuxian ORDER BY user_id"),
        _query(context.database, "SELECT user_id,saved_stone,bank_level,updated_at FROM bank_accounts ORDER BY user_id"),
        _query(context.database, "SELECT * FROM bank_account_operations ORDER BY operation_id"),
    )


class _Finished(Exception):
    pass


def _handler(context):
    messages = []
    help_messages = []
    ids = SimpleNamespace(new_id=Mock(return_value="fresh-id"))

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def send(bot, event, message, **kwargs):
        messages.append((message, kwargs))

    async def send_help(bot, event, message, **kwargs):
        help_messages.append((message, kwargs))

    async def finish():
        raise _Finished

    namespace = {
        "assign_bot": assign_bot, "check_user": lambda event: (True, {"user_id": "u"}, ""),
        "handle_send": send, "send_help_message": send_help,
        "bank": SimpleNamespace(finish=finish), "bank_command_application": context.application,
        "render_bank_reply": render_bank_reply, "BANKLEVEL": context.levels,
        "runtime_ids": ids, "logger": Mock(), "ConflictError": ConflictError,
        "RegexGroup": lambda: None, "__bank_help__": "bank help",
    }
    tree = ast.parse(FACADE.read_text(encoding="utf-8"))
    handler = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "bank_")
    handler.decorator_list = []
    exec(compile(ast.Module(body=[handler], type_ignores=[]), str(FACADE), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)

    def run(mode, argument="", *, event_id="event", fallback_id=""):
        messages.clear()
        help_messages.clear()
        event = SimpleNamespace(message_id=event_id, id=fallback_id, get_user_id=lambda: "u")
        try:
            asyncio.run(namespace["bank_"](SimpleNamespace(self_id="bot"), event, (mode, argument)))
        except _Finished:
            pass
        assert len(messages) + len(help_messages) == 1
        return (messages or help_messages)[0][0]

    return SimpleNamespace(run=run, namespace=namespace, messages=messages,
                           help_messages=help_messages, ids=ids)


@pytest.mark.parametrize("mode,action,wallet,saved", [
    (DEPOSIT, "deposit", 992, 110), (WITHDRAW, "withdrawal", 1012, 90),
])
def test_real_handler_automatically_settles_interest_and_replays_without_writes(tmp_path, mode, action, wallet, saved):
    context = _context(tmp_path)
    handler = _handler(context)
    message = handler.run(mode, "10")
    assert SUCCESS in message
    assert _query(context.database, "SELECT stone FROM user_xiuxian") == [(wallet,)]
    account = _query(context.database, "SELECT saved_stone,bank_level,updated_at FROM bank_accounts")[0]
    assert account[:2] == (saved, "1")
    assert datetime.fromisoformat(account[2]) == NOW
    assert _query(context.database, "SELECT operation_id,interest FROM bank_account_operations") == [(f"bank-{action}:event:u", 2)]
    before = _state(context)
    with patch.object(BankAccountInfoApplication, "get_info", side_effect=AssertionError("replay before current account")):
        assert REPLAY in handler.run(mode, "10")
    assert _state(context) == before
    handler.ids.new_id.assert_not_called()


def test_upgrade_receipt_precedes_current_level_limits_and_configuration(tmp_path):
    context = _context(tmp_path)
    handler = _handler(context)
    assert SUCCESS in handler.run(UPGRADE)
    assert _query(context.database, "SELECT stone FROM user_xiuxian") == [(950,)]
    assert _query(context.database, "SELECT bank_level,updated_at FROM bank_accounts") == [("2", "2026-10-07 10:00:00")]
    before = _state(context)
    context.levels.clear()
    with patch.object(BankAccountInfoApplication, "get_info", side_effect=AssertionError("upgrade replay before info")), \
            patch.object(BankUpgradeApplication, "upgrade", side_effect=AssertionError("upgrade replay must not write")):
        message = handler.run(UPGRADE)
    assert SUCCESS in message and REPLAY in message
    assert _state(context) == before


@pytest.mark.parametrize("mode,argument", [(DEPOSIT, "2000"), (WITHDRAW, "200")])
def test_rejected_writer_result_without_success_fields_is_reported_without_keyerror(tmp_path, mode, argument):
    context = _context(tmp_path)
    handler = _handler(context)
    before = _state(context)
    message = handler.run(mode, argument)
    assert SUCCESS not in message
    assert "\u4e0d\u8db3" in message
    assert _state(context) == before


def test_new_account_info_shows_wallet_and_defaults_without_creating_account(tmp_path):
    context = _context(tmp_path, account=False)
    handler = _handler(context)
    before = _state(context)
    message = handler.run(INFO)
    assert "1000" in message and "L1" in message
    assert "\u5c1a\u672a\u5b58\u5165" in message
    assert _state(context) == before
    assert handler.messages[0][1]["v3"] == "\u7075\u5e84\u7ed3\u7b97"


def test_explicit_interest_settles_once_and_replays_before_account_read(tmp_path):
    context = _context(tmp_path)
    handler = _handler(context)
    assert SUCCESS in handler.run(INTEREST)
    assert _query(context.database, "SELECT stone FROM user_xiuxian") == [(1002,)]
    before = _state(context)
    with patch.object(BankAccountInfoApplication, "get_info", side_effect=AssertionError("interest replay before info")):
        assert REPLAY in handler.run(INTEREST)
    assert _state(context) == before


@pytest.mark.parametrize("mode,writer,method", [
    (DEPOSIT, BankDepositApplication, "deposit"),
    (WITHDRAW, BankWithdrawalApplication, "withdraw"),
    (UPGRADE, BankUpgradeApplication, "upgrade"),
    (INTEREST, BankInterestApplication, "settle_interest"),
])
def test_account_read_failure_stops_before_any_writer(tmp_path, mode, writer, method):
    context = _context(tmp_path)
    handler = _handler(context)
    before = _state(context)
    with patch.object(BankAccountInfoApplication, "get_info", side_effect=sqlite3.OperationalError("unavailable")), \
            patch.object(writer, method, side_effect=AssertionError("write after read failure")) as write:
        message = handler.run(mode, "10" if mode in {DEPOSIT, WITHDRAW} else "")
        write.assert_not_called()
    assert SUCCESS not in message and "\u5f02\u5e38" in message
    assert _state(context) == before


@pytest.mark.parametrize("raw", ["", "0", "-1", "+1", "1.5", "1 2", "nan", "9" * 5000],
                         ids=["empty", "zero", "negative", "signed", "fraction", "multiple", "nan", "overlong"])
@pytest.mark.parametrize("mode", [DEPOSIT, WITHDRAW])
def test_invalid_amount_never_reaches_account_read_or_mutation(tmp_path, mode, raw):
    context = _context(tmp_path)
    handler = _handler(context)
    before = _state(context)
    with patch.object(BankAccountInfoApplication, "get_info", side_effect=AssertionError("invalid account read")) as read:
        message = handler.run(mode, raw)
        read.assert_not_called()
    assert SUCCESS not in message and "\u6b63\u786e" in message
    assert _state(context) == before


def test_reused_event_with_changed_amount_reports_conflict_without_writes(tmp_path):
    context = _context(tmp_path)
    handler = _handler(context)
    assert SUCCESS in handler.run(DEPOSIT, "10")
    before = _state(context)
    message = handler.run(DEPOSIT, "11")
    assert "\u51b2\u7a81" in message and SUCCESS not in message
    assert _state(context) == before


@pytest.mark.parametrize("payload", ["[]", '["other-user",10]'], ids=["malformed", "wrong-user"])
def test_legacy_receipt_rejection_does_not_require_success_fields_or_reach_writer(tmp_path, payload):
    context = _context(tmp_path)
    with DatabaseUnitOfWork(context.database) as uow:
        uow.execute(
            "CREATE TABLE bank_deposit_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
            "deposited INTEGER,interest INTEGER,wallet_stone INTEGER,saved_stone INTEGER,saved_at TEXT)"
        )
        uow.execute("INSERT INTO bank_deposit_operations VALUES(?,?,10,2,992,110,'then')",
                    ("bank-deposit:legacy:u", payload))
    handler = _handler(context)
    before = _state(context)
    with patch.object(BankAccountInfoApplication, "get_info", side_effect=AssertionError("invalid receipt before info")), \
            patch.object(BankDepositApplication, "deposit", side_effect=AssertionError("invalid receipt must not write")) as write:
        message = handler.run(DEPOSIT, "10", event_id="legacy")
        write.assert_not_called()
    assert SUCCESS not in message
    assert "\u51b2\u7a81" in message or "\u56de\u6267\u5f02\u5e38" in message
    assert _state(context) == before


def test_help_and_identity_fallback_preserve_existing_adapter_contract(tmp_path):
    context = _context(tmp_path)
    handler = _handler(context)
    before = _state(context)
    assert handler.run(None) == "bank help"
    assert handler.help_messages[0][1]["v1"] == "\u7075\u5e84\u5b58\u7075\u77f3"
    assert handler.run(None, "\u5e2e\u52a9") == "bank help"
    assert _state(context) == before
    assert SUCCESS in handler.run(DEPOSIT, "10", event_id=None, fallback_id="fallback")
    assert SUCCESS in handler.run(DEPOSIT, "10", event_id=None)
    assert _query(context.database, "SELECT operation_id FROM bank_account_operations ORDER BY operation_id") == [
        ("bank-deposit:fallback:u",), ("bank-deposit:u:fresh-id",),
    ]
    handler.ids.new_id.assert_called_once_with()


@pytest.mark.parametrize("action", ["deposit", "withdrawal", "upgrade", "interest"])
@pytest.mark.parametrize("status", ["stone_insufficient", "operation_conflict", "state_changed", "needs_reconcile", "receipt_invalid", "schema_missing"])
def test_presenter_rejects_without_reading_success_only_fields(action, status):
    message = render_bank_reply({"action": action, "status": status}, {})
    assert SUCCESS not in message
