from __future__ import annotations

import __future__
import ast
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.core.errors import ConflictError
from nonebot_plugin_xiuxian_2.features.beg.application import BegApplication
from nonebot_plugin_xiuxian_2.features.beg.command_application import BegCommandApplication
from nonebot_plugin_xiuxian_2.features.beg.command_replies import render_beg_reply
from nonebot_plugin_xiuxian_2.features.beg.command_repository import BegCommandRepository
from nonebot_plugin_xiuxian_2.features.beg.repository import BegRepository
from nonebot_plugin_xiuxian_2.features.beg.tests import test_beg_application as beg_fixtures
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


FACADE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_beg/__init__.py"
CREATED = datetime(2026, 9, 12, 8, 30)
REPLAY = "\u5df2\u7ecf\u5904\u7406"
CONFLICT = "\u51b2\u7a81"
DAILY_SUCCESS = "\u4f60\u83b7\u5f97\u4e86"
GIFT_SUCCESS = "\u9886\u53d6\u6210\u529f"
LEVELS = ("\u7ec3\u6c14\u5883\u521d\u671f", "\u7ec3\u6c14\u5883\u4e2d\u671f", "\u7b51\u57fa\u5883\u521d\u671f")
ACTIONS = {
    "daily_settle": ("beg_stone_", "beg-daily", "beg_daily_reward_operations", DAILY_SUCCESS),
    "novice_claim": ("novice_", "novice-gift", "novice_gift_claim_operations", GIFT_SUCCESS),
}


def _query(database, sql, parameters=()):
    with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
        return connection.execute(sql, parameters).fetchall()


def _context(tmp_path, *, now=None, failure_hook=None, initialize=True):
    database = beg_fixtures.BegApplicationTest._database(str(tmp_path)) if initialize else tmp_path / "missing.db"
    config = SimpleNamespace(beg_max_days=7, beg_max_level=LEVELS[-1],
                             beg_lingshi_lower_limit=20, beg_lingshi_upper_limit=30, max_goods_num=1000)
    config_provider = Mock(return_value=config)
    levels_provider = Mock(return_value=dict.fromkeys(LEVELS, {}))
    gift_provider = Mock(return_value={
        "name_1": "\u7075\u77f3", "amount_1": 500,
        "name_2": "Sword", "amount_2": 1, "type_2": "\u6cd5\u5668", "buff_2": 101,
        "name_3": "Manual", "amount_3": 2, "type_3": "\u529f\u6cd5", "buff_3": 102,
    })
    clock = SimpleNamespace(now=Mock(return_value=now or (CREATED + timedelta(days=1)).astimezone(timezone.utc)))
    rng = SimpleNamespace(randint=Mock(return_value=25))
    repository = BegCommandRepository(database)
    writer = BegApplication(database, repository=BegRepository(failure_hook=failure_hook))
    application = BegCommandApplication(database, config_provider, levels_provider, gift_provider,
                                        clock=clock, rng=rng, repository=repository, application=writer)
    return SimpleNamespace(database=database, config=config, config_provider=config_provider,
                           levels_provider=levels_provider, gift_provider=gift_provider,
                           clock=clock, rng=rng, repository=repository, writer=writer, application=application)


def _assets(context):
    return (
        _query(context.database, "SELECT * FROM user_xiuxian ORDER BY user_id"),
        _query(context.database, "SELECT * FROM back ORDER BY user_id,goods_id"),
        _query(context.database, "SELECT * FROM beg_daily_reward_operations ORDER BY operation_id"),
        _query(context.database, "SELECT * FROM novice_gift_claim_operations ORDER BY operation_id"),
    )


def _state(context):
    return _assets(context), _query(context.database, "SELECT * FROM operation_ledger ORDER BY operation_id,action")


def _block_current_inputs(context):
    context.repository.profile = Mock()
    context.writer.execute = Mock()
    readers = (context.repository.profile, context.writer.execute, context.config_provider,
               context.levels_provider, context.gift_provider, context.clock.now, context.rng.randint)
    for reader in readers:
        reader.reset_mock()
        reader.side_effect = AssertionError("receipt must precede current inputs and writes")
    return readers


class _Finished(Exception):
    pass


def _handler(context):
    messages, help_messages = [], []
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
        "assign_bot": assign_bot, "check_user": Mock(return_value=(True, {"user_id": "u"}, "")),
        "handle_send": send, "send_help_message": send_help,
        "beg_command_application": context.application, "render_beg_reply": render_beg_reply,
        "runtime_ids": ids, "ConflictError": ConflictError, "logger": Mock(),
        **{name: SimpleNamespace(finish=finish) for name in ("beg_stone", "novice", "beg_help")},
    }
    tree = ast.parse(FACADE.read_text(encoding="utf-8"))
    handlers = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                and node.name in {"beg_stone_", "novice_", "beg_help_"}]
    assert len(handlers) == 3
    for handler in handlers:
        handler.decorator_list = []
    exec(compile(ast.Module(body=handlers, type_ignores=[]), str(FACADE), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)

    def run(action, *, event_id="event", fallback_id=""):
        messages.clear()
        help_messages.clear()
        event = SimpleNamespace(message_id=event_id, id=fallback_id, get_user_id=lambda: "u")
        name = "beg_help_" if action == "help" else ACTIONS[action][0]
        try:
            asyncio.run(namespace[name](SimpleNamespace(self_id="bot"), event))
        except _Finished:
            pass
        assert len(messages) + len(help_messages) == 1
        return (messages or help_messages)[0][0]

    return SimpleNamespace(run=run, namespace=namespace, messages=messages, help_messages=help_messages, ids=ids)


@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("event_id,fallback_id,identity", [("event", "", "event"), (None, "fallback", "fallback"), (None, "", "fresh-id")])
def test_real_handler_settles_assets_once_and_replays_before_all_current_inputs(tmp_path, action, event_id, fallback_id, identity):
    context = _context(tmp_path)
    handler = _handler(context)
    _, prefix, table, success = ACTIONS[action]
    message = handler.run(action, event_id=event_id, fallback_id=fallback_id)
    assert success in message
    expected_user = (125, 1, 0) if action == "daily_settle" else (600, 0, 1)
    assert _query(context.database, "SELECT stone,is_beg,is_novice FROM user_xiuxian") == [expected_user]
    expected_items = [] if action == "daily_settle" else [
        (101, "\u88c5\u5907", 1, 1), (102, "\u6280\u80fd", 2, 2),
    ]
    assert _query(context.database, "SELECT goods_id,goods_type,goods_num,bind_num FROM back ORDER BY goods_id") == expected_items
    operation_id = f"{prefix}:{identity}:u"
    assert _query(context.database, f"SELECT operation_id FROM {table}") == [(operation_id,)]
    assert _query(context.database, "SELECT operation_id,action,status FROM operation_ledger") == [(operation_id, f"beg.{action}", "applied")]
    if action == "novice_claim":
        assert "Sword x1" in message and "Manual x2" in message
        assert handler.messages[0][1]["v4"] == "\u4fee\u4ed9\u5e2e\u52a9"
    else:
        context.rng.randint.assert_called_once_with(20, 30)
    before = _state(context)
    readers = _block_current_inputs(context)
    assert REPLAY in handler.run(action, event_id=event_id, fallback_id=fallback_id)
    for reader in readers:
        reader.assert_not_called()
    assert _state(context) == before
    assert handler.ids.new_id.call_count == (2 if identity == "fresh-id" else 0)


@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("changed", ["user", "action"])
def test_receipt_identity_conflict_is_not_reported_as_success(tmp_path, action, changed):
    context = _context(tmp_path)
    assert ACTIONS[action][3] in _handler(context).run(action)
    before = _state(context)
    readers = _block_current_inputs(context)
    result = context.application.execute(
        operation_id=f"{ACTIONS[action][1]}:event:u", user_id="other" if changed == "user" else "u",
        action=next(kind for kind in ACTIONS if kind != action) if changed == "action" else action,
    )
    assert result["status"] == "operation_conflict"
    message = render_beg_reply(result)
    assert CONFLICT in message and DAILY_SUCCESS not in message and GIFT_SUCCESS not in message
    for reader in readers:
        reader.assert_not_called()
    assert _state(context) == before


@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("reader", ["receipt", "profile"])
def test_read_failure_never_reaches_transactional_writer(tmp_path, action, reader):
    context = _context(tmp_path)
    before = _state(context)
    with patch.object(context.repository, reader, side_effect=sqlite3.OperationalError("unavailable")), \
            patch.object(context.writer, "execute", side_effect=AssertionError("write after read failure")) as write:
        message = _handler(context).run(action)
        write.assert_not_called()
    assert DAILY_SUCCESS not in message and GIFT_SUCCESS not in message
    assert _state(context) == before


@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("payload,amount,valid", [
    ('["u"]', 500, True), ('["other"]', 500, False), ("not-json", 500, False),
    ("[]", 500, False), ('["u"]', -1, False),
])
def test_legacy_receipts_are_replayed_or_rejected_without_profile_or_writer(tmp_path, action, payload, amount, valid):
    context = _context(tmp_path)
    _, prefix, table, _ = ACTIONS[action]
    with DatabaseUnitOfWork(context.database) as uow:
        if action == "daily_settle":
            uow.execute(f"INSERT INTO {table}(operation_id,payload,stone_reward,stone) VALUES(?,?,25,?)",
                        (f"{prefix}:legacy:u", payload, amount))
        else:
            uow.execute(f"INSERT INTO {table}(operation_id,payload,stone) VALUES(?,?,?)",
                        (f"{prefix}:legacy:u", payload, amount))
        uow.execute("DROP TABLE operation_ledger")
    before = _assets(context)
    readers = _block_current_inputs(context)
    message = _handler(context).run(action, event_id="legacy")
    if valid:
        assert REPLAY in message
    else:
        assert DAILY_SUCCESS not in message and GIFT_SUCCESS not in message
        assert CONFLICT in message or "\u56de\u6267\u5f02\u5e38" in message
    for reader in readers:
        reader.assert_not_called()
    assert _assets(context) == before
    assert _query(context.database, "SELECT name FROM sqlite_master WHERE name='operation_ledger'") == []


@pytest.mark.parametrize("action,age,allowed", [
    ("daily_settle", timedelta(days=7, seconds=1), True),
    ("daily_settle", timedelta(days=8), False),
    ("novice_claim", timedelta(days=2), True),
    ("novice_claim", timedelta(days=7), True),
    ("novice_claim", timedelta(days=7, seconds=1), False),
])
def test_aware_clock_and_legacy_naive_creation_preserve_each_age_boundary(tmp_path, action, age, allowed):
    context = _context(tmp_path, now=(CREATED + age).astimezone(timezone.utc))
    message = _handler(context).run(action)
    assert (ACTIONS[action][3] in message) is allowed
    expected = (125, 1, 0) if allowed and action == "daily_settle" else (600, 0, 1) if allowed else (100, 0, 0)
    assert _query(context.database, "SELECT stone,is_beg,is_novice FROM user_xiuxian") == [expected]
    if not allowed:
        assert _query(context.database, "SELECT * FROM back") == []


def test_daily_reset_flag_does_not_introduce_a_24_hour_cooldown(tmp_path):
    context = _context(tmp_path)
    handler = _handler(context)
    assert DAILY_SUCCESS in handler.run("daily_settle")
    with DatabaseUnitOfWork(context.database) as uow:
        uow.execute("UPDATE user_xiuxian SET is_beg=0 WHERE user_id='u'")
    assert DAILY_SUCCESS in handler.run("daily_settle", event_id="after-reset")
    assert _query(context.database, "SELECT stone,is_beg FROM user_xiuxian") == [(150, 1)]


def test_gift_failure_after_real_inventory_write_rolls_back_and_allows_same_event_retry(tmp_path):
    checkpoints = []

    def fail(checkpoint):
        checkpoints.append(checkpoint)
        if checkpoint == "after_rewards" and checkpoints.count(checkpoint) == 1:
            raise RuntimeError("injected gift failure")

    context = _context(tmp_path, failure_hook=fail)
    before = _assets(context)
    handler = _handler(context)
    message = handler.run("novice_claim")
    assert "after_rewards" in checkpoints
    assert GIFT_SUCCESS not in message
    assert _assets(context) == before
    assert _query(context.database, "SELECT operation_id,action,status FROM operation_ledger") == [
        ("novice-gift:event:u", "beg.novice_claim", "failed"),
    ]
    assert GIFT_SUCCESS in handler.run("novice_claim")
    assert _query(context.database, "SELECT stone,is_novice FROM user_xiuxian") == [(600, 1)]
    assert _query(context.database, "SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id") == [(101, 1, 1), (102, 2, 2)]
    assert _query(context.database, "SELECT status FROM operation_ledger") == [("applied",)]


@pytest.mark.parametrize("action", ACTIONS)
def test_unregistered_user_stops_before_command_application(tmp_path, action):
    context = _context(tmp_path, initialize=False)
    handler = _handler(context)
    handler.namespace["check_user"].return_value = (False, None, "register-first")
    with patch.object(context.application, "execute", side_effect=AssertionError("unregistered command")) as execute:
        assert handler.run(action) == "register-first"
        execute.assert_not_called()
    assert handler.messages[0][1]["md_type"] == "\u6211\u8981\u4fee\u4ed9"
    assert not context.database.exists()


def test_help_reads_live_configuration_and_clock_without_database_or_user_lookup(tmp_path):
    context = _context(tmp_path, initialize=False)
    handler = _handler(context)
    handler.namespace["check_user"].side_effect = AssertionError("help must not require a player")
    with patch.object(context.repository, "receipt", side_effect=AssertionError("help receipt read")) as receipt, \
            patch.object(context.repository, "profile", side_effect=AssertionError("help profile read")) as profile, \
            patch.object(context.writer, "execute", side_effect=AssertionError("help write")) as write:
        first = handler.run("help")
        assert "7\u5929" in first and LEVELS[-1] in first
        context.config.beg_max_days = 9
        context.config.beg_max_level = "NewMaxLevel"
        context.clock.now.return_value += timedelta(hours=1)
        second = handler.run("help")
        assert "9\u5929" in second and "NewMaxLevel" in second and second != first
        receipt.assert_not_called()
        profile.assert_not_called()
        write.assert_not_called()
    assert handler.help_messages[0][1]["v2"] == "\u65b0\u624b\u793c\u5305"
    context.gift_provider.assert_not_called()
    context.rng.randint.assert_not_called()
    assert not context.database.exists()
