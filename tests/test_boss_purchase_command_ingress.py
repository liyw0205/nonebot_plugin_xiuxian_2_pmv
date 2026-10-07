from __future__ import annotations

import __future__
import ast
import asyncio
import json
import sqlite3
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import tests  # Establish isolated data paths before plugin imports.

from nonebot_plugin_xiuxian_2.core.errors import ConflictError
from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
from nonebot_plugin_xiuxian_2.features.boss.purchase_command_application import BossPurchaseCommandApplication
from nonebot_plugin_xiuxian_2.features.boss.purchase_command_replies import render_boss_purchase_reply
from nonebot_plugin_xiuxian_2.features.boss.purchase_command_repository import BossPurchaseCommandRepository
from nonebot_plugin_xiuxian_2.features.boss.repository import BossPurchaseSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


FACADE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
TODAY = date(2026, 10, 7)
ITEM_ID = 1
ITEM_NAME = "灵草"
ITEM_TYPE = "药材"
SUCCESS = "成功兑换获得"
REPLAY = "已经处理"
CONFLICT = "冲突"


def _query(database, sql, parameters=()):
    with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection:
        return connection.execute(sql, parameters).fetchall()


def _context(tmp_path, *, weekly=8, integral=100):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',0)")
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
            "UNIQUE(user_id,goods_id))"
        )
        uow.execute(
            "CREATE TABLE boss_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "quantity INTEGER NOT NULL,cost INTEGER NOT NULL,integral INTEGER NOT NULL,"
            "purchased INTEGER NOT NULL,inventory INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'applied')"
        )
        OperationLedger().ensure_schema(uow)
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral INTEGER)")
        uow.execute("INSERT INTO boss_limit VALUES('u',?)", (integral,))
        uow.execute("CREATE TABLE boss(user_id TEXT PRIMARY KEY,weekly_purchases TEXT)")
        uow.execute(
            "INSERT INTO boss VALUES('u',?)",
            (json.dumps({"_last_reset": TODAY.isoformat(), str(ITEM_ID): weekly}),),
        )

    config = SimpleNamespace(世界积分商品={str(ITEM_ID): {"cost": 10, "weekly_limit": 10}})
    config_provider = Mock(return_value=config)
    item_provider = Mock(return_value={"name": ITEM_NAME, "type": ITEM_TYPE})
    clock = SimpleNamespace(now=Mock(return_value=SimpleNamespace(date=lambda: TODAY)))
    def current_integral(_user_id):
        return SimpleNamespace(status="ok", integral=int(_query(player, "SELECT integral FROM boss_limit")[0][0]))

    integral = SimpleNamespace(get_integral=Mock(side_effect=current_integral))
    writer_repository = BossPurchaseSqlRepository(game, player, clock=clock)
    writer = BossApplication(game, player, repository=writer_repository, clock=clock)
    reader = BossPurchaseCommandRepository(game)
    application = BossPurchaseCommandApplication(
        game, player, config_provider, item_provider,
        application=writer, repository=reader, integral=integral,
        max_goods_num_provider=lambda: 99, clock=clock,
    )
    return SimpleNamespace(
        game=game, player=player, config=config, config_provider=config_provider,
        item_provider=item_provider, clock=clock, integral=integral,
        writer_repository=writer_repository, writer=writer, reader=reader,
        application=application,
    )


def _state(context):
    return (
        _query(context.player, "SELECT integral FROM boss_limit"),
        _query(context.player, "SELECT weekly_purchases FROM boss"),
        _query(context.game, "SELECT goods_num,bind_num FROM back"),
        _query(context.game, "SELECT operation_id,quantity,cost,status FROM boss_purchase_operations"),
        _query(context.game, "SELECT operation_id,action,status FROM operation_ledger"),
    )


class _Finished(Exception):
    pass


class _Args:
    def __init__(self, text):
        self.text = text

    def extract_plain_text(self):
        return self.text


def _handler(context):
    messages = []
    ids = SimpleNamespace(new_id=Mock(return_value="fresh-id"))

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def send(bot, event, message, **kwargs):
        messages.append((message, kwargs))

    async def finish():
        raise _Finished

    namespace = {
        "assign_bot": assign_bot,
        "check_user": Mock(return_value=(True, {"user_id": "u"}, "")),
        "handle_send": send,
        "boss_purchase_command_application": context.application,
        "render_boss_purchase_reply": render_boss_purchase_reply,
        "runtime_ids": ids,
        "boss_ids": ids,
        "ConflictError": ConflictError,
        "logger": Mock(),
        "boss_integral_use": SimpleNamespace(finish=finish),
        "re": __import__("re"),
    }
    tree = ast.parse(FACADE.read_text(encoding="utf-8"))
    handler = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "boss_integral_use_")
    handler.decorator_list = []
    handler.args.defaults = []
    exec(compile(ast.Module(body=[handler], type_ignores=[]), str(FACADE), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)

    def run(raw, *, event_id="event", fallback_id=""):
        messages.clear()
        event = SimpleNamespace(message_id=event_id, id=fallback_id)
        try:
            asyncio.run(namespace["boss_integral_use_"](SimpleNamespace(self_id="bot"), event, _Args(raw)))
        except _Finished:
            pass
        assert len(messages) == 1
        return messages[0][0]

    return SimpleNamespace(run=run, namespace=namespace, messages=messages, ids=ids)


def test_real_handler_clips_first_quantity_and_replays_original_request_before_live_reads(tmp_path):
    context = _context(tmp_path, weekly=8)
    handler = _handler(context)
    message = handler.run("1 5")
    assert SUCCESS in message and "灵草2个" in message
    assert _query(context.player, "SELECT integral FROM boss_limit") == [(80,)]
    assert _query(context.player, "SELECT weekly_purchases FROM boss") == [
        (json.dumps({"_last_reset": TODAY.isoformat(), "1": 10}),)
    ]
    assert _query(context.game, "SELECT goods_num,bind_num FROM back") == [(2, 2)]
    before = _state(context)
    context.config_provider.side_effect = AssertionError("replay config")
    context.item_provider.side_effect = AssertionError("replay item")
    context.application.weekly_purchases = Mock(side_effect=AssertionError("replay weekly"))
    context.integral.get_integral.side_effect = AssertionError("replay integral")
    context.writer.purchase = Mock(side_effect=AssertionError("replay writer"))
    replay = handler.run("1 5")
    assert REPLAY in replay and "灵草2个" in replay
    assert _state(context) == before


@pytest.mark.parametrize("raw", ["", "1 0", "1 -1", "0 1", "1.5", "1 2 3", "9" * 100])
def test_invalid_command_arguments_do_not_read_or_write(tmp_path, raw):
    context = _context(tmp_path)
    handler = _handler(context)
    with patch.object(context.application, "execute", side_effect=AssertionError("invalid input reached app")) as execute:
        message = handler.run(raw)
        execute.assert_not_called()
    assert "成功兑换" not in message
    assert _state(context)[0:1] == ([(100,)],)


def test_user_conflict_and_quantity_conflict_are_not_success(tmp_path):
    context = _context(tmp_path, weekly=0)
    handler = _handler(context)
    assert SUCCESS in handler.run("1 1")
    before = _state(context)
    other = context.application.execute(
        operation_id="boss-purchase:event:u", user_id="other", item_id=ITEM_ID, quantity=1,
    )
    assert other["status"] == "operation_conflict"
    changed = context.application.execute(
        operation_id="boss-purchase:event:u", user_id="u", item_id=ITEM_ID, quantity=2,
    )
    assert changed["status"] == "operation_conflict"
    assert CONFLICT in render_boss_purchase_reply(other)
    assert CONFLICT in render_boss_purchase_reply(changed)
    assert _state(context) == before


def test_operation_action_conflict_is_rejected_before_live_reads(tmp_path):
    context = _context(tmp_path, weekly=0)
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute(
            "INSERT INTO operation_ledger(operation_id,action,request_hash,status,result_json,created_at,updated_at) "
            "VALUES('boss-purchase:event:u','boss.settle','bad','applied',?,'now','now')",
            ("{}",),
        )
    handler = _handler(context)
    context.config_provider.side_effect = AssertionError("action conflict read")
    assert CONFLICT in handler.run("1 1")
    assert _query(context.game, "SELECT COUNT(*) FROM boss_purchase_operations") == [(0,)]


@pytest.mark.parametrize("status", ["integral_insufficient", "limit_reached", "inventory_full"])
def test_legacy_rejected_receipt_never_renders_success(tmp_path, status):
    context = _context(tmp_path, weekly=0)
    payload = json.dumps(["u", ITEM_ID, ITEM_NAME, ITEM_TYPE, 1, 10, 10, 99], separators=(",", ":"))
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute("DROP TABLE operation_ledger")
        uow.execute(
            "INSERT INTO boss_purchase_operations(operation_id,payload,quantity,cost,integral,purchased,inventory,status) "
            "VALUES(?,?,?,?,?,?,?,?)",
            ("boss-purchase:event:u", payload, 0, 0, 100, 0, 0, status),
        )
    handler = _handler(context)
    context.config_provider.side_effect = AssertionError("legacy receipt must precede config")
    message = handler.run("1 1")
    assert "成功兑换" not in message
    assert status.replace("_", " ") not in message or status == "integral_insufficient"


def test_legacy_receipt_missing_status_is_invalid(tmp_path):
    context = _context(tmp_path, weekly=0)
    payload = json.dumps(["u", ITEM_ID, ITEM_NAME, ITEM_TYPE, 1, 10, 10, 99], separators=(",", ":"))
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute("DROP TABLE operation_ledger")
        uow.execute("ALTER TABLE boss_purchase_operations RENAME TO old_purchase")
        uow.execute(
            "CREATE TABLE boss_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "quantity INTEGER NOT NULL,cost INTEGER NOT NULL,integral INTEGER NOT NULL,purchased INTEGER NOT NULL,inventory INTEGER NOT NULL)"
        )
        uow.execute(
            "INSERT INTO boss_purchase_operations VALUES(?,?,?,?,?,?,?)",
            ("boss-purchase:event:u", payload, 0, 0, 100, 0, 0),
        )
    message = _handler(context).run("1 1")
    assert "成功兑换" not in message and "回执异常" in message


@pytest.mark.parametrize("reader_name", ["receipt", "profile"])
def test_read_failure_never_reaches_purchase_writer(tmp_path, reader_name):
    context = _context(tmp_path)
    handler = _handler(context)
    before = _state(context)
    with patch.object(context.reader, reader_name, side_effect=sqlite3.OperationalError("reader unavailable")), \
            patch.object(context.writer, "purchase", side_effect=AssertionError("writer after receipt failure")) as purchase:
        message = handler.run("1 1")
        purchase.assert_not_called()
    assert "成功兑换" not in message and "异常" in message
    assert _state(context) == before


def test_sql_failure_rolls_back_assets_and_same_operation_retries(tmp_path):
    context = _context(tmp_path, weekly=0)
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute(
            "CREATE TRIGGER fail_purchase_back AFTER INSERT ON back "
            "BEGIN SELECT RAISE(ABORT, 'injected purchase failure'); END"
        )
    handler = _handler(context)
    before = _state(context)
    assert "异常" in handler.run("1 1")
    assert _state(context)[0:3] == before[0:3]
    assert _query(context.game, "SELECT status FROM operation_ledger") == [("failed",)]
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute("DROP TRIGGER fail_purchase_back")
    assert SUCCESS in handler.run("1 1")
    assert _query(context.player, "SELECT integral FROM boss_limit") == [(90,)]
    assert _query(context.game, "SELECT goods_num,bind_num FROM back") == [(1, 1)]
    assert _query(context.game, "SELECT status FROM operation_ledger") == [("applied",)]


def test_handler_preserves_world_boss_buttons_and_identity_fallback(tmp_path):
    context = _context(tmp_path, weekly=0)
    handler = _handler(context)
    assert SUCCESS in handler.run("1", event_id=None, fallback_id="fallback")
    assert handler.messages[0][1]["md_type"] == "世界BOSS"
    assert handler.messages[0][1]["v1"] == "世界BOSS兑换"
    assert handler.messages[0][1]["v2"] == "世界BOSS商店"
    assert handler.messages[0][1]["v3"] == "世界BOSS信息"
    handler.ids.new_id.assert_not_called()
    assert SUCCESS in handler.run("1", event_id=None, fallback_id="")
    handler.ids.new_id.assert_called_once_with()
