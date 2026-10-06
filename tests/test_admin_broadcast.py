from __future__ import annotations

import __future__
import ast
import asyncio
import itertools
import re
from datetime import datetime
from html import unescape
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import quote, unquote

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.admin.broadcast_application import AdminBroadcastApplication
from nonebot_plugin_xiuxian_2.features.admin.broadcast_history_repository import AdminBroadcastHistoryRepository
from nonebot_plugin_xiuxian_2.features.admin.broadcast_repository import AdminBroadcastRepository, adapter_family
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.messaging.models import SendRequest, SendResult


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
ADMIN = ROOT / "xiuxian_admin/__init__.py"
NOW = datetime(2026, 10, 7, 12)
START_HANDLERS = ("group_broadcast_cmd_", "private_broadcast_cmd_", "global_broadcast_cmd_")
HANDLERS = START_HANDLERS + ("view_broadcast_cmd_", "cancel_broadcast_cmd_", "clear_broadcast_cmd_")


def _load(path, names, namespace):
    nodes = [node for node in ast.parse(path.read_text(encoding="utf-8")).body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
             and (names is None or node.name in names)]
    if names is not None:
        assert {node.name for node in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


class Args(str):
    def extract_plain_text(self):
        return str(self)


def _bot(adapter="QQ", bot_id="bot-a"):
    return SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: adapter),
                           self_id=bot_id, call_api=AsyncMock())


@pytest.fixture
def runtime(tmp_path):
    database = tmp_path / "message.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY,adapter TEXT,bot_id TEXT,"
                    "direction TEXT,scene TEXT,group_id TEXT,user_id TEXT,message_id TEXT,created_at TEXT)")
    sequence = itertools.count(1)
    repository = AdminBroadcastRepository(now=lambda: NOW, id_factory=lambda: f"BC{next(sequence):08d}")
    config = SimpleNamespace(markdown_status=False)
    delivery = SimpleNamespace(send=AsyncMock(return_value=SendResult("out", None, {})))
    namespace = _load(ROOT / "broadcast_manager.py", None, {
        "asyncio": asyncio, "datetime": datetime, "logger": Mock(),
        "AdminBroadcastHistoryRepository": AdminBroadcastHistoryRepository,
        "adapter_family": adapter_family,
        "get_paths": lambda: SimpleNamespace(message_db=database),
        "SendRequest": SendRequest, "delivery_service": delivery,
        "XiuConfig": lambda: config,
        "MessageSegment": SimpleNamespace(markdown=lambda bot, content: ("markdown", content)),
        "get_chat_scene": lambda event: event.scene,
        "get_group_id": lambda event: getattr(event, "group_id", ""),
        "get_user_id": lambda event: getattr(event, "user_id", ""),
    })
    app = AdminBroadcastApplication(repository, history=namespace["_history_targets"],
                                    sender=namespace["_send_broadcast_to_target"])
    namespace["_broadcast_application"] = app

    def insert(*, adapter="QQ", bot_id="bot-a", scene="group", target="100", message_id="reply"):
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("INSERT INTO messages(adapter,bot_id,direction,scene,group_id,user_id,message_id,created_at) "
                        "VALUES(?,?,'recv',?,?,?,?,?)",
                        (adapter, bot_id, scene, target, target, message_id, "2026-10-07 11:59:30"))

    return SimpleNamespace(namespace=namespace, app=app, repository=repository, config=config,
                           database=database, delivery=delivery, insert=insert, bot=_bot())


def _handler(runtime, name, raw=""):
    outputs = []

    async def send(bot, event, message):
        outputs.append(message)

    namespace = dict(runtime.namespace, re=re, unescape=unescape, quote=quote, unquote=unquote,
                     assign_bot=AsyncMock(return_value=(runtime.bot, None)),
                     handle_send=send, CommandArg=lambda: None)
    _load(ROOT / "xiuxian_admin/admin_helpers.py", (
        "parse_broadcast_duration_and_content", "parse_clear_broadcast_kind", "fix_mqqapi_inlinecmd_links",
    ), namespace)
    _load(ADMIN, (name,), namespace)
    parameters = (runtime.bot, object())
    if name != "view_broadcast_cmd_":
        parameters += (Args(raw),)
    asyncio.run(namespace[name](*parameters))
    assert len(outputs) == 1
    return outputs[0]


def test_six_handlers_share_facade_and_state_owner(runtime):
    runtime.insert()
    runtime.insert(scene="private", target="200")
    for handler, kind in zip(START_HANDLERS, ("group", "private", "global")):
        message = _handler(runtime, handler, "1\u5c0f\u65f6 announcement")
        task = runtime.app.status()[-1]
        assert task["id"] in message and task["kind"] == kind
        assert task["content"] == "announcement" and task["duration_minutes"] == 60
        assert len(task["sent_groups"]) == (kind != "private")
        assert len(task["sent_users"]) == (kind != "group")
    ids = [task["id"] for task in runtime.app.status()]
    assert all(task_id in _handler(runtime, "view_broadcast_cmd_") for task_id in ids)
    assert ids[0] in _handler(runtime, "cancel_broadcast_cmd_", ids[0].lower())
    assert runtime.app.status()[0]["canceled"]
    _handler(runtime, "clear_broadcast_cmd_", "\u79c1\u804a")
    assert [task["kind"] for task in runtime.app.status()] == ["group", "global"]
    _handler(runtime, "clear_broadcast_cmd_")
    assert runtime.app.status() == []


@pytest.mark.parametrize("scene", ["group", "private", "channel_group", "channel_private"])
def test_qq_delivery_preserves_reply_markdown_and_pending_state(runtime, scene):
    runtime.config.markdown_status = True
    runtime.insert(scene=scene, message_id="new-reply")
    runtime.insert(bot_id="bot-b", scene=scene, target="other", message_id="wrong-reply")
    runtime.delivery.send.return_value = SendResult(None, None, {}, "pending_audit", "audit")
    message = asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "global", "notice"))
    request = runtime.delivery.send.call_args.args[1]
    assert request == SendRequest(scene, "100", ("markdown", "notice"), source_message_id="new-reply")
    task = runtime.app.status()[0]
    assert task["pending_count"] == 1 and not task["sent_groups"] and not task["sent_users"]
    assert "\u672c\u8f6e\u6210\u529f\u53d1\u9001\uff1a0" in message
    assert "\u672c\u8f6e\u5f85\u5ba1\u6838\uff1a1" in message
    event = SimpleNamespace(scene=scene, group_id="100", user_id="100", message_id="next-reply")
    asyncio.run(runtime.namespace["auto_patch_broadcast_for_event"](runtime.bot, event))
    assert runtime.delivery.send.await_count == 1


@pytest.mark.parametrize("markdown", [False, True])
@pytest.mark.parametrize("scene", ["group", "private", "channel_group", "channel_private"])
def test_ob11_plain_and_forward_delivery_ports(runtime, markdown, scene):
    runtime.bot = _bot("OneBot V11")
    runtime.config.markdown_status = markdown
    runtime.insert(adapter="OneBot V11", scene=scene)
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "global", "notice"))
    family = "group" if "group" in scene else "private"
    if markdown:
        runtime.delivery.send.assert_not_awaited()
        call = runtime.bot.call_api.call_args
        assert call.args == (f"send_{family}_forward_msg",)
        assert call.kwargs[f"{'group' if family == 'group' else 'user'}_id"] == 100
        assert call.kwargs["messages"][0]["data"]["content"] == "notice"
    else:
        runtime.bot.call_api.assert_not_awaited()
        assert runtime.delivery.send.call_args.args[1] == SendRequest(family, "100", "notice")
    task = runtime.app.status()[0]
    assert len(task["sent_groups"]) + len(task["sent_users"]) == 1


def test_event_patch_rejects_other_bot_and_sends_new_targets(runtime):
    asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "group", "notice"))
    event = SimpleNamespace(scene="group", group_id="new-group", message_id="event-reply")
    for bot in (_bot(bot_id="bot-b"), _bot("OneBot V11")):
        asyncio.run(runtime.namespace["auto_patch_broadcast_for_event"](bot, event))
    runtime.delivery.send.assert_not_awaited()
    asyncio.run(runtime.namespace["auto_patch_broadcast_for_event"](runtime.bot, event))
    asyncio.run(runtime.namespace["auto_patch_broadcast_for_event"](runtime.bot, event))
    assert runtime.delivery.send.await_count == 1
    assert runtime.delivery.send.call_args.args[1].source_message_id == "event-reply"
    assert runtime.app.status()[0]["sent_groups"] == {"group:new-group"}


@pytest.mark.parametrize("action", ["cancel_broadcast", "clear_broadcast"])
def test_facade_stop_during_initial_send_reports_inflight_and_stops_next_target(runtime, action):
    runtime.insert(target="100")
    runtime.insert(target="200")

    async def deliver(bot, request):
        task_id = runtime.app.status()[0]["id"]
        args = (task_id,) if action == "cancel_broadcast" else ()
        message = runtime.namespace[action](*args)
        assert "\u65e0\u6cd5\u64a4\u56de" in message
        return SendResult("out", None, {})

    runtime.delivery.send.side_effect = deliver
    message = asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "global", "notice"))
    assert runtime.delivery.send.await_count == 1
    assert "\u5df2\u505c\u6b62" in message


@pytest.mark.parametrize("name", START_HANDLERS)
@pytest.mark.parametrize("duration", ["9" * 5000, "999999999999"])
def test_handlers_reject_extreme_duration_without_creating_task(runtime, name, duration):
    message = _handler(runtime, name, duration + "\u5929 notice")
    assert "\u65f6\u95f4\u65e0\u6548" in message
    assert runtime.app.status() == []
    runtime.delivery.send.assert_not_awaited()


@pytest.mark.parametrize("name", START_HANDLERS)
def test_handler_history_failure_is_safe_and_never_reports_created(runtime, name):
    with DatabaseUnitOfWork(runtime.database) as uow:
        uow.execute("ALTER TABLE messages RENAME COLUMN bot_id TO obsolete")
    message = _handler(runtime, name, "notice")
    assert "\u521b\u5efa\u5931\u8d25" in message
    assert "schema" not in message and runtime.app.status() == []


def test_delivery_failure_keeps_retryable_task_and_sanitizes_diagnostics(runtime):
    runtime.insert()
    runtime.delivery.send.side_effect = RuntimeError("sensitive-platform-response")
    message = asyncio.run(runtime.namespace["start_broadcast"](runtime.bot, "group", "notice"))
    task = runtime.app.status()[0]
    assert task["sent_groups"] == set() and task["inflight_count"] == 0
    assert task["errors"][0]["error"] == "RuntimeError"
    assert "\u672c\u8f6e\u53d1\u9001\u5931\u8d25\uff1a1" in message
    assert "sensitive-platform-response" not in str(runtime.namespace["logger"].mock_calls)
    runtime.delivery.send.side_effect = None
    event = SimpleNamespace(scene="group", group_id="100", message_id="new-reply")
    asyncio.run(runtime.namespace["auto_patch_broadcast_for_event"](runtime.bot, event))
    assert runtime.app.status()[0]["sent_groups"] == {"group:100"}


@pytest.mark.parametrize("handler,method", [
    ("view_broadcast_cmd_", "status"), ("cancel_broadcast_cmd_", "cancel"),
    ("clear_broadcast_cmd_", "clear"),
])
def test_state_handler_failure_has_explicit_safe_reply(runtime, monkeypatch, handler, method):
    monkeypatch.setattr(runtime.app, method, Mock(side_effect=RuntimeError("sensitive-state-error")))
    message = _handler(runtime, handler, "BC123")
    assert "\u5931\u8d25" in message and "sensitive-state-error" not in message


def test_web_creation_failure_is_not_success(runtime):
    namespace = _load(ROOT / "xiuxian_web/messages.py", (
        "api_messages_broadcast", "api_messages_broadcast_status",
    ), dict(runtime.namespace,
            session={"admin_id": "admin"}, jsonify=lambda value: value, run_async=asyncio.run,
            get_bot_by_adapter=lambda adapter: runtime.bot,
            request=SimpleNamespace(get_json=lambda: {"adapter": "QQ", "kind": "global", "content": "notice"})))
    assert namespace["api_messages_broadcast"]()["success"] is True
    assert runtime.app.status()[0]["id"] in namespace["api_messages_broadcast_status"]()["message"]
    runtime.app.clear()
    with DatabaseUnitOfWork(runtime.database) as uow:
        uow.execute("ALTER TABLE messages RENAME COLUMN bot_id TO obsolete")
    result = namespace["api_messages_broadcast"]()
    assert result["success"] is False and "schema" not in result["error"]
    assert runtime.app.status() == []


def test_six_broadcast_matchers_remain_superuser_only():
    tree = ast.parse(ADMIN.read_text(encoding="utf-8"))
    matchers = {name.removesuffix("_") for name in HANDLERS}
    found = set()
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        names = {target.id for target in node.targets if isinstance(target, ast.Name)} & matchers
        if names:
            permissions = [keyword.value for keyword in node.value.keywords if keyword.arg == "permission"]
            assert len(permissions) == 1 and ast.unparse(permissions[0]) == "SUPERUSER"
            found.update(names)
    assert found == matchers
