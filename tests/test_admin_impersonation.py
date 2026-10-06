from __future__ import annotations

import __future__
import ast
import asyncio
import builtins
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import tests
import pytest

from nonebot_plugin_xiuxian_2.features.admin.impersonation_application import AdminImpersonationApplication
from nonebot_plugin_xiuxian_2.features.admin.impersonation_repository import AdminImpersonationRepository


PACKAGE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
ADMIN = PACKAGE / "xiuxian/xiuxian_admin/__init__.py"
UTILS = PACKAGE / "xiuxian/xiuxian_utils/utils.py"


def _load(path, names, namespace):
    nodes = [node for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert len(nodes) == len(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


class Args(str):
    def __new__(cls, text, mentioned=None):
        result = super().__new__(cls, text)
        result.mentioned = mentioned
        return result

    def extract_plain_text(self):
        return str(self)


class Event:
    def get_user_id(self):
        return "real-admin"


@pytest.fixture
def runtime():
    repository = AdminImpersonationRepository()
    application = AdminImpersonationApplication(repository)
    profiles = {
        "registered": {"user_id": "registered", "user_name": "named-user", "is_ban": 0},
        "mentioned": {"user_id": "mentioned", "user_name": "mentioned-user", "is_ban": 0},
        "avatar": {"user_id": "avatar", "user_name": "avatar-user", "is_ban": 0},
    }
    reader = SimpleNamespace(
        get_user_info_with_id=Mock(side_effect=lambda uid: profiles.get(uid)),
        get_user_info_with_name=Mock(side_effect=lambda name: profiles["registered"] if name == "named-user" else None),
        get_user_cd=Mock(return_value={"type": 0}),
    )
    outputs = []

    async def send(bot, event, message):
        outputs.append(message)

    namespace = {
        "admin_impersonation_application": application,
        "impersonation_application": application,
        "_impersonating_users": application.mapping,
        "_sql_message": lambda: reader,
        "assign_bot": AsyncMock(return_value=(object(), None)),
        "handle_send": send,
        "has_at_user": lambda args: bool(args.mentioned),
        "get_at_user_id": lambda args: args.mentioned,
        "get_user_id": lambda event: "avatar",
        "CommandArg": lambda: None,
        "logger": Mock(),
    }
    _load(ADMIN, ("impersonate_user_command_",), namespace)
    return SimpleNamespace(repository=repository, application=application, reader=reader,
                           profiles=profiles, outputs=outputs, namespace=namespace)


def _call(runtime, raw="", mentioned=None):
    runtime.outputs.clear()
    asyncio.run(runtime.namespace["impersonate_user_command_"](
        object(), Event(), Args(raw, mentioned),
    ))
    assert len(runtime.outputs) == 1
    return runtime.outputs[0]


@pytest.mark.parametrize("raw,mentioned,expected", [
    ("named-user", None, "registered"),
    ("named-user", "mentioned", "mentioned"),
    ("not-registered/target", None, "not-registered/target"),
    ("", "unregistered-mention", "unregistered-mention"),
])
def test_handler_sets_actual_admin_key_for_name_mention_and_unregistered_id(runtime, raw, mentioned, expected):
    message = _call(runtime, raw, mentioned)
    assert runtime.application.get_target("real-admin") == expected
    assert runtime.application.get_target("avatar") is None
    assert runtime.application.mapping["real-admin"] == expected
    assert "\u6210\u529f\u4f2a\u88c5" in message
    if mentioned:
        runtime.reader.get_user_info_with_name.assert_not_called()


def test_handler_query_and_cancel_read_same_owner(runtime):
    assert "\u7528\u6cd5" in _call(runtime)
    _call(runtime, "named-user")
    assert "named-user" in _call(runtime)
    assert "\u5df2\u53d6\u6d88" in _call(runtime, "off")
    assert runtime.application.mapping.get("real-admin") is None
    assert "\u6ca1\u6709\u4f2a\u88c5" in _call(runtime, "\u53d6\u6d88")


def test_handler_lookup_failure_leaves_existing_identity_and_hides_error(runtime):
    runtime.application.set_target("real-admin", "original")
    runtime.reader.get_user_info_with_name.side_effect = RuntimeError("sensitive-database-error")
    message = _call(runtime, "new-target")
    assert runtime.application.get_target("real-admin") == "original"
    assert "\u64cd\u4f5c\u5931\u8d25" in message
    assert "sensitive-database-error" not in message
    assert "sensitive-database-error" not in str(runtime.namespace["logger"].mock_calls)


def test_handler_builds_valid_reply_before_replacing_identity(runtime):
    runtime.application.set_target("real-admin", "original")
    runtime.reader.get_user_info_with_name.side_effect = None
    runtime.reader.get_user_info_with_name.return_value = {"user_id": "broken-profile"}
    assert "\u64cd\u4f5c\u5931\u8d25" in _call(runtime, "new-target")
    assert runtime.application.get_target("real-admin") == "original"


def _consumers(runtime):
    imports = builtins.__import__
    blackhouse = SimpleNamespace(is_user_blackhoused=Mock(return_value=False))

    def import_port(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "blackhouse" and level == 2:
            return blackhouse
        return imports(name, globals, locals, fromlist, level)

    data_manager = SimpleNamespace(
        get_fields=Mock(return_value={"user_id": "registered", "count": 2}),
        get_field_data=Mock(return_value=2),
        update_or_write_data=Mock(),
    )
    avatar = SimpleNamespace(get_active_user_id=Mock(return_value="avatar"))
    namespace = dict(
        runtime.namespace,
        __builtins__=dict(vars(builtins), __import__=import_port),
        GroupMessageEvent=Event,
        PrivateMessageEvent=type("PrivateEvent", (), {}),
        _player_avatar=lambda: avatar,
        get_user_profile=Mock(side_effect=lambda uid: dict(runtime.profiles[uid]) if uid in runtime.profiles else None),
        _player_data_manager=lambda: data_manager,
        MessageSegment=SimpleNamespace(text=lambda bot, text: text),
        is_group_event=lambda event: False,
        XiuConfig=lambda: SimpleNamespace(message_optimization=False),
        _send_event_message=AsyncMock(),
    )
    _load(UTILS, (
        "get_impersonating_target", "check_user", "check_user_type", "check_user_md_type",
        "get_statistics_data", "update_statistics_value", "handle_pic_msg_send",
    ), namespace)
    return namespace, data_manager, blackhouse


@pytest.mark.parametrize("use_event", [True, False])
def test_check_user_prefers_owner_over_avatar_and_reverts_after_cancel(runtime, use_event):
    _call(runtime, "named-user")
    namespace, _, _ = _consumers(runtime)
    user_input = Event() if use_event else "real-admin"
    ok, profile, _ = namespace["check_user"](user_input)
    assert ok and profile["user_id"] == "registered"
    assert namespace["get_impersonating_target"]("real-admin") == "registered"
    _call(runtime, "off")
    ok, profile, _ = namespace["check_user"](user_input)
    assert ok and profile["user_id"] == "avatar"


def test_state_send_and_statistics_consumers_share_the_handler_owner(runtime):
    _call(runtime, "named-user")
    namespace, data_manager, _ = _consumers(runtime)
    assert namespace["check_user_type"]("real-admin", 0) == (True, "")
    runtime.reader.get_user_cd.assert_called_with("registered")
    namespace["check_user_md_type"](0, Event())
    runtime.reader.get_user_cd.assert_called_with("registered")
    assert namespace["get_statistics_data"]("real-admin", "count") == 2
    data_manager.get_field_data.assert_called_once_with("registered", "statistics", "count")
    namespace["update_statistics_value"]("real-admin", "count", increment=3)
    data_manager.update_or_write_data.assert_called_once_with(
        "registered", "statistics", "count", 5, data_type="INTEGER",
    )
    asyncio.run(namespace["handle_pic_msg_send"](object(), Event(), text="message"))
    request = namespace["_send_event_message"].call_args.kwargs
    assert "named-user" in request["message"] and "message" in request["message"]
    assert "\u4f2a\u88c5" in request["message"]


def test_consumer_uses_one_atomic_identity_snapshot_when_cancel_races(runtime, monkeypatch):
    _call(runtime, "named-user")
    namespace, _, _ = _consumers(runtime)
    get_target = runtime.application.get_target

    def get_then_cancel(admin_id):
        target = get_target(admin_id)
        runtime.application.cancel(admin_id)
        return target

    monkeypatch.setattr(runtime.application, "get_target", get_then_cancel)
    assert namespace["check_user_type"]("real-admin", 0) == (True, "")
    runtime.reader.get_user_cd.assert_called_once_with("registered")
    assert runtime.repository.snapshot() == {}


def test_legacy_projection_get_reads_same_mapping_without_a_second_dictionary(runtime):
    _call(runtime, "named-user")
    namespace = _load(PACKAGE / "compatibility/buff_closing_effects.py", ("_statistics_user",), {
        "sys": SimpleNamespace(modules={
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils":
                SimpleNamespace(_impersonating_users=runtime.application.mapping),
        }),
    })
    assert namespace["_statistics_user"]("real-admin") == "registered"
    _call(runtime, "off")
    assert namespace["_statistics_user"]("real-admin") == "real-admin"


def test_impersonation_matcher_remains_superuser_and_utils_has_no_check_then_lookup():
    tree = ast.parse(ADMIN.read_text(encoding="utf-8"))
    matcher = next(node.value for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "impersonate_user_command" for target in node.targets))
    assert any(keyword.arg == "permission" and ast.unparse(keyword.value) == "SUPERUSER"
               for keyword in matcher.keywords)
    utils_tree = ast.parse(UTILS.read_text(encoding="utf-8"))
    assert not any(isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
                   and node.value.id == "_impersonating_users" for node in ast.walk(utils_tree))
