from __future__ import annotations

import __future__
import ast
import asyncio
import copy
from functools import lru_cache
from html import unescape
import json
import math
from pathlib import Path
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, quote, unquote, urlsplit

import pytest


ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
ADMIN = ROOT / "xiuxian_admin/__init__.py"
CONTROLS = ROOT / "xiuxian_admin/command_controls.py"
HELPERS = ROOT / "xiuxian_admin/admin_helpers.py"
UTILS = ROOT / "xiuxian_utils/utils.py"
MANUAL = "\u4fee\u4ed9\u624b\u518c"
SENSITIVE_MARKER = "platform-echoed-user-payload"


@lru_cache(maxsize=None)
def _source_tree(source: Path):
    return ast.parse(source.read_text(encoding="utf-8"), filename=str(source))


def _load_functions(source: Path, names, namespace):
    # Execute only selected real functions, never the legacy module imports.
    nodes = []
    for name in names:
        node = copy.deepcopy(next(
            node for node in _source_tree(source).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
        ))
        node.decorator_list = []
        nodes.append(node)
    exec(compile(
        ast.Module(body=nodes, type_ignores=[]), str(source), "exec",
        flags=__future__.annotations.compiler_flag,
    ), namespace)


class _Args:
    def __init__(self, text=""):
        self.text = text

    def extract_plain_text(self):
        return self.text

    def __str__(self):
        return self.text


def _handler(name, *, source=ADMIN, at_ids=(), config=None):
    config = config or SimpleNamespace(qqq=123456, bot_uin=987654, bot_uid="bot-test-id")
    bot, assigned_bot, event = object(), object(), object()
    namespace = {
        "re": re, "math": math, "json": json, "quote": quote,
        "unquote": unquote, "unescape": unescape,
        "CommandArg": lambda: None,
        "XiuConfig": Mock(return_value=config),
        "assign_bot": AsyncMock(return_value=(assigned_bot, "group-test-id")),
        "get_at_user_ids": Mock(return_value=list(at_ids)),
        "handle_send": AsyncMock(),
        "send_help_message": AsyncMock(),
        "delivery_service": SimpleNamespace(reply=AsyncMock()),
        "MessageSegment": SimpleNamespace(
            markdown_keyboard=Mock(side_effect=lambda bot, body, rows, **kwargs: (
                "keyboard", body, rows, kwargs,
            )),
            markdown=Mock(side_effect=lambda bot, body: ("markdown", body)),
        ),
        "super_help": SimpleNamespace(finish=AsyncMock()),
        "logger": Mock(),
    }
    _load_functions(UTILS, (
        "parse_page_arg", "_normalize_button_pair", "paginate_text_blocks",
        "build_pagination_buttons",
    ), namespace)
    _load_functions(HELPERS, ("_extract_keyboard_command", "_parse_keyboard_test_rows"), namespace)
    _load_functions(source, (name,), namespace)
    return SimpleNamespace(
        **namespace, handler=namespace[name], bot=bot, assigned_bot=assigned_bot, event=event,
    )


def _invoke(harness, args=None):
    arguments = (harness.bot, harness.event)
    if args is not None:
        arguments += (_Args(args),)
    return asyncio.run(harness.handler(*arguments))


def _assert_private_error_not_logged(harness):
    assert harness.logger.mock_calls
    assert SENSITIVE_MARKER not in repr(harness.logger.mock_calls)
    assert "RuntimeError" in harness.logger.mock_calls[-1].args[0]


def _authorization_payload(text):
    match = re.search(r"https://[^\s<>()]+", text)
    assert match is not None, "authorization delivery must retain a usable URL"
    url = urlsplit(match.group())
    assert (url.netloc, url.path) == ("club.vip.qq.com", "/transfer")
    payload = json.loads(parse_qs(url.query)["open_kuikly_info"][0])
    assert payload == {
        "page_name": "ai_group_service_agreement_pop_page", "groupCode": 123456,
        "botUin": 987654, "botUid": "bot-test-id", "screen": 1,
    }
    return payload


@pytest.mark.parametrize("source,matcher,restricted", [
    (ADMIN, "super_help", True), (ADMIN, "broadcast_help_cmd", True),
    (ADMIN, "at_test_cmd", True), (ADMIN, "keyboard_test_cmd", True),
    (CONTROLS, "all_apply_cmd", False),
])
def test_output_command_permission_boundaries(source, matcher, restricted):
    declaration = next(
        node.value for node in _source_tree(source).body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == matcher for target in node.targets)
    )
    assert isinstance(declaration, ast.Call)
    assert ast.unparse(declaration.func) == "on_command"
    keywords = {keyword.arg: keyword.value for keyword in declaration.keywords}
    assert ast.literal_eval(keywords["block"]) is True
    if restricted:
        assert ast.unparse(keywords["permission"]) == "SUPERUSER"
    else:
        assert "permission" not in keywords


@pytest.mark.parametrize("raw,expected_page", [("", 1), ("invalid", 1), ("0", 1), ("2", 2)])
def test_manual_uses_real_pagination_and_shared_help_delivery(raw, expected_page):
    harness = _handler("super_help_")
    _invoke(harness, raw)
    harness.assign_bot.assert_awaited_once_with(bot=harness.bot, event=harness.event)
    harness.send_help_message.assert_awaited_once()
    call = harness.send_help_message.await_args
    assert call.args[:2] == (harness.assigned_bot, harness.event)
    match = re.search(r"\u7b2c(\d+)/(\d+)\u9875", call.args[2])
    assert match and int(match.group(1)) == expected_page
    assert int(match.group(2)) > 2
    commands = [value for key, value in call.kwargs.items() if key.startswith("v")]
    assert f"{MANUAL} {expected_page + 1}" in commands
    assert (f"{MANUAL} 1" in commands) == (expected_page == 2)
    assert len(commands) <= 4
    harness.super_help.finish.assert_awaited_once()
    harness.delivery_service.reply.assert_not_awaited()


def test_manual_clamps_page_past_end_without_next_page_button():
    harness = _handler("super_help_")
    _invoke(harness, "99999")
    call = harness.send_help_message.await_args
    match = re.search(r"\u7b2c(\d+)/(\d+)\u9875", call.args[2])
    assert match and match.group(1) == match.group(2)
    last = int(match.group(2))
    assert f"{MANUAL} {last - 1}" in call.kwargs.values()
    assert f"{MANUAL} {last + 1}" not in call.kwargs.values()


def test_broadcast_help_only_sends_static_help_and_management_buttons():
    harness = _handler("broadcast_help_cmd_")
    _invoke(harness)
    harness.send_help_message.assert_awaited_once()
    call = harness.send_help_message.await_args
    assert call.args[:2] == (harness.assigned_bot, harness.event)
    assert "\u5e7f\u64ad\u7cfb\u7edf\u5e2e\u52a9" in call.args[2]
    assert [call.kwargs[f"v{index}"] for index in range(1, 4)] == [
        "\u67e5\u770b\u5e7f\u64ad", "\u53d6\u6d88\u5e7f\u64ad", "\u6e05\u7a7a\u5e7f\u64ad",
    ]
    harness.delivery_service.reply.assert_not_awaited()
    harness.handle_send.assert_not_awaited()


def test_at_test_without_mentions_does_not_build_a_keyboard():
    harness = _handler("at_test_cmd_")
    _invoke(harness, "")
    harness.handle_send.assert_awaited_once_with(
        harness.assigned_bot, harness.event, "\u65e0\u827e\u7279\n\u827e\u7279ID\u5217\u8868\uff1a[]",
    )
    harness.MessageSegment.markdown_keyboard.assert_not_called()
    harness.delivery_service.reply.assert_not_awaited()


@pytest.mark.parametrize("count", [1, 6, 25, 26])
def test_at_test_limits_buttons_to_five_rows_and_preserves_all_reported_ids(count):
    ids = [f"user-{index}" for index in range(count)]
    harness = _handler("at_test_cmd_", at_ids=ids)
    _invoke(harness, "mentions")
    call = harness.MessageSegment.markdown_keyboard.call_args
    assert call.args[0] is harness.assigned_bot
    body, rows = call.args[1:]
    assert len(rows) <= 5 and all(len(row) <= 5 for row in rows)
    assert [button for row in rows for button in row] == [
        (str(index + 1), uid) for index, uid in enumerate(ids[:25])
    ]
    assert all(f"{index + 1}. {uid}" in body for index, uid in enumerate(ids))
    assert ("\u524d25\u4e2a" in body) == (count > 25)
    harness.delivery_service.reply.assert_awaited_once()
    assert harness.delivery_service.reply.await_args.args[:2] == (harness.assigned_bot, harness.event)
    harness.handle_send.assert_not_awaited()


@pytest.mark.parametrize("failure_stage", ["build", "send"])
def test_at_test_falls_back_without_logging_the_platform_payload(failure_stage):
    harness = _handler("at_test_cmd_", at_ids=["first-user", "second-user"])
    failing = harness.MessageSegment.markdown_keyboard if failure_stage == "build" else harness.delivery_service.reply
    failing.side_effect = RuntimeError(SENSITIVE_MARKER)
    _invoke(harness, "mentions")
    harness.handle_send.assert_awaited_once()
    assert "1. first-user" in harness.handle_send.await_args.args[2]
    assert "2. second-user" in harness.handle_send.await_args.args[2]
    _assert_private_error_not_logged(harness)


@pytest.mark.parametrize("raw,expected", [
    ("", [[("1", "2"), ("3", "3")]]),
    ("[First](mqqapi://aio/inlinecmd?command=hello%20world&enter=false)\\nSecond",
     [[("First", "hello world")], [("Second", "Second")]]),
])
def test_keyboard_test_uses_real_parser_and_shared_delivery(raw, expected):
    harness = _handler("keyboard_test_cmd_")
    _invoke(harness, raw)
    harness.MessageSegment.markdown_keyboard.assert_called_once_with(harness.assigned_bot, " ", expected)
    harness.delivery_service.reply.assert_awaited_once()
    harness.handle_send.assert_not_awaited()


@pytest.mark.parametrize("failure_stage", ["build", "send"])
def test_keyboard_test_keeps_user_diagnostic_out_of_logs(failure_stage):
    harness = _handler("keyboard_test_cmd_")
    failing = harness.MessageSegment.markdown_keyboard if failure_stage == "build" else harness.delivery_service.reply
    failing.side_effect = RuntimeError(f"message={SENSITIVE_MARKER},code=400")
    _invoke(harness, "button")
    harness.handle_send.assert_awaited_once()
    assert "400" in harness.handle_send.await_args.args[2]
    _assert_private_error_not_logged(harness)


@pytest.mark.parametrize("raw", ["", "not-a-group", "12x", "\u00b2", "0", "-1", "9" * 5000])
def test_all_apply_rejects_invalid_group_without_attempting_delivery(raw):
    harness = _handler("all_apply_cmd_", source=CONTROLS)
    _invoke(harness, raw)
    harness.handle_send.assert_awaited_once()
    harness.MessageSegment.markdown_keyboard.assert_not_called()
    harness.MessageSegment.markdown.assert_not_called()
    harness.delivery_service.reply.assert_not_awaited()


@pytest.mark.parametrize("uin,uid", [(0, "bot-test-id"), (987654, ""), ("invalid", "bot-test-id")])
def test_all_apply_rejects_missing_or_invalid_bot_configuration(uin, uid):
    harness = _handler("all_apply_cmd_", source=CONTROLS, config=SimpleNamespace(
        qqq=123456, bot_uin=uin, bot_uid=uid,
    ))
    _invoke(harness, "123456")
    harness.handle_send.assert_awaited_once()
    harness.MessageSegment.markdown_keyboard.assert_not_called()
    harness.delivery_service.reply.assert_not_awaited()


def test_all_apply_creates_group_owner_keyboard_with_structured_authorization_payload():
    harness = _handler("all_apply_cmd_", source=CONTROLS)
    _invoke(harness, "123456")
    call = harness.MessageSegment.markdown_keyboard.call_args
    assert call.args[0] is harness.assigned_bot
    assert call.kwargs == {"permission_type": 3, "specify_role_ids": ["4"]}
    assert len(call.args[2]) == 1 and len(call.args[2][0]) == 1
    _authorization_payload(call.args[2][0][0][1])
    harness.delivery_service.reply.assert_awaited_once()
    assert harness.delivery_service.reply.await_args.args[:2] == (harness.assigned_bot, harness.event)
    harness.MessageSegment.markdown.assert_not_called()
    harness.handle_send.assert_not_awaited()


@pytest.mark.parametrize("failure_stage", ["build", "send"])
def test_all_apply_markdown_fallback_retains_authorization_url(failure_stage):
    harness = _handler("all_apply_cmd_", source=CONTROLS)
    if failure_stage == "build":
        harness.MessageSegment.markdown_keyboard.side_effect = RuntimeError(SENSITIVE_MARKER)
    else:
        harness.delivery_service.reply.side_effect = [RuntimeError(SENSITIVE_MARKER), None]
    _invoke(harness, "123456")
    harness.MessageSegment.markdown.assert_called_once()
    _authorization_payload(harness.MessageSegment.markdown.call_args.args[1])
    assert harness.delivery_service.reply.await_args.args[2][0] == "markdown"
    harness.handle_send.assert_not_awaited()
    _assert_private_error_not_logged(harness)


@pytest.mark.parametrize("failure_stage", ["build", "send"])
def test_all_apply_plain_fallback_retains_authorization_url(failure_stage):
    harness = _handler("all_apply_cmd_", source=CONTROLS)
    if failure_stage == "build":
        harness.MessageSegment.markdown_keyboard.side_effect = RuntimeError(SENSITIVE_MARKER)
        harness.MessageSegment.markdown.side_effect = RuntimeError(SENSITIVE_MARKER)
    else:
        harness.delivery_service.reply.side_effect = RuntimeError(SENSITIVE_MARKER)
    _invoke(harness, "123456")
    harness.handle_send.assert_awaited_once()
    _authorization_payload(harness.handle_send.await_args.args[2])
    _assert_private_error_not_logged(harness)
