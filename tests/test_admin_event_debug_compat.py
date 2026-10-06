from __future__ import annotations

import __future__
import ast
import asyncio
import json
from pathlib import Path
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import unquote

import pytest


SOURCE = (
    Path(__file__).resolve().parents[1]
    / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/event_debug.py"
)
LINK_OK = "\u83b7\u53d6\u6210\u529f"
LINK_FAILED = "\u83b7\u53d6\u5931\u8d25"


class Event:
    def __init__(self, **values):
        self.message = "debug message"
        self.message_id = "current-message"
        self.user_id = "operator"
        self.group_id = "group"
        self.__dict__.update(values)

    def model_dump(self):
        return dict(vars(self))

    def get_message(self):
        return self.message

    def get_type(self):
        return "message"

    def get_event_name(self):
        return "message.group"

    def get_user_id(self):
        return self.user_id

    def get_session_id(self):
        return f"group_{self.group_id}_{self.user_id}"

    def is_tome(self):
        return True


@pytest.fixture
def debug():
    bot = SimpleNamespace(self_id="bot")
    namespace = {
        "json": json,
        "re": re,
        "unquote": unquote,
        "model_dump": lambda value: value.model_dump(),
        "XiuConfig": Mock(return_value=SimpleNamespace(markdown_status=False, markdown_id="")),
        "assign_bot": AsyncMock(return_value=(bot, "group")),
        "handle_send": AsyncMock(),
        "send_msg_handler": AsyncMock(),
        "delivery_service": SimpleNamespace(reply=AsyncMock()),
        "MessageSegment": SimpleNamespace(
            markdown=Mock(side_effect=lambda bot, text: {"markdown": text})
        ),
        "logger": Mock(),
        "bot": bot,
    }
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = []
            nodes.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in {"_LOOSE_URL_RE", "_MD_URL_RE"}
            for target in node.targets
        ):
            nodes.append(node)
    # Execute the real handlers/helpers without importing the plugin lifecycle.
    exec(
        compile(
            ast.Module(body=nodes, type_ignores=[]),
            str(SOURCE),
            "exec",
            flags=__future__.annotations.compiler_flag,
        ),
        namespace,
    )
    return namespace


def invoke(debug, name, event):
    asyncio.run(debug[name](debug["bot"], event))


def test_event_info_handler_reports_current_event_and_raw_payload(debug):
    event = Event(message="only this event", attachments=[{"url": "https://fixture.invalid/a"}])
    invoke(debug, "parse_event_cmd_", event)
    debug["assign_bot"].assert_awaited_once_with(bot=debug["bot"], event=event)
    debug["handle_send"].assert_awaited_once()
    message = debug["handle_send"].await_args.args[2]
    assert "only this event" in message
    assert "current-message" in message
    assert "https://fixture.invalid/a" in message
    assert '"attachments"' in message
    debug["delivery_service"].reply.assert_not_awaited()


@pytest.mark.parametrize("template", [False, True])
def test_event_info_handler_uses_configured_markdown_output(debug, template):
    debug["XiuConfig"].return_value = SimpleNamespace(
        markdown_status=True, markdown_id="template" if template else ""
    )
    invoke(debug, "parse_event_cmd_", Event())
    if template:
        debug["send_msg_handler"].assert_awaited_once()
        assert "current-message" in debug["send_msg_handler"].await_args.args[4][0]
        debug["delivery_service"].reply.assert_not_awaited()
    else:
        debug["delivery_service"].reply.assert_awaited_once()
        assert "current-message" in debug["delivery_service"].reply.await_args.args[2]["markdown"]
    debug["handle_send"].assert_not_awaited()


@pytest.mark.parametrize("has_reply", [False, True])
def test_link_handler_extracts_and_deduplicates_current_event_without_fetching(debug, has_reply):
    debug["_send_blocks"] = AsyncMock()
    url = "https://fixture.invalid/asset.png"
    event = Event(
        message=f"[asset]({url}) https:\\/\\/fixture.invalid\\/%61sset.png",
        attachments={"nested": ["https://fixture.invalid/second"]},
    )
    if has_reply:
        event.reply = SimpleNamespace(message_id="quoted", message=url)
    debug["bot"].get_msg = Mock(side_effect=AssertionError("must not fetch history"))
    invoke(debug, "fetch_link_cmd_", event)
    call = debug["_send_blocks"].await_args
    assert call.args[2] == LINK_OK
    assert call.args[3].splitlines() == [url, "https://fixture.invalid/second"]
    assert call.kwargs == {"code_lang": "text", "escape_body_urls": False}
    debug["bot"].get_msg.assert_not_called()


@pytest.mark.parametrize("has_reply", [False, True])
def test_link_handler_distinguishes_missing_reference_from_no_links(debug, has_reply):
    debug["_send_blocks"] = AsyncMock()
    event = Event()
    if has_reply:
        event.reply = SimpleNamespace(message_id="quoted", message="no link")
    invoke(debug, "fetch_link_cmd_", event)
    call = debug["_send_blocks"].await_args
    assert call.args[2] == LINK_FAILED
    if has_reply:
        assert "\u672a\u5728\u5f15\u7528\u6d88\u606f" in call.args[3]
    else:
        assert "\u8bf7\u5148\u5f15\u7528" in call.args[3]


def test_raw_handler_serializes_current_event_and_truncates_large_output(debug):
    debug["_send_blocks"] = AsyncMock()
    invoke(debug, "fetch_raw_cmd_", Event(message="x" * 15000))
    call = debug["_send_blocks"].await_args
    assert call.args[2].endswith("JSON")
    body = call.args[3]
    assert body.startswith('{\n  "message": "')
    assert 10000 < len(body) < 10100
    assert "......" in body
    assert call.kwargs == {"code_lang": "json", "escape_body_urls": False}


@pytest.mark.parametrize("size", [0, 10, 11])
def test_truncation_keeps_short_payloads_and_reports_original_length(debug, size):
    result = debug["_truncate"]("x" * size, limit=10)
    if size <= 10:
        assert result == "x" * size
    else:
        assert result.startswith("x" * 10 + "\n\n......")
        assert str(size) in result


@pytest.mark.parametrize("values,expected", [
    ({"reply": {"message_id": "raw", "content": "quoted text"}}, {"message_id": "raw", "content": "quoted text"}),
    ({"original_message": [SimpleNamespace(type="reply", data={"id": "segment"})]}, {"source": "original_message.reply_segment", "message_id": "segment"}),
    ({"message_reference": SimpleNamespace(message_id="reference")}, {"source": "message_reference", "message_id": "reference"}),
    ({"message_scene": {"ext": [{"key": "ref_msg_idx", "value": "index"}]}}, {"source": "message_scene.ext.ref_msg_idx", "ref_msg_idx": "index"}),
])
def test_reply_handler_prefers_raw_reply_and_supports_adapter_reference_shapes(debug, values, expected):
    debug["_send_blocks"] = AsyncMock()
    invoke(debug, "fetch_reply_cmd_", Event(**values))
    call = debug["_send_blocks"].await_args
    assert call.args[2].endswith("reply")
    assert json.loads(call.args[3]) == expected
    assert call.kwargs == {"code_lang": "json", "escape_body_urls": False}


def test_reply_handler_reports_absent_reference(debug):
    debug["_send_blocks"] = AsyncMock()
    invoke(debug, "fetch_reply_cmd_", Event())
    call = debug["_send_blocks"].await_args
    assert call.args[2] == LINK_FAILED
    assert "raw" in call.args[3] and "reply" in call.args[3]


@pytest.mark.parametrize("failed_deliveries", [0, 1, 2])
def test_send_blocks_falls_back_from_template_to_native_plain_and_shared_sender(debug, failed_deliveries):
    debug["XiuConfig"].return_value = SimpleNamespace(markdown_status=True, markdown_id="template")
    debug["send_msg_handler"].side_effect = RuntimeError("template rejected")
    debug["delivery_service"].reply.side_effect = (
        [RuntimeError("delivery rejected")] * failed_deliveries + [None]
    )
    event = Event()
    body = "```\nhttps://fixture.invalid/asset\n[link](https://fixture.invalid/target)"
    asyncio.run(debug["_send_blocks"](debug["bot"], event, "title", body))
    debug["send_msg_handler"].assert_awaited_once()
    calls = debug["delivery_service"].reply.await_args_list
    assert len(calls) == min(failed_deliveries + 1, 2)
    assert "https://fixture.invalid/asset" in calls[0].args[2]["markdown"]
    assert "[link]" not in calls[0].args[2]["markdown"]
    if failed_deliveries:
        assert calls[1].args[2] == f"title\n\n{body}"
    if failed_deliveries == 2:
        debug["handle_send"].assert_awaited_once_with(debug["bot"], event, f"title\n\n{body}")
    else:
        debug["handle_send"].assert_not_awaited()


def test_event_info_native_failure_falls_back_to_shared_sender(debug):
    debug["XiuConfig"].return_value = SimpleNamespace(markdown_status=True, markdown_id="")
    debug["delivery_service"].reply.side_effect = RuntimeError("native rejected")
    invoke(debug, "parse_event_cmd_", Event())
    debug["handle_send"].assert_awaited_once()
    assert "current-message" in debug["handle_send"].await_args.args[2]
    debug["logger"].warning.assert_called_once()


@pytest.mark.parametrize("handler,helper", [
    ("parse_event_cmd_", "_build_event_info_blocks"),
    ("fetch_link_cmd_", "_extract_reply_info"),
    ("fetch_raw_cmd_", "_event_to_dict"),
    ("fetch_reply_cmd_", "_extract_reply_raw_payload"),
])
def test_handlers_log_processing_failures_and_return_error_response(debug, handler, helper):
    debug[helper] = Mock(side_effect=ValueError("decode failed"))
    debug["_send_blocks"] = AsyncMock()
    invoke(debug, handler, Event())
    debug["logger"].error.assert_called_once()
    response = debug["handle_send"] if handler == "parse_event_cmd_" else debug["_send_blocks"]
    response.assert_awaited_once()
    assert "decode failed" in response.await_args.args[-1]
