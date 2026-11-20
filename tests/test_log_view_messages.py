from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def _config(markdown: bool) -> SimpleNamespace:
    return SimpleNamespace(
        at_sender=False,
        img=False,
        markdown_button_status=False,
        markdown_id="",
        markdown_id2="",
        markdown_status=markdown,
        merge_forward_send=1,
        message_optimization=False,
    )


def _send_log_view(adapter: str, *, markdown_fails: bool = False):
    bot = SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: adapter))
    event = SimpleNamespace(get_user_id=lambda: "900001")
    plain = AsyncMock()
    markdown = AsyncMock(side_effect=RuntimeError("isolated markdown failure") if markdown_fails else None)
    message = "【修仙日志】\n日期：261120\n页码：2/2\n[2026-11-20 12:00:00]\n真实日志内容"
    with (
        patch.object(utils, "XiuConfig", return_value=_config(True)),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "_send_event_message", plain),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(utils.send_msg_handler(bot, event, "修仙日志", bot.self_id, [message]))
    return message, plain, markdown


def test_log_view_summary_survives_onebot_qq_markdown_and_fallback():
    expected = ("修仙日志", "页码：2/2", "真实日志内容")

    message, onebot_plain, onebot_markdown = _send_log_view("OneBot V11")
    assert onebot_plain.call_args.kwargs["message"] == message
    assert not onebot_markdown.called

    message, qq_plain, qq_markdown = _send_log_view("QQ")
    assert all(part in qq_markdown.call_args.args[2] for part in expected)
    assert not qq_plain.called

    message, fallback_plain, failed_markdown = _send_log_view("QQ", markdown_fails=True)
    assert failed_markdown.called
    assert fallback_plain.call_args.kwargs["message"] == message
