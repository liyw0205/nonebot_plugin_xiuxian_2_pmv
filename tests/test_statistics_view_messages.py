from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def _config():
    return SimpleNamespace(
        at_sender=False,
        img=False,
        markdown_button_status=False,
        markdown_id="",
        markdown_id2="",
        markdown_status=True,
        merge_forward_send=1,
        message_optimization=False,
    )


def _send(adapter: str, *, markdown_fails: bool = False):
    bot = SimpleNamespace(
        self_id="900000", adapter=SimpleNamespace(get_name=lambda: adapter)
    )
    event = SimpleNamespace(get_user_id=lambda: "900001")
    plain = AsyncMock()
    markdown = AsyncMock(
        side_effect=RuntimeError("isolated markdown failure") if markdown_fails else None
    )
    title = "【修仙统计数据】\n"
    message = "- 修仙签到：1.7万\r- 闭关次数：5"
    with (
        patch.object(utils, "XiuConfig", return_value=_config()),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "handle_send2", plain),
        patch.object(utils, "handle_send_markdown", markdown),
    ):
        asyncio.run(
            utils.send_msg_handler(
                bot, event, "统计数据", bot.self_id, [message], title=title
            )
        )
    return title, message, plain, markdown


def test_statistics_view_message_survives_onebot_qq_markdown_and_fallback():
    expected = ("修仙签到：1.7万", "闭关次数：5")

    title, message, onebot_plain, onebot_markdown = _send("OneBot V11")
    assert onebot_plain.await_args.args[2] == title + message
    assert all(part in onebot_plain.await_args.args[2] for part in expected)
    assert not onebot_markdown.called

    title, message, qq_plain, qq_markdown = _send("QQ")
    assert all(part in qq_markdown.await_args.args[2] for part in expected)
    assert qq_markdown.await_args.kwargs["title"] == title
    assert not qq_plain.called

    title, message, fallback_plain, failed_markdown = _send(
        "QQ", markdown_fails=True
    )
    assert failed_markdown.called
    assert fallback_plain.await_args.args[2] == title + message
