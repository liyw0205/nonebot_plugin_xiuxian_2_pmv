from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_position_failure_reason_survives_handler_and_qq_fallbacks():
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    event = make_event("我的位置", "900001", 973899)
    capture = AsyncMock()
    handler = module.my_pos.handlers[0].call
    error = DomainError(
        "conflict",
        "isolated write rejection",
        {"reason": "map_status_write_rejected", "subject": "map_status"},
    )
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(module, "check_user", return_value=(True, {"user_id": "900001"}, "")),
        patch.object(module.map_application, "map_status", side_effect=error),
        patch.object(utils, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event))

    message = capture.await_args.args[2]
    assert "我的位置处理提示：地图位置" in message
    assert reason_text("map_status_write_rejected") in message

    for bot, markdown_enabled, fail_markdown in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fail_markdown:
            markdown.side_effect = RuntimeError("isolated QQ Markdown failure")
        config = SimpleNamespace(
            markdown_status=markdown_enabled,
            merge_forward_send=4 if markdown_enabled else 1,
            markdown_id="",
            markdown_button_status=False,
            reference_reply=False,
            message_optimization=False,
            img=False,
            at_sender=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "handle_send2", plain),
            patch.object(utils, "handle_send_markdown", markdown),
        ):
            asyncio.run(utils.send_msg_handler(bot, event, "我的位置", "900000", [message]))

        delivered = markdown if markdown_enabled and not fail_markdown else plain
        delivered.assert_awaited_once()
        assert "我的位置处理提示：地图位置" in delivered.await_args.args[2]
        assert reason_text("map_status_write_rejected") in delivered.await_args.args[2]
