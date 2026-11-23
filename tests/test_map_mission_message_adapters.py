from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_map_mission_rejection_survives_onebot_and_qq_message_modes():
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.features.map.schemas import MapMissionClaimResult
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    reason = "map_mission_claim_receipt_write_rejected"
    message = result_failure(
        "领取地图委托",
        MapMissionClaimResult(
            "state_changed",
            0,
            (),
            reason=reason,
            subject="receipt",
        ),
    )
    event = make_event("委托完成", "900001", 978199)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))

    for bot, markdown_enabled, fail_markdown in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fail_markdown:
            markdown.side_effect = RuntimeError("isolated QQ Markdown delivery failure")
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
            asyncio.run(utils.send_msg_handler(bot, event, "领取地图委托", "900000", [message]))

        delivered = markdown if markdown_enabled and not fail_markdown else plain
        delivered.assert_awaited_once()
        text = str(delivered.await_args.args[2])
        assert reason_text(reason) in text, text
        assert "操作回执" in text and "本次奖励已撤销" in text, text
