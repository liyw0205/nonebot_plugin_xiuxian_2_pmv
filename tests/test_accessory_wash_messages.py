from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.back.accessory_wash_domain import AccessoryWashChange
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_accessory_wash_failure_reasons_keep_uid_across_onebot_qq_and_fallback():
    event = SimpleNamespace(get_user_id=lambda: "900001")
    bot_names = ("OneBot V11", "QQ")
    failures = (
        ("accessory_wash_stone_write_rejected", "洗练石扣除未能保存"),
        ("accessory_wash_state_projection_mismatch", "饰品洗练状态与请求不一致"),
        ("accessory_wash_receipt_write_rejected", "饰品洗练操作回执保存失败"),
    )

    for reason, phrase in failures:
        outcome = AccessoryWashChange(
            "failed", "wash", "900001", reason=reason, subject="fixture-accessory-01"
        )
        reply = accessory._acc_fail_msg(outcome, action="洗练", subject="fixture-accessory-01")
        assert phrase in reply and "fixture-accessory-01" in reply

        for bot_name in bot_names:
            bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: bot_name))
            config = SimpleNamespace(
                markdown_status=bot_name == "QQ",
                markdown_id="",
                markdown_id2="",
                markdown_button_status=False,
                at_sender=False,
                message_optimization=False,
                img=False,
            )
            plain = AsyncMock()
            markdown = AsyncMock()
            with (
                patch.object(utils, "XiuConfig", return_value=config),
                patch.object(utils, "get_chat_scene", return_value="group"),
                patch.object(utils, "is_group_event", return_value=True),
                patch.object(utils, "_send_event_message", plain),
                patch.object(utils, "handle_send_markdown_type", markdown),
            ):
                asyncio.run(
                    utils.handle_send(
                        bot,
                        event,
                        reply,
                        md_type="背包",
                        k1="饰品",
                        v1="饰品背包",
                    )
                )
            if bot_name == "OneBot V11":
                assert phrase in plain.call_args.kwargs["message"]
            else:
                assert phrase in markdown.call_args.args[2]

        failed_markdown = AsyncMock(side_effect=RuntimeError("isolated QQ wash delivery failure"))
        fallback = AsyncMock()
        config = SimpleNamespace(
            markdown_status=True,
            markdown_id="",
            markdown_id2="",
            markdown_button_status=False,
            at_sender=False,
            message_optimization=False,
            img=False,
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "get_chat_scene", return_value="group"),
            patch.object(utils, "is_group_event", return_value=True),
            patch.object(utils, "handle_send_markdown_type", failed_markdown),
            patch.object(utils, "handle_send2", fallback),
        ):
            asyncio.run(
                utils.handle_send(
                    SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ")),
                    event,
                    reply,
                    md_type="背包",
                    k1="饰品",
                    v1="饰品背包",
                )
            )
        assert phrase in fallback.call_args.args[2]
