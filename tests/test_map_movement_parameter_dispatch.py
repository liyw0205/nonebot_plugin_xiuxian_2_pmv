from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_movement_reason_survives_real_handler_and_qq_formatters():
    from nonebot.adapters.onebot.v11 import Message

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    event = make_event("前往", "900001", 973899)
    result = SimpleNamespace(
        ok=False,
        code="map_move_receipt_write_rejected",
        data={"reason": "map_move_receipt_write_rejected", "subject": "receipt"},
    )
    capture = AsyncMock()
    handler = module.map_go.handlers[0].call
    target = {"id": "n2", "name": "终点", "type": "普通"}
    map_data = {
        "meta": {"move_cost": {"cross_node": 5}},
        "凡界": {"heavens": {"一重天": [{"id": "n1", "name": "起点", "type": "普通"},
                                      {"id": "n2", "name": "终点", "type": "普通"}]}},
    }
    with (
        patch.object(module, "assign_bot", AsyncMock(return_value=(onebot, "900000"))),
        patch.object(module, "check_user", return_value=(True, {"user_id": "900001", "user_stamina": 10}, "")),
        patch.object(module, "_load_map_data", return_value=map_data),
        patch.object(module, "_get_player_map_status", return_value={
            "realm": "凡界", "heaven": "一重天", "node_id": "n1", "visited_nodes": ["n1"],
        }),
        patch.object(module, "_find_node_by_id", return_value={"id": "n1", "name": "起点", "type": "普通"}),
        patch.object(module, "_find_node_by_name", return_value=("凡界", "一重天", target)),
        patch.object(module.map_application, "move", return_value=result),
        patch.object(module, "handle_send", capture),
    ):
        asyncio.run(handler(bot=onebot, event=event, args=Message("终点")))
    message = capture.await_args.args[2]
    assert "地图移动：移动回执" in message
    assert reason_text("map_move_receipt_write_rejected") in message

    for bot, use_markdown, fallback in (
        (onebot, False, False),
        (qq, False, False),
        (qq, True, False),
        (qq, True, True),
    ):
        plain, markdown = AsyncMock(), AsyncMock()
        if fallback:
            markdown.side_effect = RuntimeError("isolated map movement Markdown failure")
        config = SimpleNamespace(
            markdown_status=use_markdown,
            merge_forward_send=4 if use_markdown else 1,
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
            asyncio.run(utils.send_msg_handler(bot, event, "前往", "900000", [message]))
        delivered = markdown if use_markdown and not fallback else plain
        delivered.assert_awaited_once()
        assert "地图移动：移动回执" in delivered.await_args.args[2]
        assert reason_text("map_move_receipt_write_rejected") in delivered.await_args.args[2]
