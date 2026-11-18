from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def test_dungeon_purchase_failure_reasons_survive_onebot_qq_and_fallback() -> None:
    event = SimpleNamespace(get_user_id=lambda: "buyer")

    async def send_contract(reply: str, onebot: bool, markdown: bool, send_fails: bool) -> None:
        bot = SimpleNamespace(
            adapter=SimpleNamespace(
                get_name=lambda: "OneBot V11" if onebot else "QQ"
            )
        )
        config = SimpleNamespace(
            markdown_status=markdown,
            markdown_id="",
            markdown_id2="",
            markdown_button_status=False,
            at_sender=False,
            message_optimization=False,
            img=False,
        )
        plain = AsyncMock()
        markdown_send = AsyncMock(
            side_effect=RuntimeError("isolated dungeon purchase delivery failure")
            if send_fails
            else None
        )
        with (
            patch.object(utils, "XiuConfig", return_value=config),
            patch.object(utils, "get_chat_scene", return_value="group"),
            patch.object(utils, "is_group_event", return_value=True),
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
            patch.object(utils, "_send_event_message", plain),
            patch.object(utils, "_send_markdown_or_keyboard", markdown_send),
        ):
            await utils.handle_send(
                bot,
                event,
                reply,
                md_type="副本",
                k1="兑换",
                v1="副本兑换",
                k2="探索",
                v2="探索副本",
                k3="状态",
                v3="我的副本状态",
            )
        if onebot or not markdown or send_fails:
            plain.assert_awaited_once()
            assert plain.call_args.kwargs["message"] == reply
        else:
            markdown_send.assert_awaited_once()

    cases = (
        ("dungeon_purchase_stone_write_rejected", "扣除灵石未能保存"),
        ("dungeon_purchase_inventory_write_rejected", "物品入库未能保存"),
        ("dungeon_purchase_receipt_write_rejected", "回执未能完整保存"),
    )
    for reason, phrase in cases:
        outcome = OperationOutcome.failed(
            "purchase-message",
            "dungeon.purchase",
            f"{phrase}（原因：{reason}；主体：actor）",
            code=reason,
            data={"status": "state_changed", "reason": reason, "subject": "actor"},
            audit_category="dungeon",
        )
        reply = result_failure("副本兑换", outcome)
        assert phrase in reply
        asyncio.run(send_contract(reply, True, True, False))
        asyncio.run(send_contract(reply, False, False, False))
        asyncio.run(send_contract(reply, False, True, False))
        asyncio.run(send_contract(reply, False, True, True))
