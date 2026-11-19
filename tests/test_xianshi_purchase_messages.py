from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from nonebot.exception import FinishedException

from tests.test_guishi_schedule import trade, utils
from nonebot_plugin_xiuxian_2.core.result import OperationOutcome


def _run_purchase_failure():
    captured = AsyncMock()
    event = SimpleNamespace(message_id="purchase-message", get_user_id=lambda: "buyer")
    args = SimpleNamespace(extract_plain_text=lambda: "listing-1 1")
    rejected = OperationOutcome.rejected(
        "trade-op",
        "trade.purchase",
        "purchase rejected",
        code="state_changed",
        data={
            "status": "state_changed",
            "reason": "xianshi_purchase_inventory_write_rejected",
            "subject": "buyer:1001",
            "listing_id": "listing-1",
            "buyer_id": "buyer",
            "seller_id": "seller",
            "goods_id": 1001,
            "name": "法器",
            "goods_type": "装备",
            "quantity": 1,
            "total_cost": 200,
            "stamina_charged": 0,
        },
    )
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, "check_user", return_value=(True, {"user_id": "buyer"}, "")), \
            patch.object(trade.trade_application, "xianshi_get_items", return_value=[
                {"id": "listing-1", "name": "法器", "price": 200}
            ]), \
            patch.object(trade.trade_application, "purchase", return_value=rejected), \
            patch.object(trade, "handle_send", captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xian_buy_(object(), event, args))
    return captured, event


def test_purchase_reason_and_subject_survive_command_and_adapters():
    captured, event = _run_purchase_failure()
    reply = captured.call_args.args[2]
    assert "仙肆购买物品入背包失败" in reply
    assert "buyer:1001" in reply

    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    config = SimpleNamespace(
        markdown_status=False,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        at_sender=False,
        message_optimization=False,
        img=False,
    )
    plain = AsyncMock()
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    plain.assert_awaited_once()
    assert plain.call_args.kwargs["message"] == reply

    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    config.markdown_status = True
    md = AsyncMock()
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain), \
            patch.object(utils, "_send_markdown_or_keyboard", md):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    assert "buyer:1001" in md.call_args.args[2]

    md = AsyncMock(side_effect=RuntimeError("isolated send failure"))
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain), \
            patch.object(utils, "_send_markdown_or_keyboard", md):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    assert plain.call_args.kwargs["message"] == reply
