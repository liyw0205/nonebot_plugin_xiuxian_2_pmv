from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.features.trade.xianshi_listing_repository import XianshiListingResult
from nonebot_plugin_xiuxian_2.features.trade.xianshi_plan_listing_repository import XianshiPlanListingResult
from tests.test_guishi_schedule import trade, utils


def _event() -> SimpleNamespace:
    return SimpleNamespace(message_id="listing-message", get_user_id=lambda: "seller")


def _bot() -> SimpleNamespace:
    return SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))


def _rejected() -> XianshiListingResult:
    return XianshiListingResult(
        "state_changed", "listing-op", "seller", 1001, "法器", "装备", 600000, 2,
        reason="xianshi_listing_item_write_rejected", subject="1000000000001",
    )


def test_listing_reason_survives_normal_command_and_adapters():
    captured = AsyncMock()
    event = _event()
    bot = _bot()
    args = SimpleNamespace(extract_plain_text=lambda: "法器 600000 2")
    user = {"user_id": "seller", "stone": 1000000}
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))), \
            patch.object(trade, "check_user", return_value=(True, user, "")), \
            patch.object(trade._sql_message(), "goods_num", return_value=5), \
            patch.object(trade.items, "get_data_by_item_name", return_value=(1001, {"type": "装备"})), \
            patch.object(trade.trade_application, "xianshi_list_items", return_value=_rejected()), \
            patch.object(trade, "handle_send", captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xian_shop_add_(bot, event, args))

    reply = captured.call_args.args[2]
    assert "仙肆挂单保存失败" in reply
    assert "1000000000001" in reply

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

    config.markdown_status = True
    md = AsyncMock()
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain), \
            patch.object(utils, "_send_markdown_or_keyboard", md):
        bot.adapter = SimpleNamespace(get_name=lambda: "QQ")
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    assert "1000000000001" in md.call_args.args[2]

    md = AsyncMock(side_effect=RuntimeError("isolated send failure"))
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain), \
            patch.object(utils, "_send_markdown_or_keyboard", md):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    assert plain.call_args.kwargs["message"] == reply


def test_fast_listing_reason_is_not_raised_as_unknown_status():
    captured = AsyncMock()
    event = _event()
    bot = _bot()
    args = SimpleNamespace(extract_plain_text=lambda: "法器 600000")
    user = {"user_id": "seller", "stone": 1000000}
    message_store = SimpleNamespace(goods_num=lambda *values, **kwargs: 5)
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))), \
            patch.object(trade, "check_user", return_value=(True, user, "")), \
            patch.object(trade, "_sql_message", return_value=message_store), \
            patch.object(trade.items, "get_data_by_item_name", return_value=(1001, {"type": "装备"})), \
            patch.object(trade, "get_trade_forbid_reason", return_value=""), \
            patch.object(trade.trade_application, "xianshi_list_items", return_value=_rejected()), \
            patch.object(trade, "handle_send", captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xianshi_fast_add_(bot, event, args))
    reply = captured.call_args.args[2]
    assert "仙肆挂单保存失败" in reply
    assert "1000000000001" in reply


def test_auto_listing_reason_is_not_raised_as_unknown_status():
    captured = AsyncMock()
    event = _event()
    bot = _bot()
    args = SimpleNamespace(extract_plain_text=lambda: "装备 法器")
    user = {"user_id": "seller", "stone": 1000000}
    message_store = SimpleNamespace(
        get_back_msg=lambda user_id: [{
            "goods_id": 1001,
            "goods_name": "法器",
            "goods_type": "法器",
            "goods_num": 2,
            "bind_num": 0,
        }]
    )
    rejected = XianshiPlanListingResult(
        "state_changed", "seller", 1,
        reason="xianshi_plan_listing_receipt_write_rejected", subject="auto-op",
    )
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))), \
            patch.object(trade, "check_user", return_value=(True, user, "")), \
            patch.object(trade, "_sql_message", return_value=message_store), \
            patch.object(trade.items, "get_data_by_item_id", return_value={"type": "装备", "level": "下品法器"}), \
            patch.object(trade, "get_trade_forbid_reason", return_value=""), \
            patch.object(trade, "get_xianshi_min_price", return_value=600000), \
            patch.object(trade.trade_application, "xianshi_list_plan", return_value=rejected), \
            patch.object(trade, "handle_send", captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xianshi_auto_add_(bot, event, args))
    reply = captured.call_args.args[2]
    assert "仙肆自动上架回执保存失败" in reply
    assert "auto-op" in reply
