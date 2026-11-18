from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.features.trade.xianshi_removal_repository import (
    XianshiClearResult,
    XianshiNameRemovalResult,
    XianshiRemovalResult,
)
from tests.test_guishi_schedule import trade, utils


def _event() -> SimpleNamespace:
    return SimpleNamespace(message_id="removal-message", get_user_id=lambda: "seller")


def _bot() -> SimpleNamespace:
    return SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))


def test_name_removal_reason_survives_command_and_onebot_qq_fallback():
    captured = AsyncMock()
    bot = _bot()
    event = _event()
    args = SimpleNamespace(extract_plain_text=lambda: "法器 2")
    rejected = XianshiNameRemovalResult(
        "state_changed", "seller", "法器", 2,
        reason="xianshi_name_removal_listing_write_rejected", subject="listing-1",
    )
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))), \
            patch.object(trade, "check_user", return_value=(True, {"user_id": "seller"}, "")), \
            patch.object(trade.trade_application, "xianshi_remove_by_name", return_value=rejected), \
            patch.object(trade, "handle_send", captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xian_shop_remove_(bot, event, args))

    reply = captured.call_args.args[2]
    assert "仙肆同名挂单更新失败" in reply
    assert "listing-1" in reply

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
    assert plain.call_args.kwargs["message"] == reply

    config.markdown_status = True
    markdown = AsyncMock()
    bot.adapter = SimpleNamespace(get_name=lambda: "QQ")
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain), \
            patch.object(utils, "_send_markdown_or_keyboard", markdown):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    assert "listing-1" in markdown.call_args.args[2]

    markdown = AsyncMock(side_effect=RuntimeError("isolated send failure"))
    with patch.object(utils, "XiuConfig", return_value=config), \
            patch.object(utils, "get_chat_scene", return_value="group"), \
            patch.object(utils, "is_group_event", return_value=True), \
            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
            patch.object(utils, "_send_event_message", plain), \
            patch.object(utils, "_send_markdown_or_keyboard", markdown):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    assert plain.call_args.kwargs["message"] == reply


def test_admin_and_clear_removal_rejections_are_explicit_replies():
    bot = _bot()
    event = _event()
    admin_captured = AsyncMock()
    admin_args = SimpleNamespace(extract_plain_text=lambda: "listing-1")
    admin_rejected = XianshiRemovalResult(
        "state_changed", "listing-1", "seller", 1001, "法器", "装备", 2,
        reason="xianshi_removal_receipt_write_rejected", subject="remove-1",
    )
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))), \
            patch.object(trade, "SUPERUSER", new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, "xianshi_remove_listing", return_value=admin_rejected), \
            patch.object(trade, "handle_send", admin_captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xian_shop_remove_by_admin_(bot, event, admin_args))
    assert "仙肆下架回执保存失败" in admin_captured.call_args.args[2]
    assert "remove-1" in admin_captured.call_args.args[2]

    clear_captured = AsyncMock()
    clear_rejected = XianshiClearResult(
        "state_changed", 2, 3,
        reason="xianshi_clear_listing_write_rejected", subject="xianshi_item",
    )
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))), \
            patch.object(trade, "SUPERUSER", new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, "xianshi_clear_all", return_value=clear_rejected), \
            patch.object(trade, "handle_send", clear_captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xian_shop_off_all_(bot, event))
    assert any("仙肆挂单批量删除失败" in call.args[2] for call in clear_captured.await_args_list)
    assert any("xianshi_item" in call.args[2] for call in clear_captured.await_args_list)
