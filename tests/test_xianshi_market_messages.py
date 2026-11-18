"""仙肆只读查询的真实 handler 与 OneBot/QQ 投递合同。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
import pytest
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.features.trade.xianshi_market_repository import XianshiMarketQuote
from tests.test_guishi_schedule import trade, utils


def _event() -> SimpleNamespace:
    return SimpleNamespace(message_id="market-query-message", get_user_id=lambda: "buyer")


def _bot(adapter: str = "OneBot V11") -> SimpleNamespace:
    return SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: adapter))


def _run_query() -> tuple[AsyncMock, str, dict]:
    captured = AsyncMock()
    bot = _bot()
    event = _event()
    args = SimpleNamespace(extract_plain_text=lambda: "破境丹")
    quote = XianshiMarketQuote(
        "破境丹",
        trade_count=1,
        traded_quantity=2,
        min_unit_price=1000,
        max_unit_price=1200,
        avg_unit_price=1100,
        last_traded_at="2026-11-01 20:15:00",
        last_unit_price=1100,
        listing_count=1,
        listing_quantity=2,
        seller_count=1,
        min_listing_price=1200,
    )
    with (
        patch.object(trade, "assign_bot", new=AsyncMock(return_value=(bot, None))),
        patch.object(trade, "check_user", return_value=(True, {"user_id": "buyer"}, "")),
        patch.object(trade.xianshi_market_application, "quote", return_value=quote),
        patch.object(trade.xianshi_market_application, "names", return_value=()),
        patch.object(trade, "handle_send", captured),
    ):
        with pytest.raises(FinishedException):
            asyncio.run(trade.xianshi_market_(bot, event, args))
    reply = captured.call_args.args[2]
    return captured, reply, captured.call_args.kwargs


def test_market_query_reply_preserves_item_and_status_across_adapters() -> None:
    captured, reply, send_kwargs = _run_query()
    assert "【仙肆行情】破境丹" in reply
    assert "近7天成交：1 笔 / 2 件" in reply
    assert "最低在售单价：1200灵石" in reply

    event = _event()
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
    with (
        patch.object(utils, "XiuConfig", return_value=config),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", plain),
    ):
        asyncio.run(utils.handle_send(_bot(), event, reply, **send_kwargs))
    assert plain.call_args.kwargs["message"] == reply

    config.markdown_status = True
    markdown = AsyncMock()
    qq = _bot("QQ")
    with (
        patch.object(utils, "XiuConfig", return_value=config),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", plain),
        patch.object(utils, "_send_markdown_or_keyboard", markdown),
    ):
        asyncio.run(utils.handle_send(qq, event, reply, **send_kwargs))
    assert "破境丹" in markdown.call_args.args[2]

    markdown = AsyncMock(side_effect=RuntimeError("isolated market delivery failure"))
    with (
        patch.object(utils, "XiuConfig", return_value=config),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", plain),
        patch.object(utils, "_send_markdown_or_keyboard", markdown),
    ):
        asyncio.run(utils.handle_send(qq, event, reply, **send_kwargs))
    assert plain.call_args.kwargs["message"] == reply

