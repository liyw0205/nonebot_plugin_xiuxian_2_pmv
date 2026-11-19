"""双修保护失败原因在真实 handler 与 OneBot/QQ 投递间保持可见。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
import pytest
from nonebot.exception import FinishedException

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerProtectionResult


def _event() -> SimpleNamespace:
    return SimpleNamespace(message_id="protection-message", get_user_id=lambda: "u1")


def _bot(adapter: str = "OneBot V11") -> SimpleNamespace:
    return SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: adapter))


def test_protection_rejection_reason_survives_command_and_adapters() -> None:
    bot = _bot()
    event = _event()
    captured = AsyncMock()
    rejected = PartnerProtectionResult(
        "state_changed",
        "on",
        "on",
        "partner_protection_receipt_write_rejected",
        "relation:cultivation-protection:protection-message:u1",
    )
    with (
        patch.object(partner, "assign_bot", new=AsyncMock(return_value=(bot, None))),
        patch.object(partner, "check_user", return_value=(True, {"user_id": "u1"}, "")),
        patch.object(partner, "load_player_user", return_value="on"),
        patch.object(partner, "_partner_protection_service", return_value=SimpleNamespace(set_status=lambda *args: rejected)),
        patch.object(partner, "handle_send", captured),
    ):
        with pytest.raises(FinishedException):
            asyncio.run(
                partner.two_exp_protect_(
                    bot,
                    event,
                    SimpleNamespace(extract_plain_text=lambda: "拒绝"),
                )
            )

    reply = captured.call_args.args[2]
    send_kwargs = captured.call_args.kwargs
    assert "双修保护操作回执保存失败" in reply
    assert "操作 relation:cultivation-protection:protection-message:u1" in reply

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
    assert "partner:cultivation" not in markdown.call_args.args[2]
    assert "操作 relation:cultivation-protection" in markdown.call_args.args[2]

    markdown = AsyncMock(side_effect=RuntimeError("isolated protection delivery failure"))
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
