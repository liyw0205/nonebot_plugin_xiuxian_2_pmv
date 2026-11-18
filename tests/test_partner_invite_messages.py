from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import failure_reply, result_failure
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerInviteResult
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


def _bot(adapter: str) -> SimpleNamespace:
    return SimpleNamespace(self_id="bot", adapter=SimpleNamespace(get_name=lambda: adapter))


def _event() -> SimpleNamespace:
    return SimpleNamespace(message_id="invite-failure", get_user_id=lambda: "target")


def _config(markdown: bool) -> SimpleNamespace:
    return SimpleNamespace(
        markdown_status=markdown,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        at_sender=False,
        message_optimization=False,
        img=False,
    )


def _send(bot, event, message, plain, markdown=None) -> None:
    with (
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", plain),
        patch.object(utils, "_send_markdown_or_keyboard", markdown or AsyncMock()),
    ):
        asyncio.run(
            utils.handle_send(
                bot, event, message,
                md_type="buff", k1="双修", v1="双修",
                k2="次数", v2="我的双修次数", k3="修为", v3="我的修为",
            )
        )


def test_partner_invite_failure_reasons_survive_onebot_qq_and_fallback() -> None:
    event = _event()
    replies = (
        result_failure(
            "拒绝双修",
            PartnerInviteResult(
                "state_changed", reason="partner_invite_resolve_write_rejected", subject="target"
            ),
        ),
        failure_reply(
            "同意双修",
            DomainError(
                "partner_cultivation_invite_write_rejected",
                "invite acceptance was not persisted",
                {"reason": "partner_cultivation_invite_write_rejected", "subject": "target"},
            ),
        ),
    )
    expected = ("邀请处理状态保存失败", "邀请状态保存失败")

    for reply, marker in zip(replies, expected):
        assert marker in reply
        onebot_plain = AsyncMock()
        with patch.object(utils, "XiuConfig", return_value=_config(False)):
            _send(_bot("OneBot V11"), event, reply, onebot_plain)
        assert onebot_plain.call_args.kwargs["message"] == reply

        qq_plain, qq_markdown = AsyncMock(), AsyncMock()
        with patch.object(utils, "XiuConfig", return_value=_config(True)):
            _send(_bot("QQ"), event, reply, qq_plain, qq_markdown)
        assert marker in qq_markdown.call_args.args[2]

        fallback_plain, failed_markdown = AsyncMock(), AsyncMock(side_effect=RuntimeError("mock markdown delivery failure"))
        with patch.object(utils, "XiuConfig", return_value=_config(True)):
            _send(_bot("QQ"), event, reply, fallback_plain, failed_markdown)
        assert fallback_plain.call_args.kwargs["message"] == reply
