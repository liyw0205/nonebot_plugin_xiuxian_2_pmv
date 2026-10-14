"""Activity-center delivery contract across QQ and OneBot adapters.

The same read-only center card must render correctly through the shared
send entry on every adapter: OneBot V11 always receives the plain-text
degradation, QQ without markdown the same, and QQ with markdown keeps
the native blue-link channel. Business flow is verified once; QQ is only
checked for formatting/degradation differences, never real sends.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)

ROOT = Path(__file__).resolve().parents[1]

CONFIG = {
    "template_key": "spring",
    "enabled": True,
    "name": "春日庆典",
    "description": "配置说明",
    "start_time": "2026-10-01",
    "end_time": "2026-10-20",
    "daily_tasks": [],
    "weekly_tasks": [],
    "gameplay_activities": [],
    "extensions": {"activity_pass": {"enabled": False}},
}


def _center_text() -> str:
    app = ActivityReadModelApplication(
        "/tmp/activity-center-delivery-missing.db",
        config_loader=lambda: CONFIG,
        clock=SimpleNamespace(now=lambda: datetime(2026, 10, 15, 12)),
    )
    return app.activity_center_text()


def _event() -> SimpleNamespace:
    return SimpleNamespace(get_user_id=lambda: "u1", message_id="m1")


def _bot(*, onebot: bool) -> SimpleNamespace:
    name = "OneBot V11" if onebot else "QQ"
    return SimpleNamespace(
        self_id="bot-1",
        adapter=SimpleNamespace(get_name=lambda: name),
    )


def _config_stub(*, markdown: bool) -> type:
    class _XiuConfig:
        def __init__(self, *args, **kwargs) -> None:
            self.markdown_status = markdown
            self.img = False
            self.at_sender = False
            self.message_optimization = False
            self.markdown_button_status = False
            self.markdown_id2 = False

    return _XiuConfig


def _send_through_entry(bot, text: str, *, markdown: bool):
    """Run the shared handle_send decision seam, capturing the terminal send."""
    plain_sent: list[str] = []
    md_sent: list[str] = []

    async def capture_plain(*args, **kwargs):
        plain_sent.append(str(kwargs.get("message") or (args[-1] if args else "")))

    async def capture_markdown(*args, **kwargs):
        md_sent.append(str(kwargs.get("md_text") or (args[2] if len(args) > 2 else "")))

    with (
        patch.object(utils, "XiuConfig", _config_stub(markdown=markdown)),
        patch.object(utils, "get_chat_scene", lambda event: "group"),
        patch.object(utils, "is_group_event", lambda event: True),
        patch.object(utils, "get_real_id_async", AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", capture_plain),
        patch.object(utils, "_send_markdown_or_keyboard", capture_markdown),
    ):
        asyncio.run(
            utils.handle_send(
                bot,
                _event(),
                text,
                native_markdown=True,
                fallback_msg=strip_md_command_links(text),
            )
        )
    return plain_sent, md_sent


def test_onebot_receives_plain_degradation_once() -> None:
    text = _center_text()
    assert "mqqapi://aio/inlinecmd" in text and "**" in text
    plain_sent, md_sent = _send_through_entry(_bot(onebot=True), text, markdown=True)
    assert md_sent == []
    assert plain_sent == [strip_md_command_links(text)]
    plain = plain_sent[0]
    assert "**" not in plain and "mqqapi" not in plain
    # the degraded text keeps readable entry names for the player
    assert "活动签到" in plain and "活动中心" in plain


def test_qq_without_markdown_uses_the_same_plain_contract() -> None:
    text = _center_text()
    plain_sent, md_sent = _send_through_entry(_bot(onebot=False), text, markdown=False)
    assert md_sent == []
    assert plain_sent == [strip_md_command_links(text)]


def test_qq_with_markdown_keeps_native_blue_links() -> None:
    text = _center_text()
    plain_sent, md_sent = _send_through_entry(_bot(onebot=False), text, markdown=True)
    assert plain_sent == []
    # 原生通道按既有约定把 \n 归一为 \r 换行（handle_send_native_markdown），
    # 但绝不剥 []()：蓝字命令链接原样保留。
    assert md_sent == [text.replace("\n", "\r")]
    assert "mqqapi://aio/inlinecmd" in md_sent[0]
    assert "](mqqapi" in md_sent[0] and "\n" not in md_sent[0]


def test_plain_degradation_is_idempotent() -> None:
    once = strip_md_command_links(_center_text())
    assert strip_md_command_links(once) == once


def test_enriched_card_still_degrades_cleanly_on_onebot() -> None:
    """玩法入口加入链接行后，纯文本降级仍不得泄漏 markdown 或蓝字 URL。"""
    config = dict(
        CONFIG,
        gameplay_activities=[
            {
                "key": "spring_collect",
                "type": "collect_words",
                "enabled": True,
                "name": "春日集字",
                "start_time": "0",
                "end_time": "无限",
            },
        ],
    )
    app = ActivityReadModelApplication(
        "/tmp/activity-center-delivery-missing.db",
        config_loader=lambda: config,
        clock=SimpleNamespace(now=lambda: datetime(2026, 10, 15, 12)),
    )
    text = app.activity_center_text()
    assert "mqqapi://aio/inlinecmd" in text and "集字背包" in text

    plain_sent, md_sent = _send_through_entry(_bot(onebot=True), text, markdown=True)
    assert md_sent == []
    plain = plain_sent[0]
    assert "**" not in plain and "mqqapi" not in plain
    assert "集字背包" in plain and "活动签到" in plain

    plain_sent, md_sent = _send_through_entry(_bot(onebot=False), text, markdown=True)
    assert plain_sent == []
    assert "](mqqapi" in md_sent[0] and "\n" not in md_sent[0]


def test_center_handler_wires_shared_entry_with_fallback() -> None:
    source = (
        ROOT
        / "nonebot_plugin_xiuxian_2"
        / "xiuxian"
        / "xiuxian_activity"
        / "__init__.py"
    ).read_text(encoding="utf-8")
    start = source.index("@activity_center_cmd.handle")
    block = source[start : source.index("@", start + 1)]
    assert "native_markdown=True" in block
    assert "fallback_msg=strip_md_command_links(text)" in block
