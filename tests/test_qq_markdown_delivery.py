"""签到的 QQ 投递契约：显式 markdown intent、群/私聊、OneBot 纯文本回退。

部署日志里签到走的是 `NoneBotMessageGateway -> delivery_service.reply(content)`，
`reply.content` 里的 Markdown 直接当纯文本发出去（`**`、`---` 原样可见，
日历的空格列也散开）。这里锁定修复后的契约：

* `intent="markdown"` 的字符串计划 → `reply_enhanced`（原生 Markdown）；
* 非 QQ Bot / 无 markdown 能力 / 发送异常 → 同一份文案的纯文本回退；
* 默认 `plain` 与媒体段继续走 `reply`，行为不变。
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.adapters.nonebot.gateway import (
    NoneBotMessageGateway,
    plain_text_fallback,
)
from nonebot_plugin_xiuxian_2.core.result import INTENT_MARKDOWN, ReplyPlan
from nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery import MessageDeliveryService
from nonebot_plugin_xiuxian_2.xiuxian.qq_compat.capabilities import QQCapabilityRegistry
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import (
    _extract_markdown_keyboard_buttons,
)

SIGN_TEXT = (
    "**修仙签到**\n"
    "\n"
    "---\n"
    "\n"
    "✅ 签到成功，获取100块灵石!\n"
    "**签到日历 2026-10**\n"
    "\n"
    "| 一 | 二 | 三 | 四 | 五 | 六 | 日 |\n"
    "| --- | --- | --- | --- | --- | --- | --- |\n"
    "| 12 | 13✓ | 14✓ | 15· | 16 | 17 | 18 |\n"
    "\n"
    "✓ 已签｜· 今日｜— 非本月｜本月已签 2 天｜连续 2 天"
)


class _Context:
    def __init__(self, bot, event):
        self.bot = bot
        self.raw_event = event


class _Event:
    def __init__(self, scene: str, self_id: str = "10000"):
        self.scene = scene
        self.message_id = f"msg-{scene}"
        self.self_id = self_id


class _RecordingDelivery:
    """Records which delivery entry point the gateway picked."""

    def __init__(self) -> None:
        self.plain: list[tuple[object, object, object]] = []
        self.enhanced: list[dict[str, object]] = []

    async def reply(self, bot, event, message, **kwargs):
        self.plain.append((bot, event, message))
        return "plain-id"

    async def reply_enhanced(
        self,
        bot,
        event,
        *,
        markdown,
        fallback_text,
        keyboard_rows=None,
        button_id="",
        allow_plain_fallback=True,
        **kwargs,
    ):
        self.enhanced.append(
            {
                "markdown": markdown,
                "fallback_text": fallback_text,
                "allow_plain_fallback": allow_plain_fallback,
                "scene": getattr(event, "scene", ""),
            }
        )
        return "md-id"


class _RecordingMessageDelivery(MessageDeliveryService):
    """Real delivery policy, captured before it reaches the Bot API."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.sent: list[object] = []

    async def reply(self, bot, event, message, **kwargs):
        self.sent.append(message)
        return "ok"


class GatewayIntentTests(unittest.IsolatedAsyncioTestCase):
    async def test_markdown_intent_uses_markdown_capable_delivery(self) -> None:
        delivery = _RecordingDelivery()
        gateway = NoneBotMessageGateway(delivery)

        await gateway.send(_Context(object(), _Event("group")), ReplyPlan(SIGN_TEXT, intent=INTENT_MARKDOWN))

        self.assertEqual([], delivery.plain)
        self.assertEqual(1, len(delivery.enhanced))
        call = delivery.enhanced[0]
        self.assertEqual(SIGN_TEXT, call["markdown"])
        self.assertTrue(call["allow_plain_fallback"])
        fallback = str(call["fallback_text"])
        self.assertIn("签到成功", fallback)
        self.assertNotIn("**", fallback)
        self.assertNotIn("\n---\n", fallback)
        self.assertIn("\n----------\n", fallback)
        self.assertIn("13✓", fallback)

    async def test_markdown_intent_reaches_both_group_and_private(self) -> None:
        for scene in ("group", "private"):
            with self.subTest(scene=scene):
                delivery = _RecordingDelivery()
                gateway = NoneBotMessageGateway(delivery)

                await gateway.send(
                    _Context(object(), _Event(scene)),
                    ReplyPlan(SIGN_TEXT, intent=INTENT_MARKDOWN),
                )

                self.assertEqual([], delivery.plain)
                self.assertEqual(scene, delivery.enhanced[0]["scene"])

    async def test_plain_intent_keeps_raw_content(self) -> None:
        delivery = _RecordingDelivery()
        gateway = NoneBotMessageGateway(delivery)

        await gateway.send(_Context(object(), _Event("group")), ReplyPlan(SIGN_TEXT))

        self.assertEqual([], delivery.enhanced)
        self.assertEqual(SIGN_TEXT, delivery.plain[0][2])

    async def test_media_or_segment_content_is_never_rewritten(self) -> None:
        delivery = _RecordingDelivery()
        gateway = NoneBotMessageGateway(delivery)
        segment = [{"type": "image", "data": {"file": "a.png"}}]

        await gateway.send(
            _Context(object(), _Event("group")),
            ReplyPlan(segment, intent=INTENT_MARKDOWN),
        )

        self.assertEqual([], delivery.enhanced)
        self.assertIs(segment, delivery.plain[0][2])

    async def test_blank_markdown_content_falls_back_to_plain_reply(self) -> None:
        delivery = _RecordingDelivery()
        gateway = NoneBotMessageGateway(delivery)

        await gateway.send(_Context(object(), _Event("group")), ReplyPlan("   ", intent=INTENT_MARKDOWN))

        self.assertEqual([], delivery.enhanced)
        self.assertEqual("   ", delivery.plain[0][2])


class DeliveryFallbackTests(unittest.IsolatedAsyncioTestCase):
    """Real `reply_enhanced` policy: capability gate + plain fallback."""

    async def test_onebot_bot_receives_plain_text_for_group_and_private(self) -> None:
        for scene in ("group", "private"):
            with self.subTest(scene=scene):
                delivery = _RecordingMessageDelivery()
                gateway = NoneBotMessageGateway(delivery)
                with patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
                    return_value=False,
                ):
                    await gateway.send(
                        _Context(object(), _Event(scene)),
                        ReplyPlan(SIGN_TEXT, intent=INTENT_MARKDOWN),
                    )

                self.assertEqual(1, len(delivery.sent))
                sent = str(delivery.sent[0])
                self.assertIn("签到成功", sent)
                self.assertNotIn("**", sent)
                self.assertNotIn("\n---\n", sent)
                self.assertIn("\n----------\n", sent)

    async def test_qq_bot_with_markdown_capability_sends_markdown_segment(self) -> None:
        class _FakeSegment:
            @staticmethod
            def markdown(bot, msg, button_id=""):
                return ("markdown", msg)

            @staticmethod
            def markdown_keyboard(bot, msg, rows):
                return ("markdown_keyboard", msg)

        delivery = _RecordingMessageDelivery(capabilities=QQCapabilityRegistry())
        gateway = NoneBotMessageGateway(delivery)

        class _Bot:
            self_id = "10000"

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
            return_value=True,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.adapter_compat.MessageSegment",
            _FakeSegment,
        ):
            await gateway.send(
                _Context(_Bot(), _Event("group")),
                ReplyPlan(SIGN_TEXT, intent=INTENT_MARKDOWN),
            )

        self.assertEqual(1, len(delivery.sent))
        self.assertEqual(("markdown", SIGN_TEXT), delivery.sent[0])
        # 最终 Markdown 消息段里必须是真表格：表头 + 分隔行 + 生成的数据行。
        segment = delivery.sent[0][1]
        self.assertIn("| 一 | 二 | 三 | 四 | 五 | 六 | 日 |", segment)
        self.assertIn("| --- | --- | --- | --- | --- | --- | --- |", segment)
        self.assertIn("| 12 | 13✓ | 14✓ | 15· | 16 | 17 | 18 |", segment)
        self.assertIn("本月已签 2 天", segment)
        self.assertIn("连续 2 天", segment)

    async def test_qq_bot_without_markdown_capability_falls_back(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.qq_compat.capabilities import QQCapabilities

        delivery = _RecordingMessageDelivery(
            capabilities=QQCapabilityRegistry(default=QQCapabilities(markdown=False))
        )
        gateway = NoneBotMessageGateway(delivery)

        class _Bot:
            self_id = "10000"

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
            return_value=True,
        ):
            await gateway.send(
                _Context(_Bot(), _Event("group")),
                ReplyPlan(SIGN_TEXT, intent=INTENT_MARKDOWN),
            )

        self.assertEqual(1, len(delivery.sent))
        self.assertNotIn("**", str(delivery.sent[0]))
        self.assertIn("签到成功", str(delivery.sent[0]))

    async def test_markdown_send_failure_falls_back_to_plain_text(self) -> None:
        delivery = _RecordingMessageDelivery()
        gateway = NoneBotMessageGateway(delivery)

        class _BrokenSegment:
            @staticmethod
            def markdown(bot, msg, button_id=""):
                raise RuntimeError("markdown unavailable")

        class _Bot:
            self_id = "10000"

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
            return_value=True,
        ), patch(
            "nonebot_plugin_xiuxian_2.xiuxian.adapter_compat.MessageSegment",
            _BrokenSegment,
        ):
            await gateway.send(
                _Context(_Bot(), _Event("private")),
                ReplyPlan(SIGN_TEXT, intent=INTENT_MARKDOWN),
            )

        self.assertEqual(1, len(delivery.sent))
        self.assertNotIn("**", str(delivery.sent[0]))
        self.assertIn("本月已签 2 天", str(delivery.sent[0]))


class CapabilityConfigTests(unittest.TestCase):
    """The delivery path is capability-gated by existing config only."""

    def test_unset_capability_config_enables_markdown(self) -> None:
        """部署 `.env` 未配置 `xiuxian_qq_capabilities` 时默认开启 Markdown。"""

        from types import SimpleNamespace

        registry = QQCapabilityRegistry.from_config(
            SimpleNamespace(xiuxian_qq_capabilities=None)
        )

        class _Bot:
            self_id = "10000"

        self.assertTrue(registry.get(_Bot()).markdown)

    def test_per_bot_config_can_turn_markdown_off(self) -> None:
        import json
        from types import SimpleNamespace

        registry = QQCapabilityRegistry.from_config(
            SimpleNamespace(
                xiuxian_qq_capabilities=json.dumps(
                    {"bots": {"10000": {"markdown": False}}}
                )
            )
        )

        class _Bot:
            self_id = "10000"

        self.assertFalse(registry.get(_Bot()).markdown)


class PlainFallbackTests(unittest.TestCase):
    def test_fallback_strips_markdown_and_keeps_the_calendar(self) -> None:
        text = plain_text_fallback(SIGN_TEXT)

        self.assertNotIn("**", text)
        self.assertNotIn("\n---\n\n", text)
        self.assertIn("修仙签到", text)
        self.assertIn("| 12 | 13✓ | 14✓ | 15· | 16 | 17 | 18 |", text)
        self.assertIn("本月已签 2 天｜连续 2 天", text)


class KeyboardPresentationTests(unittest.TestCase):
    def test_inline_commands_move_to_keyboard_without_duplicate_blue_links(self) -> None:
        markdown = (
            "详情\r\r"
            "[查看](mqqapi://aio/inlinecmd?command=%E6%88%91%E7%9A%84%E8%83%8C%E5%8C%85%202&enter=false&reply=false) | "
            "[下一页](mqqapi://aio/inlinecmd?command=%E4%B8%8B%E4%B8%80%E9%A1%B5&enter=false&reply=false)"
        )
        content, rows = _extract_markdown_keyboard_buttons(markdown)

        self.assertEqual("详情", content)
        self.assertEqual(
            [[("查看", "我的背包 2"), ("下一页", "下一页")]],
            rows,
        )
        self.assertNotIn("mqqapi://aio/inlinecmd", content)


if __name__ == "__main__":
    unittest.main()
