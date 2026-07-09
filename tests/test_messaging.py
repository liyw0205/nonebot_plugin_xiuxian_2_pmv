from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.adapter_message_records import (
    extract_result_message_id,
    extract_result_reference_id,
)
from nonebot_plugin_xiuxian_2.xiuxian.messaging import (
    ButtonSpec,
    KeyboardSpec,
    MessageDeliveryService,
    SendRequest,
    SendResult,
    escape_qq_markdown,
    keyboard_plain_text,
    render_markdown_template,
)
from nonebot_plugin_xiuxian_2.xiuxian.qq_compat import (
    QQCapabilities,
    QQCapabilityRegistry,
)


class MessageResultTests(unittest.TestCase):
    def test_extractors_support_nested_and_list_results(self) -> None:
        result = [
            {"ignored": True},
            {"data": {"msg_id": "message-1", "ext_info": {"ref_idx": "ref-1"}}},
        ]
        self.assertEqual(extract_result_message_id(result), "message-1")
        self.assertEqual(extract_result_reference_id(result), "ref-1")

    def test_reference_id_supports_message_scene_ext(self) -> None:
        result = SimpleNamespace(message_scene=SimpleNamespace(ext=["a=1&msg_idx=REF%3A123"]))
        self.assertEqual(extract_result_reference_id(result), "REF:123")

        nested = {"data": {"message_scene": {"ext": "msg_idx=REFIDX%2F456"}}}
        self.assertEqual(extract_result_reference_id(nested), "REFIDX/456")

    def test_send_result_preserves_raw_response(self) -> None:
        raw = {"id": "message-1", "msg_idx": "ref-1"}
        result = SendResult.from_raw(raw)
        self.assertEqual(result.message_id, "message-1")
        self.assertEqual(result.reference_id, "ref-1")
        self.assertIs(result.raw, raw)

    def test_keyboard_spec_and_markdown_template_are_validated(self) -> None:
        spec = KeyboardSpec(
            rows=((ButtonSpec("状态", "/状态"), ButtonSpec("官网", "https://example.invalid")),)
        )
        self.assertEqual(keyboard_plain_text(spec), "状态 | 官网")
        with self.assertRaises(ValueError):
            ButtonSpec("错误", "javascript:alert(1)").validate()

        self.assertEqual(escape_qq_markdown("[道友]*"), r"\[道友\]\*")
        self.assertEqual(
            render_markdown_template("欢迎 {name}", {"name": "[道友]"}),
            r"欢迎 \[道友\]",
        )

    def test_capabilities_can_be_overridden_per_bot(self) -> None:
        registry = QQCapabilityRegistry({"bot-1": {"keyboard": False}})
        self.assertFalse(registry.get("bot-1").keyboard)
        self.assertTrue(registry.get(SimpleNamespace(self_id="bot-2")).keyboard)
        self.assertFalse(QQCapabilities.from_mapping({"markdown": "false"}).markdown)
        self.assertEqual(QQCapabilities.from_mapping(None), QQCapabilities())


class DeliveryServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_delivery_reuses_existing_sender(self) -> None:
        service = MessageDeliveryService()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_group_message",
            new=AsyncMock(return_value={"msg_id": "message-1"}),
        ) as sender:
            result = await service.send(
                object(),
                SendRequest(
                    "group",
                    "group-1",
                    "hello",
                    reference_id="ref-1",
                    revoke_after=5,
                ),
            )

        self.assertEqual(result.message_id, "message-1")
        sender.assert_awaited_once_with(
            unittest.mock.ANY,
            group_id="group-1",
            message="hello",
            message_reference_id="ref-1",
            revoke_after=5,
        )


if __name__ == "__main__":
    unittest.main()
