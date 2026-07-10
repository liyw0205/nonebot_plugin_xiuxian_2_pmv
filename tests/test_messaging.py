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
from nonebot_plugin_xiuxian_2.xiuxian.adapter_message_sender import send_private_message
from nonebot_plugin_xiuxian_2.xiuxian.messaging import (
    ButtonSpec,
    DeliveryError,
    KeyboardSpec,
    MessageDeliveryService,
    SendRequest,
    SendResult,
    escape_qq_markdown,
    keyboard_plain_text,
    render_markdown_template,
)
from nonebot_plugin_xiuxian_2.xiuxian.infrastructure import RuntimeMetrics
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
    async def test_qq_sender_maps_source_message_to_adapter_reply_id(self) -> None:
        bot = SimpleNamespace(
            send_to_c2c=AsyncMock(return_value={"id": "message-0"})
        )
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.adapter_message_sender.is_qq_bot",
                return_value=True,
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.adapter_message_sender._record_send"
            ) as record,
        ):
            await send_private_message(
                bot,
                user_id="user-0",
                message="reply",
                source_message_id="source-0",
                msg_seq=7,
            )

        bot.send_to_c2c.assert_awaited_once_with(
            openid="user-0",
            message="reply",
            msg_seq=7,
            msg_id="source-0",
        )
        record.assert_called_once_with(
            bot,
            scene="private",
            message="reply",
            result={"id": "message-0"},
            user_id="user-0",
            source_message_id="source-0",
            revoke_time=0,
        )

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

    async def test_qq_delivery_retries_msg_seq_conflict(self) -> None:
        metrics = RuntimeMetrics()
        service = MessageDeliveryService(max_msg_seq_retries=2, metrics=metrics)

        class MsgSeqConflict(RuntimeError):
            code = 40054005

        sender = AsyncMock(
            side_effect=[MsgSeqConflict("消息被去重"), {"id": "message-retried"}]
        )
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
                return_value=True,
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_group_message",
                new=sender,
            ),
            patch.object(service._sequences, "next", side_effect=[101, 102]),
        ):
            result = await service.send(
                SimpleNamespace(self_id="bot-1"),
                SendRequest("group", "group-1", "hello"),
            )

        self.assertEqual(result.message_id, "message-retried")
        self.assertEqual(sender.await_count, 2)
        self.assertEqual(sender.await_args_list[0].kwargs["msg_seq"], 101)
        self.assertEqual(sender.await_args_list[1].kwargs["msg_seq"], 102)
        self.assertEqual(metrics.get("delivery.retry.msg_seq_conflict"), 1)

    async def test_explicit_msg_seq_is_preserved_without_retry_generation(self) -> None:
        service = MessageDeliveryService()
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
                return_value=True,
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_private_message",
                new=AsyncMock(return_value={"id": "message-explicit"}),
            ) as sender,
            patch.object(service._sequences, "next") as next_sequence,
        ):
            await service.send(
                SimpleNamespace(self_id="bot-1"),
                SendRequest("private", "user-1", "hello"),
                msg_seq=77,
            )

        next_sequence.assert_not_called()
        sender.assert_awaited_once_with(
            unittest.mock.ANY,
            user_id="user-1",
            message="hello",
            msg_seq=77,
        )

    async def test_audit_exception_returns_pending_result(self) -> None:
        metrics = RuntimeMetrics()
        service = MessageDeliveryService(metrics=metrics)

        class AuditException(RuntimeError):
            audit_id = "audit-1"

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_group_message",
            new=AsyncMock(side_effect=AuditException()),
        ):
            result = await service.send(
                object(),
                SendRequest("group", "group-1", "hello"),
            )

        self.assertEqual(result.status, "pending_audit")
        self.assertEqual(result.audit_id, "audit-1")
        self.assertIsNone(result.message_id)
        self.assertEqual(metrics.get("delivery.audit.pending"), 1)

    async def test_audit_timeout_is_reported(self) -> None:
        metrics = RuntimeMetrics()
        service = MessageDeliveryService(metrics=metrics)

        class AuditException(RuntimeError):
            audit_id = "audit-timeout"

            async def get_audit_result(self, timeout):
                raise TimeoutError(f"audit timeout after {timeout}")

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_group_message",
            new=AsyncMock(side_effect=AuditException()),
        ):
            with self.assertRaises(DeliveryError):
                await service.send(
                    object(),
                    SendRequest("group", "group-1", "hello", audit_timeout=0.1),
                )

        self.assertEqual(metrics.get("delivery.audit.timeout"), 1)

    async def test_rate_limit_error_is_classified_as_retryable(self) -> None:
        service = MessageDeliveryService()

        class RateLimitException(RuntimeError):
            status_code = 429

        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_group_message",
            new=AsyncMock(side_effect=RateLimitException("rate limit")),
        ):
            with self.assertRaises(DeliveryError) as raised:
                await service.send(
                    object(),
                    SendRequest("group", "group-1", "hello"),
                )

        self.assertEqual(raised.exception.kind, "rate_limited")
        self.assertTrue(raised.exception.retryable)

    async def test_enhanced_reply_falls_back_when_markdown_is_disabled(self) -> None:
        registry = QQCapabilityRegistry({"bot-1": {"markdown": False}})
        service = MessageDeliveryService(capabilities=registry)
        bot = SimpleNamespace(self_id="bot-1")
        event = object()
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
                return_value=True,
            ),
            patch.object(
                service,
                "reply",
                new=AsyncMock(return_value=SendResult("message-6", None, {})),
            ) as reply,
        ):
            await service.reply_enhanced(
                bot,
                event,
                markdown="**状态**",
                fallback_text="状态",
                keyboard_rows=[[('查看', '/状态')]],
            )

        reply.assert_awaited_once_with(
            bot,
            event,
            "状态",
            include_reference=False,
        )

    async def test_enhanced_reply_drops_keyboard_when_capability_is_disabled(self) -> None:
        registry = QQCapabilityRegistry({"bot-1": {"keyboard": False}})
        service = MessageDeliveryService(capabilities=registry)
        bot = SimpleNamespace(self_id="bot-1")
        event = object()
        markdown_message = object()
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.is_qq_bot",
                return_value=True,
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.adapter_compat.MessageSegment.markdown",
                return_value=markdown_message,
            ) as markdown,
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.adapter_compat.MessageSegment.markdown_keyboard"
            ) as keyboard,
            patch.object(
                service,
                "reply",
                new=AsyncMock(return_value=SendResult("message-7", None, {})),
            ) as reply,
        ):
            await service.reply_enhanced(
                bot,
                event,
                markdown="**状态**",
                fallback_text="状态",
                keyboard_rows=[[('查看', '/状态')]],
                button_id="keyboard-template",
            )

        keyboard.assert_not_called()
        markdown.assert_called_once_with(bot, "**状态**", "")
        reply.assert_awaited_once_with(
            bot,
            event,
            markdown_message,
            include_reference=False,
        )

    async def test_reply_source_is_forwarded_for_recording(self) -> None:
        service = MessageDeliveryService()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.send_private_message",
            new=AsyncMock(return_value={"id": "message-2", "ref_idx": "ref-2"}),
        ) as sender:
            result = await service.send(
                object(),
                SendRequest(
                    "private",
                    "user-1",
                    "reply",
                    source_message_id="source-1",
                ),
            )

        self.assertEqual(result.reference_id, "ref-2")
        sender.assert_awaited_once_with(
            unittest.mock.ANY,
            user_id="user-1",
            message="reply",
            source_message_id="source-1",
        )

    async def test_channel_delivery_uses_adapter_and_records_result(self) -> None:
        service = MessageDeliveryService()
        bot = SimpleNamespace(
            send_to_channel=AsyncMock(return_value={"message_id": "message-3"})
        )
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.record_send_message"
        ) as record:
            result = await service.send(
                bot,
                SendRequest(
                    "channel_group",
                    "channel-1",
                    "hello",
                    source_message_id="source-2",
                ),
                msg_seq=123,
            )

        self.assertEqual(result.message_id, "message-3")
        bot.send_to_channel.assert_awaited_once_with(
            channel_id="channel-1",
            message="hello",
            msg_id="source-2",
        )
        record.assert_called_once_with(
            bot,
            scene="channel_group",
            message="hello",
            message_id="message-3",
            source_message_id="source-2",
            group_id="channel-1",
            raw_result={"message_id": "message-3"},
        )

    async def test_reply_routes_event_context_through_delivery_request(self) -> None:
        service = MessageDeliveryService()
        event = SimpleNamespace(message_id="source-3")
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.get_chat_scene",
                return_value="group",
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.get_group_id",
                return_value="group-2",
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.get_message_reference_id",
                return_value="ref-3",
            ),
            patch.object(
                service,
                "send",
                new=AsyncMock(return_value=SendResult("message-4", None, {})),
            ) as send,
        ):
            result = await service.reply(object(), event, "status")

        self.assertEqual(result.message_id, "message-4")
        request = send.await_args.args[1]
        self.assertEqual(request.scene, "group")
        self.assertEqual(request.target_id, "group-2")
        self.assertEqual(request.reference_id, "ref-3")
        self.assertEqual(request.source_message_id, "source-3")

    async def test_reply_can_disable_reference_without_losing_source(self) -> None:
        service = MessageDeliveryService()
        event = SimpleNamespace(id="source-4")
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.get_chat_scene",
                return_value="private",
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.get_user_id",
                return_value="user-2",
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery.get_message_reference_id"
            ) as get_reference,
            patch.object(
                service,
                "send",
                new=AsyncMock(return_value=SendResult("message-5", None, {})),
            ) as send,
        ):
            await service.reply(
                object(),
                event,
                "help",
                include_reference=False,
            )

        get_reference.assert_not_called()
        request = send.await_args.args[1]
        self.assertIsNone(request.reference_id)
        self.assertEqual(request.source_message_id, "source-4")


if __name__ == "__main__":
    unittest.main()
