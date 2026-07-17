from __future__ import annotations

import asyncio
from typing import Any

from ..adapter_compat import (
    get_chat_scene,
    get_group_id,
    get_message_reference_id,
    get_user_id,
)
from ..adapter_message_actions import delete_message_compat
from ..adapter_message_records import record_send_message
from ..adapter_message_sender import is_qq_bot, send_group_message, send_private_message
from ..qq_compat import QQCapabilityRegistry
from ..infrastructure import RuntimeMetrics, runtime_metrics, settings
from .models import SendRequest, SendResult
from .media import MediaInput, MediaResolver, media_resolver
from .reliability import (
    DeliveryError,
    MessageSequenceStrategy,
    classify_delivery_error,
    is_msg_seq_conflict,
)


class MessageDeliveryService:
    """收敛主动发送与回复入口，底层继续复用现有 Adapter 兼容实现。"""

    def __init__(
        self,
        *,
        max_msg_seq_retries: int = 3,
        capabilities: QQCapabilityRegistry | None = None,
        metrics: RuntimeMetrics | None = None,
        media: MediaResolver | None = None,
    ) -> None:
        self._sequences = MessageSequenceStrategy()
        self._max_msg_seq_retries = max(0, int(max_msg_seq_retries))
        self._capabilities = capabilities or QQCapabilityRegistry()
        self._metrics = metrics or runtime_metrics
        self._media = media or media_resolver

    async def _send_with_policy(
        self,
        bot: Any,
        request: SendRequest,
        kwargs: dict[str, Any],
    ) -> Any:
        generated_sequence = (
            is_qq_bot(bot)
            and request.scene in {"group", "private"}
            and "msg_seq" not in kwargs
        )
        attempts = self._max_msg_seq_retries + 1 if generated_sequence else 1
        for attempt in range(attempts):
            call_kwargs = dict(kwargs)
            if generated_sequence:
                call_kwargs["msg_seq"] = self._sequences.next(
                    bot, request.scene, request.target_id
                )
            try:
                if request.scene == "group":
                    return await send_group_message(
                        bot,
                        group_id=request.target_id,
                        message=request.message,
                        **call_kwargs,
                    )
                return await send_private_message(
                    bot,
                    user_id=request.target_id,
                    message=request.message,
                    **call_kwargs,
                )
            except Exception as exc:
                if generated_sequence and is_msg_seq_conflict(exc) and attempt + 1 < attempts:
                    self._metrics.increment("delivery.retry.msg_seq_conflict")
                    await asyncio.sleep(0)
                    continue
                raise
        raise RuntimeError("消息投递重试流程异常结束")

    async def _audit_result(self, exc: BaseException, timeout: float) -> SendResult:
        audit_id = str(getattr(exc, "audit_id", "") or "") or None
        if timeout <= 0 or not callable(getattr(exc, "get_audit_result", None)):
            self._metrics.increment("delivery.audit.pending")
            return SendResult(None, None, exc, status="pending_audit", audit_id=audit_id)
        try:
            raw = await exc.get_audit_result(timeout=timeout)  # type: ignore[attr-defined]
        except Exception as audit_exc:
            if isinstance(audit_exc, (asyncio.TimeoutError, TimeoutError)):
                self._metrics.increment("delivery.audit.timeout")
            kind, retryable = classify_delivery_error(audit_exc)
            raise DeliveryError(kind, retryable, audit_exc) from audit_exc
        event_type = str(
            getattr(raw, "__type__", "")
            or getattr(raw, "event_type", "")
            or ""
        ).upper()
        if "MESSAGE_AUDIT_PASS" not in event_type:
            self._metrics.increment("delivery.audit.rejected")
            rejected = RuntimeError(f"消息审核未通过: {event_type or 'unknown'}")
            raise DeliveryError("audit_rejected", False, rejected)
        return SendResult.from_raw(raw)

    async def send(self, bot: Any, request: SendRequest, **kwargs: Any) -> SendResult:
        if request.reference_id:
            kwargs.setdefault("message_reference_id", request.reference_id)
        if request.source_message_id:
            kwargs.setdefault("source_message_id", request.source_message_id)
        if request.revoke_after:
            kwargs.setdefault("revoke_after", request.revoke_after)

        try:
            if request.scene in {"group", "private"}:
                raw = await self._send_with_policy(bot, request, kwargs)
            elif request.scene == "channel_group":
                return await self._send_channel(bot, request, kwargs, private=False)
            elif request.scene == "channel_private":
                return await self._send_channel(bot, request, kwargs, private=True)
            else:
                raise ValueError(f"不支持的消息场景: {request.scene}")
        except Exception as exc:
            if exc.__class__.__name__ == "AuditException" or hasattr(exc, "audit_id"):
                return await self._audit_result(exc, request.audit_timeout)
            if isinstance(exc, DeliveryError):
                raise
            kind, retryable = classify_delivery_error(exc)
            raise DeliveryError(kind, retryable, exc) from exc
        return SendResult.from_raw(raw)

    async def _send_channel(
        self,
        bot: Any,
        request: SendRequest,
        kwargs: dict[str, Any],
        *,
        private: bool,
    ) -> SendResult:
        kwargs = dict(kwargs)
        source_message_id = str(kwargs.pop("source_message_id", "") or "")
        kwargs.pop("message_reference_id", None)
        kwargs.pop("msg_seq", None)
        if source_message_id:
            kwargs.setdefault("msg_id", source_message_id)
        if private:
            raw = await bot.send_to_dms(
                guild_id=request.target_id,
                message=request.message,
                **kwargs,
            )
        else:
            raw = await bot.send_to_channel(
                channel_id=request.target_id,
                message=request.message,
                **kwargs,
            )
        result = SendResult.from_raw(raw)
        record_kwargs = {
            "scene": request.scene,
            "message": request.message,
            "message_id": result.message_id or "",
            "source_message_id": source_message_id,
            "raw_result": raw,
        }
        record_kwargs["user_id" if private else "group_id"] = request.target_id
        record_send_message(bot, **record_kwargs)
        return result

    async def send_to_channel(
        self,
        bot: Any,
        channel_id: Any,
        message: Any,
        **kwargs: Any,
    ) -> SendResult:
        return await self.send(
            bot,
            SendRequest("channel_group", str(channel_id), message),
            **kwargs,
        )

    async def send_to_channel_user(
        self,
        bot: Any,
        guild_id: Any,
        message: Any,
        **kwargs: Any,
    ) -> SendResult:
        return await self.send(
            bot,
            SendRequest("channel_private", str(guild_id), message),
            **kwargs,
        )

    async def send_to_group(
        self,
        bot: Any,
        group_id: Any,
        message: Any,
        **kwargs: Any,
    ) -> SendResult:
        return await self.send(
            bot,
            SendRequest("group", str(group_id), message),
            **kwargs,
        )

    async def send_to_user(
        self,
        bot: Any,
        user_id: Any,
        message: Any,
        **kwargs: Any,
    ) -> SendResult:
        return await self.send(
            bot,
            SendRequest("private", str(user_id), message),
            **kwargs,
        )

    async def reply(
        self,
        bot: Any,
        event: Any,
        message: Any,
        *,
        include_reference: bool = True,
        **kwargs: Any,
    ) -> SendResult:
        scene = get_chat_scene(event)
        source_message_id = str(
            getattr(event, "message_id", "") or getattr(event, "id", "") or ""
        )
        # lifecycle notice（入群/退群）需要 event_id 走被动消息，否则主动发送 40034105
        event_id = getattr(event, "event_id", None) or None
        if event_id and "event_id" not in kwargs:
            kwargs["event_id"] = str(event_id)
        reference_id = get_message_reference_id(event) if include_reference else None
        if scene in {"group", "channel_group"}:
            request = SendRequest(
                scene,
                str(get_group_id(event)),
                message,
                reference_id=reference_id,
                source_message_id=source_message_id,
            )
        elif scene in {"private", "channel_private"}:
            target_id = (
                getattr(event, "guild_id", None)
                if scene == "channel_private"
                else get_user_id(event)
            )
            request = SendRequest(
                scene,
                str(target_id),
                message,
                reference_id=reference_id,
                source_message_id=source_message_id,
            )
        else:
            raw = await bot.send(event=event, message=message, **kwargs)
            return SendResult.from_raw(raw)
        return await self.send(bot, request, **kwargs)

    async def reply_enhanced(
        self,
        bot: Any,
        event: Any,
        *,
        markdown: str,
        fallback_text: str,
        keyboard_rows: list[list[tuple[str, str]]] | None = None,
        button_id: str = "",
        **kwargs: Any,
    ) -> SendResult:
        """按 Bot 能力发送 Markdown/keyboard，并保证纯文本降级。"""
        capabilities = self._capabilities.get(bot)
        if not is_qq_bot(bot) or not capabilities.markdown:
            return await self.reply(
                bot,
                event,
                fallback_text,
                include_reference=False,
                **kwargs,
            )

        try:
            from ..adapter_compat import MessageSegment

            rows = keyboard_rows or []
            if rows and capabilities.keyboard:
                message = MessageSegment.markdown_keyboard(bot, markdown or " ", rows)
            else:
                message = MessageSegment.markdown(
                    bot,
                    markdown or " ",
                    button_id if capabilities.keyboard else "",
                )
            return await self.reply(
                bot,
                event,
                message,
                include_reference=False,
                **kwargs,
            )
        except Exception:
            return await self.reply(
                bot,
                event,
                fallback_text,
                include_reference=False,
                **kwargs,
            )

    async def reply_media(
        self,
        bot: Any,
        event: Any,
        media: MediaInput,
        *,
        include_reference: bool = False,
        **kwargs: Any,
    ) -> SendResult:
        segment = await self._media.build_segment(bot, media)
        return await self.reply(
            bot,
            event,
            segment,
            include_reference=include_reference,
            **kwargs,
        )

    async def recall(
        self,
        bot: Any,
        *,
        scene: str,
        message_id: str,
        group_id: str = "",
        user_id: str = "",
    ) -> Any:
        return await delete_message_compat(
            bot,
            scene=scene,
            message_id=message_id,
            group_id=group_id,
            user_id=user_id,
        )


delivery_service = MessageDeliveryService(
    capabilities=QQCapabilityRegistry.from_config(
        type("CapabilityConfig", (), {
            "xiuxian_qq_capabilities": settings.get("xiuxian_qq_capabilities", None)
        })()
    )
)


__all__ = ["MessageDeliveryService", "delivery_service"]
