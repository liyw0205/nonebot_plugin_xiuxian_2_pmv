from __future__ import annotations

from typing import Any

from ..adapter_compat import (
    get_chat_scene,
    get_group_id,
    get_message_reference_id,
    get_user_id,
)
from ..adapter_message_actions import delete_message_compat
from ..adapter_message_records import record_send_message
from ..adapter_message_sender import send_group_message, send_private_message
from .models import SendRequest, SendResult


class MessageDeliveryService:
    """收敛主动发送与回复入口，底层继续复用现有 Adapter 兼容实现。"""

    async def send(self, bot: Any, request: SendRequest, **kwargs: Any) -> SendResult:
        if request.reference_id:
            kwargs.setdefault("message_reference_id", request.reference_id)
        if request.source_message_id:
            kwargs.setdefault("source_message_id", request.source_message_id)
        if request.revoke_after:
            kwargs.setdefault("revoke_after", request.revoke_after)

        if request.scene == "group":
            raw = await send_group_message(
                bot,
                group_id=request.target_id,
                message=request.message,
                **kwargs,
            )
        elif request.scene == "private":
            raw = await send_private_message(
                bot,
                user_id=request.target_id,
                message=request.message,
                **kwargs,
            )
        elif request.scene == "channel_group":
            source_message_id = str(kwargs.pop("source_message_id", "") or "")
            kwargs.pop("message_reference_id", None)
            kwargs.pop("msg_seq", None)
            if source_message_id:
                kwargs.setdefault("msg_id", source_message_id)
            raw = await bot.send_to_channel(
                channel_id=request.target_id,
                message=request.message,
                **kwargs,
            )
            result = SendResult.from_raw(raw)
            record_send_message(
                bot,
                scene=request.scene,
                message=request.message,
                message_id=result.message_id or "",
                source_message_id=source_message_id,
                group_id=request.target_id,
                raw_result=raw,
            )
        elif request.scene == "channel_private":
            source_message_id = str(kwargs.pop("source_message_id", "") or "")
            kwargs.pop("message_reference_id", None)
            kwargs.pop("msg_seq", None)
            if source_message_id:
                kwargs.setdefault("msg_id", source_message_id)
            raw = await bot.send_to_dms(
                guild_id=request.target_id,
                message=request.message,
                **kwargs,
            )
            result = SendResult.from_raw(raw)
            record_send_message(
                bot,
                scene=request.scene,
                message=request.message,
                message_id=result.message_id or "",
                source_message_id=source_message_id,
                user_id=request.target_id,
                raw_result=raw,
            )
        else:
            raise ValueError(f"不支持的消息场景: {request.scene}")
        return result if request.scene.startswith("channel_") else SendResult.from_raw(raw)

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


delivery_service = MessageDeliveryService()


__all__ = ["MessageDeliveryService", "delivery_service"]
