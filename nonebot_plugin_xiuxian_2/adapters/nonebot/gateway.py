from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan
from ...infrastructure.messaging import MessageGatewayAdapter


class NoneBotMessageGateway(MessageGatewayAdapter):
    """Adapter around a configured ``MessageDeliveryService.reply``."""

    def __init__(self, delivery_service: Any) -> None:
        async def send(context: Any, reply: ReplyPlan):
            event = getattr(context, "raw_event", context)
            bot = getattr(context, "bot", None)
            return await delivery_service.reply(bot, event, reply.content)

        super().__init__(send)


__all__ = ["NoneBotMessageGateway"]
