from __future__ import annotations

from typing import Any

from ...core.result import ReplyPlan, is_markdown_intent
from ...infrastructure.messaging import MessageGatewayAdapter


def plain_text_fallback(content: str) -> str:
    """Downgrade a Markdown plan to the text a non-Markdown Bot can show.

    Reuses the shared downgrade so the fallback keeps the same wording as the
    rest of the plugin instead of inventing a second stripper.
    """

    from ...xiuxian.xiuxian_utils.message_markdown import strip_md_command_links

    return strip_md_command_links(content)


class NoneBotMessageGateway(MessageGatewayAdapter):
    """Adapter around a configured ``MessageDeliveryService.reply``.

    Delivery follows the plan's explicit ``intent``:

    * ``intent="markdown"`` with string content -> ``reply_enhanced`` (native
      QQ Markdown, capability-gated, plain-text fallback when the Bot is not a
      QQ Bot, lacks the capability or the send raises);
    * anything else (default ``plain``, non-string content such as media
      segments) -> ``reply`` exactly as before.
    """

    def __init__(self, delivery_service: Any) -> None:
        async def send(context: Any, reply: ReplyPlan):
            event = getattr(context, "raw_event", context)
            bot = getattr(context, "bot", None)
            content = reply.content
            if is_markdown_intent(getattr(reply, "intent", "")) and isinstance(
                content, str
            ) and content.strip():
                enhanced = getattr(delivery_service, "reply_enhanced", None)
                if callable(enhanced):
                    return await enhanced(
                        bot,
                        event,
                        markdown=content,
                        fallback_text=plain_text_fallback(content),
                        allow_plain_fallback=True,
                    )
            return await delivery_service.reply(bot, event, content)

        super().__init__(send)


__all__ = ["NoneBotMessageGateway", "plain_text_fallback"]
