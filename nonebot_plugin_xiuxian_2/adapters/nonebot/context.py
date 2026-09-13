from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...core.result import ReplyPlan


@dataclass(frozen=True)
class CommandContext:
    platform: str
    scene: str
    group_id: str | None
    user_id: str
    message_id: str | None
    raw_event: Any = None
    bot: Any = None


def _value(obj: Any, *names: str) -> Any:
    for name in names:
        value = getattr(obj, name, None)
        if value not in (None, ""):
            return value
        if isinstance(obj, dict) and obj.get(name) not in (None, ""):
            return obj[name]
    return None


def context_from_event(event: Any, *, platform: str | None = None, bot: Any = None) -> CommandContext:
    module_parts = str(getattr(event.__class__, "__module__", "")).split(".")
    detected = platform
    if not detected:
        if "qq" in module_parts:
            detected = "qq"
        elif "onebot" in module_parts:
            detected = "onebot"
        else:
            detected = module_parts[-2] if len(module_parts) > 1 else "unknown"
    detected = detected if detected not in {"", "event"} else "unknown"
    group_id = _value(event, "group_id", "group_openid")
    guild_id = _value(event, "guild_id")
    user_id = _value(event, "user_id", "user_openid", "author_id", "sender_id")
    author = _value(event, "author", "sender")
    if not user_id and author is not None:
        user_id = _value(author, "id", "user_id", "member_openid", "user_openid")
    scene = "group" if group_id else "private"
    if guild_id and not group_id:
        group_id = guild_id
        scene = "channel"
    return CommandContext(
        platform=detected,
        scene=scene,
        group_id=str(group_id) if group_id is not None else None,
        user_id=str(user_id or ""),
        message_id=str(_value(event, "message_id", "id", "event_id") or "") or None,
        raw_event=event,
        bot=bot,
    )


class ReplyGateway:
    """Adapter boundary; concrete NoneBot delivery is injected by composition."""

    def __init__(self, sender) -> None:
        self._sender = sender

    async def send(self, context: CommandContext, reply: ReplyPlan) -> Any:
        return await self._sender(context, reply)


__all__ = ["CommandContext", "ReplyGateway", "context_from_event"]
