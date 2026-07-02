from __future__ import annotations

from . import ensure_vendored_adapters

ensure_vendored_adapters()

from nonebot.adapters.onebot.v11 import Bot, Message, MessageSegment  # noqa: E402
from nonebot.adapters.onebot.v11.event import (  # noqa: E402
    GroupMessageEvent,
    PrivateMessageEvent,
)

__all__ = [
    "Bot",
    "GroupMessageEvent",
    "Message",
    "MessageSegment",
    "PrivateMessageEvent",
]
