from __future__ import annotations

from . import ensure_vendored_adapters

ensure_vendored_adapters()

from nonebot.adapters.qq import Bot, Message, MessageSegment  # noqa: E402
from nonebot.adapters.qq import event as qq_event  # noqa: E402
from nonebot.adapters.qq.event import (  # noqa: E402
    AtMessageCreateEvent,
    C2CMessageCreateEvent,
    DirectMessageCreateEvent,
)
from nonebot.adapters.qq.models import (  # noqa: E402
    Action,
    Button,
    InlineKeyboard,
    InlineKeyboardRow,
    MessageKeyboard,
    MessageMarkdown,
    Permission,
    RenderData,
)

GroupAtMessageCreateEvent = getattr(qq_event, "GroupAtMessageCreateEvent", None)
GroupMessageCreateEvent = getattr(qq_event, "GroupMessageCreateEvent", None)

__all__ = [
    "Action",
    "AtMessageCreateEvent",
    "Bot",
    "Button",
    "C2CMessageCreateEvent",
    "DirectMessageCreateEvent",
    "GroupAtMessageCreateEvent",
    "GroupMessageCreateEvent",
    "InlineKeyboard",
    "InlineKeyboardRow",
    "Message",
    "MessageKeyboard",
    "MessageMarkdown",
    "MessageSegment",
    "Permission",
    "RenderData",
]
