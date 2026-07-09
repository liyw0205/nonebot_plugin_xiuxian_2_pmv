from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


QQScene = Literal["group", "c2c", "channel", "interaction", "lifecycle", "unknown"]


@dataclass(frozen=True)
class QQMentionState:
    at_self: bool = False
    at_all: bool = False
    other_user_ids: tuple[str, ...] = ()
    other_bot_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class QQAttachment:
    content_type: str
    url: str | None = None
    filename: str | None = None
    size: int | None = None
    width: int | None = None
    height: int | None = None
    raw: Any = None


@dataclass(frozen=True)
class QQEventContext:
    event_type: str
    scene: QQScene
    user_id: str
    raw_user_id: str
    union_openid: str | None
    member_openid: str | None
    group_id: str | None
    guild_id: str | None
    channel_id: str | None
    message_id: str | None
    event_id: str | None
    reference_id: str | None
    content: str
    raw_content: str
    mentions: QQMentionState
    attachments: tuple[QQAttachment, ...]
    raw_event: Any


@dataclass(frozen=True)
class QQInteractionContext:
    interaction_id: str
    event_id: str | None
    scene: QQScene
    user_id: str
    group_id: str | None
    guild_id: str | None
    channel_id: str | None
    message_id: str | None
    button_id: str | None
    button_data: str | None
    feature_id: str | None
    raw_event: Any


@dataclass(frozen=True)
class QQLifecycleContext:
    event_type: str
    user_id: str
    group_id: str | None
    action: str
    raw_event: Any


__all__ = [
    "QQAttachment",
    "QQEventContext",
    "QQInteractionContext",
    "QQLifecycleContext",
    "QQMentionState",
    "QQScene",
]
