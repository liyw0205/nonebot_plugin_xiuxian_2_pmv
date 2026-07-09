from __future__ import annotations

from typing import Any, Iterable

from ..adapter_compat import get_message_reference_id
from .models import QQAttachment, QQEventContext, QQMentionState, QQScene


def _value(source: Any, *names: str) -> Any:
    for name in names:
        if isinstance(source, dict):
            value = source.get(name)
        else:
            value = getattr(source, name, None)
        if value is not None and value != "":
            return value
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _event_type(event: Any) -> str:
    value = _value(event, "event_type", "__type__")
    if value is not None:
        return _text(getattr(value, "value", value))
    try:
        return _text(event.get_event_name())
    except Exception:
        return event.__class__.__name__


def is_qq_event(event: Any) -> bool:
    module_name = event.__class__.__module__.lower()
    if "nonebot.adapters.qq" in module_name:
        return True
    event_type = _event_type(event).upper()
    return event_type.startswith(("C2C_", "GROUP_", "FRIEND_", "INTERACTION_")) or any(
        _value(event, name) is not None
        for name in ("group_openid", "user_openid", "member_openid", "message_scene")
    )


def get_qq_scene(event: Any) -> QQScene:
    event_type = _event_type(event).upper()
    if "INTERACTION" in event_type or event.__class__.__name__ == "InteractionCreateEvent":
        return "interaction"
    if event_type.startswith(("FRIEND_", "GROUP_")) and "MESSAGE_CREATE" not in event_type:
        return "lifecycle"
    if event_type.startswith("C2C_") or _value(event, "user_openid") is not None:
        return "c2c"
    if event_type.startswith("GROUP_") and "MESSAGE_CREATE" in event_type:
        return "group"
    if _value(event, "group_openid", "group_id") is not None and _value(
        event, "guild_id", "channel_id"
    ) is None:
        return "group"
    if _value(event, "guild_id", "channel_id") is not None:
        return "channel"
    return "unknown"


def _iter_values(value: Any) -> Iterable[Any]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, dict)):
        return (value,)
    try:
        return tuple(value)
    except TypeError:
        return (value,)


def _parse_mentions(event: Any) -> QQMentionState:
    at_self = False
    at_all = False
    users: list[str] = []
    bots: list[str] = []
    for mention in _iter_values(_value(event, "mentions")):
        scope = _text(_value(mention, "scope")).lower()
        is_self = bool(_value(mention, "is_you"))
        is_bot = bool(_value(mention, "bot"))
        mention_id = _text(_value(mention, "member_openid", "user_openid", "id"))
        if scope == "all":
            at_all = True
        elif is_self:
            at_self = True
        elif mention_id:
            (bots if is_bot else users).append(mention_id)
    return QQMentionState(at_self, at_all, tuple(users), tuple(bots))


def _parse_attachments(event: Any) -> tuple[QQAttachment, ...]:
    attachments: list[QQAttachment] = []
    for item in _iter_values(_value(event, "attachments")):
        attachments.append(
            QQAttachment(
                content_type=_text(_value(item, "content_type", "type")),
                url=_text(_value(item, "url")) or None,
                filename=_text(_value(item, "filename", "name")) or None,
                size=_value(item, "size"),
                width=_value(item, "width"),
                height=_value(item, "height"),
                raw=item,
            )
        )
    return tuple(attachments)


def _message_content(event: Any) -> tuple[str, str]:
    raw = _text(_value(event, "raw_content", "content", "raw_message"))
    try:
        message = event.get_message()
        if hasattr(message, "extract_plain_text"):
            content = _text(message.extract_plain_text())
        else:
            content = _text(message)
    except Exception:
        content = raw
    return content, raw


def from_nonebot_event(event: Any) -> QQEventContext:
    if not is_qq_event(event):
        raise ValueError(f"不是 QQ 官方 Adapter 事件: {type(event)!r}")

    author = _value(event, "author")
    resolved = _value(_value(event, "data"), "resolved")
    user_id = _text(
        _value(
            event,
            "member_openid",
            "group_member_openid",
            "user_openid",
            "openid",
            "user_id",
        )
        or _value(author, "member_openid", "user_openid", "id")
        or _value(resolved, "user_id")
    )
    raw_user_id = _text(_value(author, "id") or _value(event, "user_id") or user_id)
    content, raw_content = _message_content(event)
    return QQEventContext(
        event_type=_event_type(event),
        scene=get_qq_scene(event),
        user_id=user_id,
        raw_user_id=raw_user_id,
        union_openid=_text(_value(author, "union_openid")) or None,
        member_openid=_text(
            _value(event, "member_openid", "group_member_openid")
            or _value(author, "member_openid")
        )
        or None,
        group_id=_text(_value(event, "group_openid", "group_id")) or None,
        guild_id=_text(_value(event, "guild_id")) or None,
        channel_id=_text(_value(event, "channel_id")) or None,
        message_id=_text(_value(event, "message_id", "id")) or None,
        event_id=_text(_value(event, "event_id")) or None,
        reference_id=get_message_reference_id(event),
        content=content,
        raw_content=raw_content,
        mentions=_parse_mentions(event),
        attachments=_parse_attachments(event),
        raw_event=event,
    )


__all__ = ["from_nonebot_event", "get_qq_scene", "is_qq_event"]
