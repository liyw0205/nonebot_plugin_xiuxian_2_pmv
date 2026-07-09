from __future__ import annotations

from typing import Any

from .context import _event_type, _text, _value, get_qq_scene, is_qq_event
from .models import QQLifecycleContext


_LIFECYCLE_ACTIONS = {
    "FRIEND_ADD": "friend_add",
    "FRIEND_DEL": "friend_remove",
    "C2C_MSG_REJECT": "c2c_reject",
    "C2C_MSG_RECEIVE": "c2c_receive",
    "GROUP_ADD_ROBOT": "bot_join_group",
    "GROUP_DEL_ROBOT": "bot_leave_group",
    "GROUP_MSG_REJECT": "group_reject",
    "GROUP_MSG_RECEIVE": "group_receive",
    "GROUP_MEMBER_ADD": "member_join_group",
    "GROUP_MEMBER_REMOVE": "member_leave_group",
}


def is_lifecycle_event(event: Any) -> bool:
    return is_qq_event(event) and (
        get_qq_scene(event) == "lifecycle" or _event_type(event).upper() in _LIFECYCLE_ACTIONS
    )


def get_lifecycle_context(event: Any) -> QQLifecycleContext:
    event_type = _event_type(event).upper()
    if not is_lifecycle_event(event) or event_type not in _LIFECYCLE_ACTIONS:
        raise ValueError(f"不是受支持的 QQ 生命周期事件: {event_type}")
    return QQLifecycleContext(
        event_type=event_type,
        user_id=_text(
            _value(event, "member_openid", "op_member_openid", "openid", "user_openid")
        ),
        group_id=_text(_value(event, "group_openid", "group_id")) or None,
        action=_LIFECYCLE_ACTIONS[event_type],
        raw_event=event,
    )


__all__ = ["get_lifecycle_context", "is_lifecycle_event"]
