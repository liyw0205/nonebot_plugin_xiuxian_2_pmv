from __future__ import annotations

from dataclasses import dataclass, field
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

_GROUP_ACTIONS = {
    "bot_join_group",
    "bot_leave_group",
    "group_reject",
    "group_receive",
    "member_join_group",
    "member_leave_group",
}


@dataclass(frozen=True)
class LifecycleGroupState:
    bot_id: str
    group_id: str
    joined: bool | None = None
    message_receive_enabled: bool | None = None
    event_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class LifecycleApplyResult:
    context: QQLifecycleContext
    state: LifecycleGroupState | None
    action_count: int
    member_left: bool = False


@dataclass
class _MutableGroupState:
    joined: bool | None = None
    message_receive_enabled: bool | None = None
    event_counts: dict[str, int] = field(default_factory=dict)


class LifecycleStateRegistry:
    """记录 QQ 生命周期事件形成的进程内群能力状态。"""

    def __init__(self) -> None:
        self._groups: dict[tuple[str, str], _MutableGroupState] = {}
        self._bot_action_counts: dict[tuple[str, str], int] = {}

    def apply(self, bot: Any, event: Any) -> LifecycleApplyResult:
        context = get_lifecycle_context(event)
        bot_id = str(getattr(bot, "self_id", "") or "")
        if not bot_id:
            raise ValueError("QQ 生命周期状态更新缺少 bot.self_id")

        count_key = (bot_id, context.action)
        action_count = self._bot_action_counts.get(count_key, 0) + 1
        self._bot_action_counts[count_key] = action_count

        state = None
        if context.action in _GROUP_ACTIONS:
            if not context.group_id:
                raise ValueError(f"QQ 群生命周期事件缺少 group_id: {context.event_type}")
            key = (bot_id, context.group_id)
            mutable = self._groups.setdefault(key, _MutableGroupState())
            mutable.event_counts[context.action] = (
                mutable.event_counts.get(context.action, 0) + 1
            )
            if context.action == "bot_join_group":
                mutable.joined = True
            elif context.action == "bot_leave_group":
                mutable.joined = False
                mutable.message_receive_enabled = False
            elif context.action == "group_receive":
                mutable.message_receive_enabled = True
            elif context.action == "group_reject":
                mutable.message_receive_enabled = False
            state = self._snapshot(bot_id, context.group_id, mutable)

        return LifecycleApplyResult(
            context=context,
            state=state,
            action_count=action_count,
            member_left=context.action == "member_leave_group",
        )

    def get_group_state(self, bot_id: str, group_id: str) -> LifecycleGroupState | None:
        mutable = self._groups.get((str(bot_id), str(group_id)))
        if mutable is None:
            return None
        return self._snapshot(str(bot_id), str(group_id), mutable)

    def get_action_count(self, bot_id: str, action: str) -> int:
        return self._bot_action_counts.get((str(bot_id), action), 0)

    def clear(self) -> None:
        self._groups.clear()
        self._bot_action_counts.clear()

    @staticmethod
    def _snapshot(
        bot_id: str,
        group_id: str,
        state: _MutableGroupState,
    ) -> LifecycleGroupState:
        return LifecycleGroupState(
            bot_id=bot_id,
            group_id=group_id,
            joined=state.joined,
            message_receive_enabled=state.message_receive_enabled,
            event_counts=dict(state.event_counts),
        )


_LIFECYCLE_STATE_REGISTRY = LifecycleStateRegistry()


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


def apply_lifecycle_event(bot: Any, event: Any) -> LifecycleApplyResult:
    return _LIFECYCLE_STATE_REGISTRY.apply(bot, event)


def get_lifecycle_group_state(bot_id: str, group_id: str) -> LifecycleGroupState | None:
    return _LIFECYCLE_STATE_REGISTRY.get_group_state(bot_id, group_id)


__all__ = [
    "LifecycleApplyResult",
    "LifecycleGroupState",
    "LifecycleStateRegistry",
    "apply_lifecycle_event",
    "get_lifecycle_context",
    "get_lifecycle_group_state",
    "is_lifecycle_event",
]
