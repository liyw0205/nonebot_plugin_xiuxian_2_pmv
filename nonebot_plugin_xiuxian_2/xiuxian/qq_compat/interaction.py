from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Any

from .context import _event_type, _text, _value, get_qq_scene, is_qq_event
from .models import QQInteractionContext


def is_interaction_event(event: Any) -> bool:
    return is_qq_event(event) and (
        "INTERACTION" in _event_type(event).upper()
        or event.__class__.__name__ == "InteractionCreateEvent"
    )


def get_interaction_context(event: Any) -> QQInteractionContext:
    if not is_interaction_event(event):
        raise ValueError(f"不是 QQ 交互事件: {type(event)!r}")
    resolved = _value(_value(event, "data"), "resolved")
    return QQInteractionContext(
        interaction_id=_text(_value(event, "interaction_id", "id")),
        event_id=_text(_value(event, "event_id")) or None,
        scene=get_qq_scene(event),
        user_id=_text(
            _value(event, "group_member_openid", "user_openid")
            or _value(resolved, "user_id")
        ),
        group_id=_text(_value(event, "group_openid", "group_id")) or None,
        guild_id=_text(_value(event, "guild_id")) or None,
        channel_id=_text(_value(event, "channel_id")) or None,
        message_id=_text(_value(resolved, "message_id")) or None,
        button_id=_text(_value(resolved, "button_id")) or None,
        button_data=_text(_value(resolved, "button_data")) or None,
        feature_id=_text(_value(resolved, "feature_id")) or None,
        raw_event=event,
    )


class InteractionAcknowledger:
    """按 interaction_id 保证进程内最多确认一次。"""

    def __init__(self, *, max_tracked: int = 5000) -> None:
        if max_tracked <= 0:
            raise ValueError("max_tracked 必须大于 0")
        self._max_tracked = max_tracked
        self._acked: OrderedDict[str, None] = OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}

    async def ack(self, bot: Any, event: Any, code: int = 0) -> bool:
        context = get_interaction_context(event)
        if not context.interaction_id:
            raise ValueError("交互事件缺少 interaction_id")
        if code not in range(6):
            raise ValueError("QQ 交互 ACK code 必须位于 0 到 5")

        lock = self._locks.setdefault(context.interaction_id, asyncio.Lock())
        try:
            async with lock:
                if context.interaction_id in self._acked:
                    return False
                if hasattr(bot, "put_interaction"):
                    await bot.put_interaction(interaction_id=context.interaction_id, code=code)
                elif hasattr(bot, "call_api"):
                    await bot.call_api(
                        "put_interaction",
                        interaction_id=context.interaction_id,
                        code=code,
                    )
                else:
                    raise RuntimeError(f"当前 bot 不支持 QQ 交互 ACK: {type(bot)!r}")
                self._acked[context.interaction_id] = None
                while len(self._acked) > self._max_tracked:
                    self._acked.popitem(last=False)
                return True
        finally:
            self._locks.pop(context.interaction_id, None)


_ACKNOWLEDGER = InteractionAcknowledger()


async def ack_interaction(bot: Any, event: Any, code: int = 0) -> bool:
    return await _ACKNOWLEDGER.ack(bot, event, code)


async def run_with_interaction_ack(
    bot: Any,
    event: Any,
    operation,
    *,
    timeout: float = 1.8,
    success_code: int = 0,
    fallback_code: int = 0,
):
    """执行交互处理，并在超时前发送一次平台 ACK。"""

    task = asyncio.create_task(operation())
    try:
        result = await asyncio.wait_for(asyncio.shield(task), timeout=max(0.0, timeout))
    except asyncio.TimeoutError:
        await ack_interaction(bot, event, fallback_code)
        return await task
    except BaseException:
        await ack_interaction(bot, event, fallback_code)
        raise
    await ack_interaction(bot, event, success_code)
    return result


__all__ = [
    "InteractionAcknowledger",
    "ack_interaction",
    "get_interaction_context",
    "is_interaction_event",
    "run_with_interaction_ack",
]
