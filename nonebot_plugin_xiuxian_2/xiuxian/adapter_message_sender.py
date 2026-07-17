from __future__ import annotations

import random
from typing import Any

from .adapter_message_actions import schedule_delete_message
from .adapter_message_records import extract_result_message_id, record_send_message


def _maybe_int(value: Any) -> Any:
    text = str(value)
    return int(text) if text.isdigit() else value


def _is_ob11_bot(bot: Any) -> bool:
    try:
        from .adapter_compat import HAS_OB11, OB11Bot

        return bool(HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot))
    except Exception:
        return False


def is_qq_bot(bot: Any) -> bool:
    try:
        from .adapter_compat import HAS_QQ, QQBot

        return bool(HAS_QQ and QQBot is not None and isinstance(bot, QQBot))
    except Exception:
        return False


def _pop_revoke_time(kwargs: dict[str, Any]) -> int | float:
    return kwargs.pop("revoke_time", kwargs.pop("revoke_after", 0))


def _pop_reference_id(kwargs: dict[str, Any]) -> Any:
    for key in (
        "msg_ref_id",
        "reference_id",
        "message_reference_id",
        "reference_message_id",
        "quote_message_id",
    ):
        value = kwargs.pop(key, None)
        if value:
            return value
    return None


async def _call_qq_send(send_func, **kwargs):
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    try:
        return await send_func(**kwargs)
    except TypeError as e:
        if "msg_ref_id" not in str(e):
            raise
        kwargs.pop("msg_ref_id", None)
        return await send_func(**kwargs)


def _record_send(
    bot: Any,
    *,
    scene: str,
    message: Any,
    result: Any,
    group_id: str = "",
    user_id: str = "",
    source_message_id: str = "",
    revoke_time: int | float = 0,
):
    message_id = extract_result_message_id(result)
    record_send_message(
        bot,
        scene=scene,
        message=message,
        message_id=message_id,
        source_message_id=source_message_id,
        group_id=group_id,
        user_id=user_id,
        raw_result=result,
    )
    schedule_delete_message(
        bot,
        scene=scene,
        message_id=message_id,
        group_id=group_id,
        user_id=user_id,
        revoke_time=revoke_time,
    )


async def send_group_message(bot: Any, *, group_id: Any, message: Any, **kwargs):
    """主动发送群消息，按适配器调用实际接口。"""
    revoke_time = _pop_revoke_time(kwargs)
    source_message_id = str(kwargs.pop("source_message_id", "") or "")

    if _is_ob11_bot(bot):
        patched = bool(getattr(bot, "__message_db_ob11_send_patched__", False))
        api_kwargs = dict(kwargs)
        if patched and revoke_time:
            api_kwargs["revoke_time"] = revoke_time
        result = await bot.call_api(
            "send_group_msg",
            group_id=_maybe_int(group_id),
            message=message,
            **api_kwargs,
        )
        if not patched:
            _record_send(
                bot,
                scene="group",
                message=message,
                result=result,
                group_id=str(group_id or ""),
                source_message_id=source_message_id,
                revoke_time=revoke_time,
            )
        return result

    if is_qq_bot(bot):
        msg_ref_id = _pop_reference_id(kwargs)
        msg_seq = kwargs.pop("msg_seq", random.randint(1, 900000))
        if source_message_id:
            # 普通消息回复用 msg_id；lifecycle 事件 id 形如 GROUP_ADD_ROBOT:xxx，应走 event_id
            eid = str(source_message_id)
            if ":" in eid and eid.split(":", 1)[0].isupper():
                kwargs.setdefault("event_id", eid)
            else:
                kwargs.setdefault("msg_id", eid)
        # 显式 event_id 优先
        event_id = kwargs.pop("event_id", None)
        call_kwargs = dict(kwargs)
        if event_id:
            call_kwargs["event_id"] = str(event_id)
        result = await _call_qq_send(
            bot.send_to_group,
            group_openid=str(group_id),
            message=message,
            msg_seq=int(msg_seq),
            msg_ref_id=msg_ref_id,
            **call_kwargs,
        )
        _record_send(
            bot,
            scene="group",
            message=message,
            result=result,
            group_id=str(group_id or ""),
            source_message_id=source_message_id,
            revoke_time=revoke_time,
        )
        return result

    if hasattr(bot, "call_api"):
        result = await bot.call_api(
            "send_group_msg",
            group_id=_maybe_int(group_id),
            message=message,
            **kwargs,
        )
        _record_send(
            bot,
            scene="group",
            message=message,
            result=result,
            group_id=str(group_id or ""),
            source_message_id=source_message_id,
            revoke_time=revoke_time,
        )
        return result

    raise RuntimeError(f"当前 bot 不支持主动群消息发送: {type(bot)!r}")


async def send_private_message(bot: Any, *, user_id: Any, message: Any, **kwargs):
    """主动发送私聊消息，按适配器调用实际接口。"""
    revoke_time = _pop_revoke_time(kwargs)
    source_message_id = str(kwargs.pop("source_message_id", "") or "")

    if _is_ob11_bot(bot):
        patched = bool(getattr(bot, "__message_db_ob11_send_patched__", False))
        api_kwargs = dict(kwargs)
        if patched and revoke_time:
            api_kwargs["revoke_time"] = revoke_time
        result = await bot.call_api(
            "send_private_msg",
            user_id=_maybe_int(user_id),
            message=message,
            **api_kwargs,
        )
        if not patched:
            _record_send(
                bot,
                scene="private",
                message=message,
                result=result,
                user_id=str(user_id or ""),
                source_message_id=source_message_id,
                revoke_time=revoke_time,
            )
        return result

    if is_qq_bot(bot):
        msg_ref_id = _pop_reference_id(kwargs)
        msg_seq = kwargs.pop("msg_seq", random.randint(1, 900000))
        if source_message_id:
            kwargs.setdefault("msg_id", source_message_id)
        result = await _call_qq_send(
            bot.send_to_c2c,
            openid=str(user_id),
            message=message,
            msg_seq=int(msg_seq),
            msg_ref_id=msg_ref_id,
            **kwargs,
        )
        _record_send(
            bot,
            scene="private",
            message=message,
            result=result,
            user_id=str(user_id or ""),
            source_message_id=source_message_id,
            revoke_time=revoke_time,
        )
        return result

    if hasattr(bot, "call_api"):
        result = await bot.call_api(
            "send_private_msg",
            user_id=_maybe_int(user_id),
            message=message,
            **kwargs,
        )
        _record_send(
            bot,
            scene="private",
            message=message,
            result=result,
            user_id=str(user_id or ""),
            source_message_id=source_message_id,
            revoke_time=revoke_time,
        )
        return result

    raise RuntimeError(f"当前 bot 不支持主动私聊消息发送: {type(bot)!r}")


__all__ = [
    "is_qq_bot",
    "send_group_message",
    "send_private_message",
]
