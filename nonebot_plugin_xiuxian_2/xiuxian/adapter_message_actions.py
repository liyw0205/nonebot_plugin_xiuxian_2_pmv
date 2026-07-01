from __future__ import annotations

import asyncio
from typing import Any

from nonebot.log import logger

from .adapter_message_records import get_adapter_name


async def delete_message_compat(
    bot: Any,
    *,
    scene: str,
    message_id: str,
    group_id: str = "",
    user_id: str = "",
):
    """
    跨适配器通用撤回接口。

    scene:
    - group
    - private
    - channel_group
    - channel_private
    """
    if not message_id:
        raise ValueError("message_id 不能为空")

    from .adapter_compat import HAS_OB11, HAS_QQ, OB11Bot, QQBot

    adapter = get_adapter_name(bot)

    if HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot):
        mid = int(message_id) if str(message_id).isdigit() else message_id

        if hasattr(bot, "delete_msg"):
            return await bot.delete_msg(message_id=mid)

        return await bot.call_api("delete_msg", message_id=mid)

    if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
        if scene == "group":
            if not group_id:
                raise ValueError("QQ 群聊撤回需要 group_id/group_openid")

            return await bot.delete_group_message(
                group_openid=str(group_id),
                message_id=str(message_id),
            )

        if scene == "private":
            if not user_id:
                raise ValueError("QQ 私聊撤回需要 user_id/openid")

            return await bot.delete_c2c_message(
                openid=str(user_id),
                message_id=str(message_id),
            )

        if scene == "channel_group":
            if not group_id:
                raise ValueError("QQ 频道群聊撤回需要 channel_id")

            return await bot.delete_message(
                channel_id=str(group_id),
                message_id=str(message_id),
            )

        if scene == "channel_private":
            guild_id = group_id or user_id
            if not guild_id:
                raise ValueError("QQ 频道私信撤回需要 guild_id")

            return await bot.delete_dms_message(
                guild_id=str(guild_id),
                message_id=str(message_id),
            )

    raise RuntimeError(f"当前适配器不支持通用撤回: {adapter}")


def schedule_delete_message(
    bot: Any,
    *,
    scene: str,
    message_id: str,
    group_id: str = "",
    user_id: str = "",
    revoke_time: int | float = 0,
):
    """
    定时撤回消息。

    revoke_time:
    - <= 0 不撤回
    - > 0 按秒延迟撤回
    """
    try:
        revoke_time = float(revoke_time or 0)
    except Exception:
        revoke_time = 0

    if revoke_time <= 0 or not message_id:
        return

    async def _job():
        try:
            await asyncio.sleep(revoke_time)

            await delete_message_compat(
                bot,
                scene=scene,
                message_id=message_id,
                group_id=group_id,
                user_id=user_id,
            )

            logger.info(
                f"[自动撤回] 已撤回消息 scene={scene}, message_id={message_id}, "
                f"group_id={group_id}, user_id={user_id}"
            )

        except Exception as e:
            logger.warning(
                f"[自动撤回] 撤回失败 scene={scene}, message_id={message_id}, "
                f"group_id={group_id}, user_id={user_id}: {e}"
            )

    try:
        asyncio.create_task(_job())
    except RuntimeError:
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_job())
        except Exception as e:
            logger.warning(f"[自动撤回] 创建撤回任务失败: {e}")


__all__ = [
    "delete_message_compat",
    "schedule_delete_message",
]
