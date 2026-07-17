"""进群欢迎 + 全量群自动标记。"""

from __future__ import annotations

from nonebot import on_notice
from nonebot.log import logger
from nonebot.matcher import Matcher
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..qq_compat.lifecycle import apply_lifecycle_event, is_lifecycle_event
from ..xiuxian_config import JsonConfig, XiuConfig
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import handle_send


def _event_type_name(event) -> str:
    names: list[str] = []
    for attr in ("__type__", "type"):
        value = getattr(event, attr, None)
        if value is not None:
            names.append(str(value))
    try:
        names.append(str(event.get_event_name()))
    except Exception:
        pass
    return " ".join(names).upper()


def _extract_group_id(event) -> str:
    for key in ("group_openid", "group_id", "groupId"):
        value = getattr(event, key, None)
        if value:
            return str(value)
    return ""


def _is_member_join(event) -> bool:
    name = _event_type_name(event)
    return any(
        token in name
        for token in (
            "GROUP_MEMBER_ADD",
            "GROUPINCREASE",
            "GROUP_INCREASE",
            "MEMBER_JOIN",
            "NOTICE.GROUP_INCREASE",
        )
    )


def _is_full_message_signal(event) -> bool:
    """
    全量消息信号：
    - GROUP_MESSAGE_CREATE（群全量消息）
    - group_receive / GROUP_MSG_RECEIVE（申请全量后收到消息）
    """
    name = _event_type_name(event)
    if "GROUP_MESSAGE_CREATE" in name:
        return True
    if "GROUP_MSG_RECEIVE" in name:
        return True
    try:
        if is_lifecycle_event(event):
            # 交给 apply 后根据 action 判断
            return True
    except Exception:
        pass
    return False


lifecycle_notice = on_notice(priority=5, block=False)


@lifecycle_notice.handle()
async def handle_group_lifecycle(bot: Bot, event, matcher: Matcher):
    conf = JsonConfig()
    group_id = _extract_group_id(event)
    event_name = _event_type_name(event)

    # 1) 全量群自动标记：申请/收到全量消息事件时记录
    try:
        if is_lifecycle_event(event):
            result = apply_lifecycle_event(bot, event)
            action = result.context.action
            gid = result.context.group_id or group_id
            if action in {"group_receive", "bot_join_group"} and gid:
                if conf.mark_full_message_group(gid):
                    logger.info(f"[全量群] 自动标记 group={gid} via={action}")
            # 进群欢迎：成员进群
            if action == "member_join_group" and gid and conf.is_group_welcome_enabled(gid):
                await _send_welcome(bot, event, gid)
                await matcher.finish()
        elif "GROUP_MESSAGE_CREATE" in event_name and group_id:
            if conf.mark_full_message_group(group_id):
                logger.info(f"[全量群] 自动标记 group={group_id} via=GROUP_MESSAGE_CREATE")
    except Exception as e:
        logger.debug(f"[全量群/生命周期] 处理失败: {e}")

    # 2) OneBot / 其他适配器成员进群
    if _is_member_join(event) and group_id and conf.is_group_welcome_enabled(group_id):
        await _send_welcome(bot, event, group_id)
        await matcher.finish()


async def _send_welcome(bot: Bot, event, group_id: str) -> None:
    conf = JsonConfig()
    if not conf.is_group_welcome_enabled(group_id):
        return
    msg = (XiuConfig().group_welcome_msg or "").strip() or "欢迎道友入群！"
    try:
        bot, _ = await assign_bot(bot=bot, event=event)
    except Exception:
        pass
    try:
        await handle_send(
            bot,
            event,
            msg,
            md_type="修仙",
            k1="我要修仙",
            v1="我要修仙",
            k2="修仙帮助",
            v2="修仙帮助",
            k3="关闭欢迎",
            v3="关闭进群欢迎",
            k4="娱乐帮助",
            v4="娱乐帮助",
        )
    except Exception as e:
        logger.warning(f"[进群欢迎] 发送失败 group={group_id}: {e}")


# ---------- 开关指令（对齐默认回复式本群配置） ----------
welcome_enable_cmd = on_command(
    "开启进群欢迎",
    aliases={"启用进群欢迎", "打开进群欢迎"},
    permission=SUPERUSER,
    priority=5,
    block=True,
)
welcome_disable_cmd = on_command(
    "关闭进群欢迎",
    aliases={"禁用进群欢迎", "关掉进群欢迎"},
    permission=SUPERUSER,
    priority=5,
    block=True,
)


@welcome_enable_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def welcome_enable_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    if isinstance(event, PrivateMessageEvent):
        await handle_send(bot, event, "请在群内使用：开启进群欢迎")
        await welcome_enable_cmd.finish()
    group_id = str(getattr(event, "group_id", "") or "")
    ok, msg = JsonConfig().set_group_welcome(group_id, enabled=True)
    await handle_send(
        bot,
        event,
        msg if ok else msg,
        md_type="修仙",
        k1="关闭欢迎",
        v1="关闭进群欢迎",
        k2="修仙帮助",
        v2="修仙帮助",
    )
    await welcome_enable_cmd.finish()


@welcome_disable_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def welcome_disable_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    if isinstance(event, PrivateMessageEvent):
        await handle_send(bot, event, "请在群内使用：关闭进群欢迎")
        await welcome_disable_cmd.finish()
    group_id = str(getattr(event, "group_id", "") or "")
    ok, msg = JsonConfig().set_group_welcome(group_id, enabled=False)
    await handle_send(
        bot,
        event,
        msg if ok else msg,
        md_type="修仙",
        k1="开启欢迎",
        v1="开启进群欢迎",
        k2="修仙帮助",
        v2="修仙帮助",
    )
    await welcome_disable_cmd.finish()
