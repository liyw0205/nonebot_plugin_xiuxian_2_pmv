"""进群欢迎：成员欢迎 / bot入驻 分开；全量群标记不在此处理。"""

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


def _is_bot_join(event) -> bool:
    name = _event_type_name(event)
    return "GROUP_ADD_ROBOT" in name or "BOT_JOIN" in name


def _member_welcome_text() -> str:
    return (XiuConfig().group_welcome_msg or "").strip() or "欢迎道友入群！"


def _bot_join_text() -> str:
    msg = (getattr(XiuConfig(), "group_bot_join_msg", None) or "").strip()
    if msg:
        return msg
    # 未配置时也不复用成员欢迎文案，避免“同一段话”
    return (
        "修仙之路波澜壮阔，修仙助手到了。\n"
        "先发【我要修仙】入门，不会玩就【修仙帮助】/【娱乐帮助】。"
    )


async def _send_group_notice(bot: Bot, event, group_id: str, msg: str, *, kind: str) -> None:
    if not msg or not group_id:
        return
    try:
        bot, _ = await assign_bot(bot=bot, event=event)
    except Exception:
        pass

    # lifecycle notice：优先 bot.send（带 event_id）
    try:
        send = getattr(bot, "send", None)
        if callable(send):
            await send(event, msg)
            logger.info(f"[{kind}] 已发送 group={group_id} via=bot.send")
            return
    except Exception as e:
        logger.warning(f"[{kind}] bot.send 失败 group={group_id}: {e}")

    try:
        send_to_group = getattr(bot, "send_to_group", None)
        if callable(send_to_group):
            event_id = getattr(event, "event_id", None) or getattr(event, "id", None)
            kwargs = {"group_openid": group_id, "message": msg}
            if event_id:
                kwargs["event_id"] = event_id
            await send_to_group(**kwargs)
            logger.info(f"[{kind}] 已发送 group={group_id} via=send_to_group")
            return
    except Exception as e:
        logger.warning(f"[{kind}] send_to_group 失败 group={group_id}: {e}")

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
        logger.info(f"[{kind}] 已发送 group={group_id} via=handle_send")
    except Exception as e:
        logger.warning(f"[{kind}] 发送失败 group={group_id}: {e}")


lifecycle_notice = on_notice(priority=5, block=False)


@lifecycle_notice.handle()
async def handle_group_lifecycle(bot: Bot, event, matcher: Matcher):
    conf = JsonConfig()
    group_id = _extract_group_id(event)
    event_name = _event_type_name(event)

    try:
        action = ""
        gid = group_id
        if is_lifecycle_event(event):
            result = apply_lifecycle_event(bot, event)
            action = result.context.action
            gid = result.context.group_id or group_id
        elif _is_bot_join(event):
            action = "bot_join_group"
        elif _is_member_join(event):
            action = "member_join_group"

        if not gid or not conf.is_group_welcome_enabled(gid):
            return

        # bot 入群：单独文案（入驻提示）
        if action == "bot_join_group":
            logger.info(f"[Bot入驻] group={gid} event={event_name}")
            await _send_group_notice(bot, event, gid, _bot_join_text(), kind="Bot入驻")
            await matcher.finish()

        # 成员入群：欢迎文案（需要 intent.group_members）
        if action == "member_join_group":
            logger.info(f"[成员欢迎] group={gid} event={event_name}")
            await _send_group_notice(bot, event, gid, _member_welcome_text(), kind="成员欢迎")
            await matcher.finish()
    except Exception as e:
        from nonebot.exception import FinishedException

        if isinstance(e, FinishedException):
            raise
        logger.debug(f"[进群欢迎/生命周期] 处理失败: {e}")


# ---------- 开关指令 ----------
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
    group_id = str(getattr(event, "group_id", "") or getattr(event, "group_openid", "") or "")
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
    group_id = str(getattr(event, "group_id", "") or getattr(event, "group_openid", "") or "")
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
