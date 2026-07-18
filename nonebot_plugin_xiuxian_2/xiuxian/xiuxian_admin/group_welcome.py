"""进群欢迎：成员欢迎 / bot入驻 分开。

Markdown 开关与其它指令一致：
- markdown_status=True：原生 MD 蓝字/按钮（lifecycle 用 event_id 被动发）
- markdown_status=False：纯文本

注意：notice 事件不要先 bot.send（installed adapter 会 Event cannot be replied to!），
直接 send_to_group(event_id=...)。
"""

from __future__ import annotations

from nonebot import on_notice
from nonebot.log import logger
from nonebot.matcher import Matcher
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, MessageSegment, PrivateMessageEvent
from ..on_compat import on_command
from ..qq_compat.lifecycle import apply_lifecycle_event, is_lifecycle_event
from ..xiuxian_config import JsonConfig, XiuConfig
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.message_markdown import strip_md_command_links
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


def _extract_event_id(event) -> str:
    for key in ("event_id", "id"):
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


def _is_bot_leave(event) -> bool:
    name = _event_type_name(event)
    return "GROUP_DEL_ROBOT" in name or "BOT_LEAVE" in name


def _is_group_allowed_for_bot(bot: Bot, group_id: str) -> bool:
    """与全局 response_group/shield_group 白名单一致。

    response_group=True：仅响应 shield_group 列表中的群（白名单）。
    response_group=False：屏蔽 shield_group 列表中的群（黑名单）。
    put_bot 为空时不做 bot 过滤（兼容未配置）。
    """
    gid = str(group_id or "").strip()
    if not gid:
        return False

    cfg = XiuConfig()
    put_bot = [str(x) for x in (getattr(cfg, "put_bot", None) or [])]
    if put_bot and str(getattr(bot, "self_id", "") or "") not in put_bot:
        return False

    shield = {str(x) for x in (getattr(cfg, "shield_group", None) or [])}
    response_group = bool(getattr(cfg, "response_group", False))
    if response_group:
        return gid in shield
    return gid not in shield


def _member_welcome_text() -> str:
    return (XiuConfig().group_welcome_msg or "").strip() or "欢迎道友入群！"


def _bot_join_text() -> str:
    msg = (getattr(XiuConfig(), "group_bot_join_msg", None) or "").strip()
    if msg:
        return msg
    return (
        "必死之境机逢仙缘，修仙之路波澜壮阔！\n"
        "> 发送：\n"
        "【我要修仙】踏入修仙界\n"
        "【修仙帮助】查看玩法\n"
        "【娱乐帮助】查看娱乐功能。"
    )


def _welcome_buttons() -> list[list[tuple[str, str]]]:
    return [
        [("我要修仙", "我要修仙"), ("修仙帮助", "修仙帮助"), ("娱乐帮助", "娱乐帮助")],
        [("关闭欢迎", "关闭进群欢迎")],
    ]


def _welcome_md_text(msg: str) -> str:
    body = (msg or " ").replace("\n", "\r")
    links = (
        "[我要修仙](mqqapi://aio/inlinecmd?command=我要修仙&enter=false&reply=false)"
        " | [修仙帮助](mqqapi://aio/inlinecmd?command=修仙帮助&enter=false&reply=false)"
        " | [娱乐帮助](mqqapi://aio/inlinecmd?command=娱乐帮助&enter=false&reply=false)"
        " | [关闭欢迎](mqqapi://aio/inlinecmd?command=关闭进群欢迎&enter=false&reply=false)"
    )
    return f"{body}\r\r---\r\r{links}"


async def _send_lifecycle_message(bot: Bot, event, group_id: str, message, *, kind: str) -> bool:
    """lifecycle 发送：优先 send_to_group(event_id)，避免 bot.send 对 notice 报 cannot reply。"""
    event_id = _extract_event_id(event)

    # 1) 直接群发 + event_id（被动）—— 成员进群/bot 入群的正确路径
    try:
        send_to_group = getattr(bot, "send_to_group", None)
        if callable(send_to_group):
            kwargs = {"group_openid": group_id, "message": message}
            if event_id:
                kwargs["event_id"] = event_id
            await send_to_group(**kwargs)
            logger.info(f"[{kind}] 已发送 group={group_id} via=send_to_group event_id={bool(event_id)}")
            return True
    except Exception as e:
        logger.warning(f"[{kind}] send_to_group 失败 group={group_id}: {e}")

    # 2) vendor bot.send 可能支持 GroupMemberAddEvent；仅作兜底，失败降为 debug 避免刷警告
    try:
        send = getattr(bot, "send", None)
        if callable(send):
            await send(event, message)
            logger.info(f"[{kind}] 已发送 group={group_id} via=bot.send")
            return True
    except Exception as e:
        # installed adapter 常见：Event cannot be replied to!
        logger.debug(f"[{kind}] bot.send 不可用 group={group_id}: {e}")

    return False


async def _send_group_notice(bot: Bot, event, group_id: str, msg: str, *, kind: str) -> None:
    if not msg or not group_id:
        return
    try:
        bot, _ = await assign_bot(bot=bot, event=event)
    except Exception:
        pass

    cfg = XiuConfig()
    md_on = bool(getattr(cfg, "markdown_status", False))
    plain = strip_md_command_links(msg)

    if md_on:
        try:
            md_body = msg.replace("\n", "\r")
            if bool(getattr(cfg, "markdown_button_status", False)):
                message = MessageSegment.markdown_keyboard(bot, md_body, _welcome_buttons())
            else:
                message = MessageSegment.markdown(bot, _welcome_md_text(msg))
            if await _send_lifecycle_message(bot, event, group_id, message, kind=f"{kind}/md"):
                return
        except Exception as e:
            logger.warning(f"[{kind}] 构造/发送 MD 失败 group={group_id}: {e}")

        # handle_send 对 lifecycle 可能变主动发；仅在上面失败时尝试一次
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
                k3="娱乐帮助",
                v3="娱乐帮助",
                k4="关闭欢迎",
                v4="关闭进群欢迎",
                at_msg=False,
            )
            logger.info(f"[{kind}] 已发送 group={group_id} via=handle_send(md)")
            return
        except Exception as e:
            logger.warning(f"[{kind}] handle_send(md) 失败 group={group_id}: {e}")

    if await _send_lifecycle_message(bot, event, group_id, plain, kind=f"{kind}/plain"):
        return
    try:
        await handle_send(bot, event, plain, at_msg=False)
        logger.info(f"[{kind}] 已发送 group={group_id} via=handle_send(plain)")
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
        elif _is_bot_leave(event):
            action = "bot_leave_group"
        elif _is_member_join(event):
            action = "member_join_group"

        if not gid:
            return

        # bot 退群：全量取消标记不受欢迎开关/白名单影响
        if action == "bot_leave_group":
            if conf.unmark_full_message_group(gid):
                logger.info(f"[全量群] bot退群取消标记 group={gid}")
            return

        # 仅响应群 / 屏蔽群：欢迎也必须遵守，否则白名单外群仍会欢迎
        if not _is_group_allowed_for_bot(bot, gid):
            logger.debug(f"[进群欢迎] 非响应群，已忽略 group={gid} action={action}")
            return

        if not conf.is_group_welcome_enabled(gid):
            return

        if action == "bot_join_group":
            logger.info(f"[Bot入驻] group={gid} event={event_name}")
            await _send_group_notice(bot, event, gid, _bot_join_text(), kind="Bot入驻")
            await matcher.finish()

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
