"""进群欢迎：成员欢迎 / bot入驻 分开。

Markdown 开关与其它指令一致：
- markdown_status=True：原生 MD 蓝字/按钮（lifecycle 用 event_id 被动发）
- markdown_status=False：纯文本

注意：notice 事件不要先 bot.send（installed adapter 会 Event cannot be replied to!），
直接 send_to_group(event_id=...)。
"""

from __future__ import annotations


from nonebot.log import logger
from nonebot.matcher import Matcher
from ...features.admin.config_application import AdminConfigApplication
from ...features.group_lifecycle.notice_application import GroupLifecycleNoticeApplication

from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    is_group_admin_or_owner,
)
from ..on_compat import on_command, on_notice
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import handle_send

admin_config_application = AdminConfigApplication()
group_lifecycle_notice_application = GroupLifecycleNoticeApplication(admin_config_application)


lifecycle_notice = on_notice(priority=5, block=False)


@lifecycle_notice.handle()
async def handle_group_lifecycle(bot: Bot, event, matcher: Matcher):
    try:
        decision = await group_lifecycle_notice_application.handle(bot, event)
        if decision.finish_matcher:
            await matcher.finish()
    except Exception as e:
        from nonebot.exception import FinishedException

        if isinstance(e, FinishedException):
            raise
        logger.debug(f"[进群欢迎/生命周期] 处理失败: {e}")


# ---------- 开关指令 ----------
# 超管 / 群主 / 管理员 均可本群开关（权限在 handle 内判定，兼容 QQ member_role）
welcome_enable_cmd = on_command(
    "开启进群欢迎",
    aliases={"启用进群欢迎", "打开进群欢迎"},
    priority=5,
    block=True,
)
welcome_disable_cmd = on_command(
    "关闭进群欢迎",
    aliases={"禁用进群欢迎", "关掉进群欢迎"},
    priority=5,
    block=True,
)


def _can_toggle_welcome(bot: Bot, event) -> bool:
    try:
        uid = str(event.get_user_id())
    except Exception:
        uid = str(getattr(event, "user_id", "") or "")
    superusers = set(getattr(getattr(bot, "config", None), "superusers", None) or ())
    if uid and uid in superusers:
        return True
    return is_group_admin_or_owner(event)


@welcome_enable_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def welcome_enable_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    if isinstance(event, PrivateMessageEvent):
        await handle_send(bot, event, "请在群内使用：开启进群欢迎")
        await welcome_enable_cmd.finish()
    if not _can_toggle_welcome(bot, event):
        await handle_send(bot, event, "仅群主、管理员或超管可开关本群进群欢迎")
        await welcome_enable_cmd.finish()
    group_id = str(getattr(event, "group_id", "") or getattr(event, "group_openid", "") or "")
    ok, msg = admin_config_application.set_group_welcome(
        group_id, enabled=True, globally_enabled=XiuConfig().group_welcome,
    )
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
    if not _can_toggle_welcome(bot, event):
        await handle_send(bot, event, "仅群主、管理员或超管可开关本群进群欢迎")
        await welcome_disable_cmd.finish()
    group_id = str(getattr(event, "group_id", "") or getattr(event, "group_openid", "") or "")
    ok, msg = admin_config_application.set_group_welcome(
        group_id, enabled=False, globally_enabled=XiuConfig().group_welcome,
    )
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
