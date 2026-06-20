from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, MessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import check_user, handle_send, send_help_message

from .service import build_activity_info, build_rank_text, claim_sign, set_enabled


activity_help_cmd = on_command("活动帮助", priority=7, block=True)
activity_info_cmd = on_command("活动", aliases={"活动信息"}, priority=10, block=True)
activity_sign_cmd = on_command("活动签到", aliases={"节日签到"}, priority=10, block=True)
activity_rank_cmd = on_command("活动排行", priority=10, block=True)

activity_open_cmd = on_command("开启活动", permission=SUPERUSER, priority=5, block=True)
activity_close_cmd = on_command("关闭活动", permission=SUPERUSER, priority=5, block=True)


async def _ensure_user(event) -> tuple[bool, dict | None, str]:
    is_user, user_info, msg = check_user(event)
    return is_user, user_info, msg


@activity_help_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot,
        event,
        ACTIVITY_HELP,
        k1="信息",
        v1="活动",
        k2="签到",
        v2="活动签到",
        k3="排行",
        v3="活动排行",
    )


@activity_info_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    text = build_activity_info(str(user_info["user_id"]))
    await handle_send(
        bot,
        event,
        text,
        native_markdown=True,
        fallback_msg=text,
    )


@activity_sign_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    ok, text = claim_sign(str(user_info["user_id"]))
    await handle_send(bot, event, text if ok else f"活动签到失败：{text}")


@activity_rank_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, build_rank_text(10))


@activity_open_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    set_enabled(True)
    await handle_send(bot, event, "已开启活动")


@activity_close_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    set_enabled(False)
    await handle_send(bot, event, "已关闭活动")


ACTIVITY_HELP = """
节日签到活动帮助
═════════════
【用户命令】
1. 活动 / 活动信息
2. 活动签到
3. 活动排行

【管理员命令】
1. 开启活动
2. 关闭活动

活动配置文件：
data/xiuxian/activity/activity_config.json
""".strip()
