from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, Message, MessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import check_user, handle_send, send_help_message

from .service import (
    build_activity_info,
    build_collect_bag_text,
    build_activity_points_text,
    build_activity_shop_text,
    build_rank_text,
    claim_collect_phrase,
    claim_point_shop_item,
    claim_sign,
    set_enabled,
)


activity_help_cmd = on_command("活动帮助", priority=7, block=True)
activity_manage_cmd = on_command("活动管理", permission=SUPERUSER, priority=5, block=True)
activity_info_cmd = on_command("活动", aliases={"活动信息"}, priority=10, block=True)
activity_sign_cmd = on_command("活动签到", aliases={"节日签到"}, priority=10, block=True)
activity_rank_cmd = on_command("活动排行", priority=10, block=True)
activity_bag_cmd = on_command("活动背包", aliases={"活动字牌", "集字背包"}, priority=10, block=True)
activity_exchange_cmd = on_command("活动兑换", aliases={"集字兑换"}, priority=10, block=True)
activity_points_cmd = on_command("活动积分", aliases={"积分活动"}, priority=10, block=True)
activity_shop_cmd = on_command("活动商店", aliases={"积分商店"}, priority=10, block=True)
activity_buy_cmd = on_command("活动购买", aliases={"活动兑换商品", "活动商城兑换"}, priority=10, block=True)

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
        k3="背包",
        v3="活动背包",
    )


@activity_manage_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent):
    await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot,
        event,
        ACTIVITY_MANAGE_HELP,
        k1="开启",
        v1="开启活动 集字",
        k2="关闭",
        v2="关闭活动 集字",
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


@activity_bag_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    text = build_collect_bag_text(str(user_info["user_id"]))
    await handle_send(bot, event, text)


@activity_exchange_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    ok, text = claim_collect_phrase(str(user_info["user_id"]), args.extract_plain_text())
    await handle_send(bot, event, text if ok else f"活动兑换失败：{text}")


@activity_points_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    text = build_activity_points_text(str(user_info["user_id"]))
    await handle_send(bot, event, text)


@activity_shop_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    text = build_activity_shop_text(str(user_info["user_id"]))
    await handle_send(bot, event, text)


@activity_buy_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    ok, text = claim_point_shop_item(str(user_info["user_id"]), args.extract_plain_text())
    await handle_send(bot, event, text if ok else f"活动购买失败：{text}")


@activity_open_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    text = set_enabled(True, args.extract_plain_text())
    await handle_send(bot, event, text)


@activity_close_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    text = set_enabled(False, args.extract_plain_text())
    await handle_send(bot, event, text)


ACTIVITY_HELP = """
活动帮助
═════════════
【活动指令】
1. 活动 / 活动信息
2. 活动签到
3. 活动背包 / 活动字牌
4. 活动兑换 端午安康
5. 活动积分
6. 活动商店
7. 活动购买 灵石补给
8. 活动排行
""".strip()


ACTIVITY_MANAGE_HELP = """
活动管理
═════════════
【管理指令】
1. 开启活动
2. 关闭活动
3. 开启活动 集字
4. 关闭活动 集字
5. 开启活动 积分
6. 关闭活动 积分
7. 开启活动 全部 / 关闭活动 全部
8. 活动后台可维护模板、任务、玩法和奖励
""".strip()
