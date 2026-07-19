from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
import time

from ..adapter_compat import Bot, GroupMessageEvent, Message, MessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import check_user, handle_send, send_help_message

from .service import (
    build_activity_gameplay_text,
    build_activity_info,
    build_activity_pass_text,
    build_activity_task_progress_text,
    build_activity_rewards_text,
    build_activity_tasks_text,
    build_collect_bag_text,
    build_activity_points_text,
    build_activity_shop_text,
    build_rank_text,
    claim_activity_rewards,
    claim_activity_pass_rewards,
    claim_activity_tasks,
    claim_collect_phrase,
    claim_point_shop_item,
    claim_sign,
    set_enabled,
)


activity_help_cmd = on_command("活动帮助", priority=7, block=True)
activity_manage_cmd = on_command("活动管理", permission=SUPERUSER, priority=5, block=True)
activity_info_cmd = on_command("活动", aliases={"活动信息", "活动进度", "活动日程"}, priority=10, block=True)
activity_claim_cmd = on_command("活动领取", aliases={"活动一键领取", "领取活动奖励", "活动领奖"}, priority=10, block=True)
activity_rewards_cmd = on_command("活动奖励", priority=10, block=True)
activity_tasks_cmd = on_command("活动任务", aliases={"活动目标", "活动日常"}, priority=10, block=True)
activity_task_claim_cmd = on_command("活动任务领取", aliases={"领取活动任务"}, priority=10, block=True)
activity_pass_cmd = on_command("活动战令", aliases={"活动通行证", "活动活跃"}, priority=10, block=True)
activity_pass_claim_cmd = on_command("活动战令领取", aliases={"领取活动战令", "领取活动通行证"}, priority=10, block=True)
activity_gameplay_cmd = on_command("活动玩法", priority=10, block=True)
activity_sign_cmd = on_command("活动签到", aliases={"节日签到"}, priority=10, block=True)
activity_rank_cmd = on_command("活动排行", priority=10, block=True)
activity_bag_cmd = on_command("活动背包", aliases={"活动字牌", "集字背包"}, priority=10, block=True)
activity_exchange_cmd = on_command("活动兑换", aliases={"集字兑换"}, priority=10, block=True)
activity_points_cmd = on_command("活动积分", aliases={"积分活动"}, priority=10, block=True)
activity_shop_cmd = on_command("活动商店", aliases={"积分商店"}, priority=10, block=True)
activity_buy_cmd = on_command("活动购买", aliases={"活动兑换商品", "活动商城兑换"}, priority=10, block=True)
activity_boss_cmd = on_command("活动首领", aliases={"活动BOSS", "活动Boss"}, priority=10, block=True)
activity_boss_rank_cmd = on_command("活动首领排行", aliases={"活动BOSS排行", "首领伤害榜"}, priority=10, block=True)
activity_boss_atk_cmd = on_command("活动讨伐", aliases={"活动首领攻击", "使用爆竹", "使用烟花"}, priority=10, block=True)
activity_boss_claim_cmd = on_command("活动首领领奖", aliases={"领取首领奖励", "首领领奖"}, priority=10, block=True)

activity_open_cmd = on_command("开启活动", permission=SUPERUSER, priority=5, block=True)
activity_close_cmd = on_command("关闭活动", permission=SUPERUSER, priority=5, block=True)


async def _ensure_user(event) -> tuple[bool, dict | None, str]:
    is_user, user_info, msg = check_user(event)
    return is_user, user_info, msg


def _activity_operation_id(event, action: str, user_id: str) -> str:
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    return f"activity:{action}:{user_id}:{event_id or time.time_ns()}"


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


@activity_rewards_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    text = build_activity_rewards_text()
    await handle_send(
        bot,
        event,
        text,
        native_markdown=True,
        fallback_msg=text,
    )


@activity_claim_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    ok, text = claim_activity_rewards(
        user_id,
        _activity_operation_id(event, "claim-all", user_id),
    )
    await handle_send(bot, event, text if ok else f"活动领取失败：{text}")


@activity_tasks_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if is_user:
        text = build_activity_task_progress_text(str(user_info["user_id"]))
    else:
        text = build_activity_tasks_text()
    await handle_send(
        bot,
        event,
        text,
        native_markdown=True,
        fallback_msg=text,
    )


@activity_task_claim_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    ok, text = claim_activity_tasks(
        user_id,
        args.extract_plain_text(),
        _activity_operation_id(event, "task-claim", user_id),
    )
    await handle_send(bot, event, text if ok else f"活动任务领取失败：{text}")


@activity_pass_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    text = build_activity_pass_text(str(user_info["user_id"]))
    await handle_send(bot, event, text)


@activity_pass_claim_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    ok, text = claim_activity_pass_rewards(
        user_id,
        args.extract_plain_text(),
        _activity_operation_id(event, "pass-claim", user_id),
    )
    await handle_send(bot, event, text if ok else f"活动战令领取失败：{text}")


@activity_gameplay_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    text = build_activity_gameplay_text()
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

    user_id = str(user_info["user_id"])
    ok, text = claim_sign(user_id, _activity_operation_id(event, "sign", user_id))
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

    user_id = str(user_info["user_id"])
    ok, text = claim_collect_phrase(
        user_id,
        args.extract_plain_text(),
        _activity_operation_id(event, "collect-exchange", user_id),
    )
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

    user_id = str(user_info["user_id"])
    ok, text = claim_point_shop_item(
        user_id, args.extract_plain_text(),
        _activity_operation_id(event, "point-shop", user_id),
    )
    await handle_send(bot, event, text if ok else f"活动购买失败：{text}")


@activity_boss_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    from .activity_boss import build_boss_status_text

    await handle_send(bot, event, build_boss_status_text(str(user_info["user_id"])))


@activity_boss_rank_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await assign_bot(bot=bot, event=event)
    from .activity_boss import build_boss_rank_text

    await handle_send(bot, event, build_boss_rank_text("", 15))


@activity_boss_atk_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    from .activity_boss import fight_cooperative_boss, use_item_on_boss

    raw = args.extract_plain_text().strip()
    uid = str(user_info["user_id"])
    operation_id = _activity_operation_id(event, "boss-item" if raw else "boss-coop", uid)
    if raw:
        ok, text = use_item_on_boss(uid, raw, operation_id)
        await handle_send(bot, event, text)
        return
    ok, text = fight_cooperative_boss(uid, operation_id=operation_id)
    await handle_send(bot, event, text)


@activity_boss_claim_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = await _ensure_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    from .activity_boss import claim_boss_rewards

    ok, text = claim_boss_rewards(str(user_info["user_id"]), args.extract_plain_text())
    await handle_send(bot, event, text if ok else text)


@activity_open_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    operator_id = str(event.get_user_id())
    text = set_enabled(
        True,
        args.extract_plain_text(),
        operation_id=_activity_operation_id(event, "config-open", operator_id),
        operator_id=operator_id,
    )
    await handle_send(bot, event, text)


@activity_close_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: MessageEvent, args: Message = CommandArg()):
    await assign_bot(bot=bot, event=event)
    operator_id = str(event.get_user_id())
    text = set_enabled(
        False,
        args.extract_plain_text(),
        operation_id=_activity_operation_id(event, "config-close", operator_id),
        operator_id=operator_id,
    )
    await handle_send(bot, event, text)


ACTIVITY_HELP = """
**活动帮助**
---
**活动指令**
- 活动 / 活动信息
> 查看概览
- 活动奖励
> 查看活动奖励
- 活动领取
> 任务、战令、首领奖励一键领取
- 活动任务 / 活动任务领取
- 活动玩法
- 活动签到
- 活动背包 / 活动字牌
- 活动兑换 端午安康
- 活动积分
- 活动商店
- 活动购买 灵石补给
- 活动战令 / 活动战令领取
- 活动排行
- 活动首领 / 活动首领排行
- 活动讨伐
> 协作首领直接打，或使用 活动讨伐 爆竹/烟花
- 活动首领领奖 排行 / 进度
""".strip()


ACTIVITY_MANAGE_HELP = """
**活动管理**
---
**管理指令**
- 开启活动
- 关闭活动
- 开启活动 集字
- 关闭活动 集字
- 开启活动 积分
- 关闭活动 积分
- 开启活动 首领 / 关闭活动 首领
- 开启活动 战令 / 关闭活动 战令
- 开启活动 全部 / 关闭活动 全部
- 活动后台可维护模板、任务、玩法和奖励
""".strip()
