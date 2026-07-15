from nonebot.params import CommandArg
import time

from ..adapter_compat import Bot, GroupMessageEvent, Message, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import check_user, handle_send
from .task_data import task_manager


task_info = on_command("我的任务", aliases={"修仙任务", "任务列表"}, priority=6, block=True)
daily_task = on_command("每日任务", aliases={"今日任务"}, priority=6, block=True)
weekly_task = on_command("周常任务", aliases={"每周任务"}, priority=6, block=True)
claim_task = on_command("领取任务奖励", aliases={"任务奖励", "领取每日任务奖励", "领取周常任务奖励"}, priority=6, block=True)


def _parse_cycle(text: str) -> str | None:
    text = (text or "").strip()
    if any(key in text for key in ("每日", "日常", "今日")):
        return "daily"
    if any(key in text for key in ("周常", "每周", "本周")):
        return "weekly"
    return None


@task_info.handle(parameterless=[Cooldown(cd_time=0)])
async def task_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await task_info.finish()

    msg = task_manager.build_status_message(user_info["user_id"])
    await handle_send(
        bot,
        event,
        msg,
        md_type="修仙",
        k1="领取",
        v1="领取任务奖励",
        k2="每日",
        v2="每日任务",
        k3="周常",
        v3="周常任务",
    )
    await task_info.finish()


@daily_task.handle(parameterless=[Cooldown(cd_time=0)])
async def daily_task_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await daily_task.finish()

    msg = task_manager.build_status_message(user_info["user_id"], "daily")
    await handle_send(
        bot,
        event,
        msg,
        md_type="修仙",
        k1="领取",
        v1="领取任务奖励 每日",
        k2="全部",
        v2="我的任务",
        k3="签到",
        v3="修仙签到",
    )
    await daily_task.finish()


@weekly_task.handle(parameterless=[Cooldown(cd_time=0)])
async def weekly_task_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await weekly_task.finish()

    msg = task_manager.build_status_message(user_info["user_id"], "weekly")
    await handle_send(
        bot,
        event,
        msg,
        md_type="修仙",
        k1="领取",
        v1="领取任务奖励 周常",
        k2="全部",
        v2="我的任务",
        k3="BOSS",
        v3="查询世界BOSS",
    )
    await weekly_task.finish()


@claim_task.handle(parameterless=[Cooldown(cd_time=0)])
async def claim_task_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await claim_task.finish()

    arg_text = args.extract_plain_text()
    cycle = _parse_cycle(arg_text) or _parse_cycle(str(event.message))
    user_id = str(user_info["user_id"])
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"task-reward-claim:{user_id}:{event_id or time.time_ns()}"
    # 先回放：成功后 claimed 会变成“无可领”。
    prior = task_manager.reward_claim_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        if prior.tasks:
            lines = ["任务奖励领取成功："]
            for task in prior.tasks:
                reward_parts = []
                for item in task.get("items", ()):
                    reward_parts.append(f"{item['name']}x{item['amount']}")
                lines.append(
                    f"- {task['name']}：{'、'.join(reward_parts) if reward_parts else '无'}"
                )
            msg = "\n".join(lines) + "\n该领奖请求已经处理，无需重复提交。"
        else:
            msg = "当前没有可领取的任务奖励。\n该领奖请求已经处理，无需重复提交。"
    else:
        try:
            msg = task_manager.claim_rewards(operation_id, user_id, cycle)
        except Exception:
            msg = "任务奖励领取失败，请稍后重试。"
    await handle_send(
        bot,
        event,
        msg,
        md_type="修仙",
        k1="任务",
        v1="我的任务",
        k2="每日",
        v2="每日任务",
        k3="周常",
        v3="周常任务",
    )
    await claim_task.finish()
