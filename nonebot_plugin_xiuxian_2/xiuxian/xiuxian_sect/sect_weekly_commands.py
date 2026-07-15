import time

from nonebot.params import CommandArg

from ...paths import get_paths
from ..adapter_compat import Bot, GroupMessageEvent, Message, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import check_user, handle_send, number_to
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .sect_weekly import sect_weekly_goal_manager
from .transaction_service import SectWeeklyRewardClaimService


items = Items()
sql_message = XiuxianDateManage()
sect_weekly_reward_service = SectWeeklyRewardClaimService(
    get_paths().game_db,
    get_paths().player_db,
    sql_message.lock,
)

sect_weekly = on_command("宗门周常", priority=7, block=True)
sect_weekly_claim = on_command("领取宗门周常", priority=7, block=True)
sect_weekly_rank = on_command("宗门周常排行", priority=7, block=True)


def _format_sect_weekly_reward(reward: dict) -> str:
    parts = []
    for item in reward.get("items", []) or []:
        item_info = items.get_data_by_item_id(item.get("id") or item.get("goods_id"))
        item_name = item_info["name"] if item_info else str(item.get("id") or item.get("goods_id"))
        parts.append(f"{item_name}x{int(item.get('amount', item.get('num', 1)) or 1)}")
    if int(reward.get("stone", 0) or 0) > 0:
        parts.append(f"灵石{number_to(reward['stone'])}")
    if int(reward.get("exp", 0) or 0) > 0:
        parts.append(f"修为{number_to(reward['exp'])}")
    if int(reward.get("sect_contribution", 0) or 0) > 0:
        parts.append(f"宗门贡献{number_to(reward['sect_contribution'])}")
    if int(reward.get("sect_scale", 0) or 0) > 0:
        parts.append(f"宗门建设度{number_to(reward['sect_scale'])}")
    if int(reward.get("sect_materials", 0) or 0) > 0:
        parts.append(f"宗门资材{number_to(reward['sect_materials'])}")
    if int(reward.get("boss_integral", 0) or 0) > 0:
        parts.append(f"BOSS积分{number_to(reward['boss_integral'])}")
    return "、".join(parts) if parts else "无"


def _build_sect_weekly_status(goal: dict, user_id: str) -> str:
    if str(user_id) in goal.get("claimed_users", []):
        return "已领取"
    if goal.get("completed"):
        return "可领取"
    return "进行中"


def _sect_weekly_operation_id(event, user_id: str, goal_key: str) -> str:
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"sect-weekly:{event_id}:{user_id}:{goal_key}"
    return f"sect-weekly:{user_id}:{goal_key}:{time.time_ns()}"


def _prepare_claim_goals(goals: list[dict]) -> list[dict]:
    prepared = []
    for goal in goals:
        reward = dict(goal["rewards"])
        reward_items = []
        for item in reward.get("items", []) or []:
            item_id = int(item.get("id") or item.get("goods_id"))
            item_info = items.get_data_by_item_id(item_id)
            if not item_info:
                raise ValueError(f"宗门周常奖励物品不存在：{item_id}")
            reward_items.append(
                {
                    **item,
                    "id": item_id,
                    "name": item_info["name"],
                    "type": item_info["type"],
                }
            )
        reward["items"] = reward_items
        prepared.append({**goal, "rewards": reward})
    return prepared


@sect_weekly.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_weekly_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门周常目标"""
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_weekly.finish()

    user_id = str(user_info["user_id"])
    sect_id = user_info.get("sect_id")
    if not sect_id:
        await handle_send(
            bot,
            event,
            "道友尚未加入宗门，无法查看宗门周常。",
            md_type="宗门",
            k1="加入",
            v1="宗门加入",
            k2="列表",
            v2="宗门列表",
            k3="帮助",
            v3="宗门帮助",
        )
        await sect_weekly.finish()

    goals = sect_weekly_goal_manager.list_goals(sect_id)
    week_key = sect_weekly_goal_manager.current_week_key()
    sect_info = sql_message.get_sect_info(sect_id) or {}
    lines = [
        "【宗门周常】",
        f"宗门：{sect_info.get('sect_name', sect_id)}",
        f"周期：{week_key}",
        "",
        "本周目标：",
    ]
    for goal in goals:
        status = _build_sect_weekly_status(goal, user_id)
        lines.extend(
            [
                f"{goal['name']}（{status}）",
                f"进度：{number_to(goal['progress'])}/{number_to(goal['target'])}",
                f"内容：{goal['desc']}",
                f"奖励：{_format_sect_weekly_reward(goal['rewards'])}",
                "",
            ]
        )
    lines.append("可执行操作：领取宗门周常、宗门周常排行、宗门建设")
    await handle_send(
        bot,
        event,
        "\n".join(lines).strip(),
        md_type="宗门",
        k1="领取",
        v1="领取宗门周常",
        k2="排行",
        v2="宗门周常排行",
        k3="建设",
        v3="宗门建设",
    )
    await sect_weekly.finish()


@sect_weekly_claim.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_weekly_claim_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    """领取宗门周常奖励"""
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_weekly_claim.finish()

    user_id = str(user_info["user_id"])
    sect_id = user_info.get("sect_id")
    if not sect_id:
        await handle_send(
            bot,
            event,
            "道友尚未加入宗门，无法领取宗门周常。",
            md_type="宗门",
            k1="加入",
            v1="宗门加入",
            k2="列表",
            v2="宗门列表",
            k3="帮助",
            v3="宗门帮助",
        )
        await sect_weekly_claim.finish()

    arg = args.extract_plain_text().strip()
    goals = sect_weekly_goal_manager.list_goals(sect_id)
    if arg:
        goal_key = sect_weekly_goal_manager.resolve_goal_key(arg)
        if not goal_key:
            await handle_send(
                bot,
                event,
                "未找到对应宗门周常目标，请发送【宗门周常】查看目标名称。",
                md_type="宗门",
                k1="周常",
                v1="宗门周常",
                k2="排行",
                v2="宗门周常排行",
                k3="建设",
                v3="宗门建设",
            )
            await sect_weekly_claim.finish()
        goals = [goal for goal in goals if goal["key"] == goal_key]

    claimable = [
        goal
        for goal in goals
        if goal.get("completed") and user_id not in goal.get("claimed_users", [])
    ]
    if not claimable:
        msg = "暂无可领取的宗门周常奖励。"
        if arg:
            msg = "该宗门周常目标尚未完成或已经领取。"
        await handle_send(
            bot,
            event,
            msg,
            md_type="宗门",
            k1="周常",
            v1="宗门周常",
            k2="排行",
            v2="宗门周常排行",
            k3="建设",
            v3="宗门建设",
        )
        await sect_weekly_claim.finish()

    week_key = sect_weekly_goal_manager.current_week_key()
    goal_scope = claimable[0]["key"] if len(claimable) == 1 else "all"
    result = sect_weekly_reward_service.claim(
        _sect_weekly_operation_id(event, user_id, goal_scope),
        user_id,
        sect_id,
        week_key,
        _prepare_claim_goals(claimable),
        XiuConfig().max_goods_num,
    )
    status_messages = {
        "not_completed": "宗门周常目标尚未完成或状态已变化。",
        "already_claimed": "宗门周常奖励已经领取。",
        "inventory_full": "背包物品已达上限，宗门周常奖励尚未领取。",
        "sect_changed": "宗门归属已变化，宗门周常奖励尚未领取。",
        "sect_missing": "宗门状态异常，宗门周常奖励尚未领取。",
        "user_missing": "未找到道友数据，宗门周常奖励尚未领取。",
        "operation_conflict": "领取请求状态冲突，请重新发送命令。",
    }
    if result.succeeded:
        msg = "领取成功：\n" + "\n".join(
            f"{name}：{reward_text}" for name, reward_text in result.rewards
        )
    else:
        msg = status_messages.get(result.status, "宗门周常奖励领取失败，请稍后重试。")
    await handle_send(
        bot,
        event,
        msg,
        md_type="宗门",
        k1="周常",
        v1="宗门周常",
        k2="排行",
        v2="宗门周常排行",
        k3="建设",
        v3="宗门建设",
    )
    await sect_weekly_claim.finish()


@sect_weekly_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_weekly_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门周常排行"""
    bot, _ = await assign_bot(bot=bot, event=event)
    rows = sect_weekly_goal_manager.weekly_rank(limit=10)
    week_key = sect_weekly_goal_manager.current_week_key()
    if not rows:
        msg = f"【宗门周常排行】\n周期：{week_key}\n暂无宗门周常进度。"
    else:
        lines = ["【宗门周常排行】", f"周期：{week_key}", ""]
        for idx, row in enumerate(rows, start=1):
            total_progress = int(row.get("total_progress", 0) or 0)
            lines.append(f"{idx}. {row.get('sect_name') or row.get('sect_id')}：{number_to(total_progress)}")
        msg = "\n".join(lines)
    await handle_send(
        bot,
        event,
        msg,
        md_type="宗门",
        k1="周常",
        v1="宗门周常",
        k2="领取",
        v2="领取宗门周常",
        k3="建设",
        v3="宗门建设",
    )
    await sect_weekly_rank.finish()
