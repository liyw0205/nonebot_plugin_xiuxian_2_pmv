import asyncio
import random
import time
from datetime import datetime, timedelta
from fractions import Fraction
from typing import Union, Any

from nonebot import require
from ..on_compat import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.log import logger

from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    Message,
    MessageSegment,
    get_at_user_id,
)
from ..messaging.delivery import delivery_service

from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, leave_harm_time, OtherSet, PlayerDataManager
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, number_to, check_user_type, _impersonating_users, send_help_message
from ..xiuxian_utils.player_fight import pve_fight, resolve_final_user_statuses
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata

from .dungeon_manager import DungeonManager
from .team_manager import (
    get_user_team, get_team_info, expire_team_invite,
)
from .team_command_service import (
    TeamInviteResponseResult,
    build_invite_response_message,
    build_team_view,
    build_team_view_message,
    build_transfer_team_not_member_message,
    build_transfer_team_self_message,
    build_transfer_team_success_message,
    build_team_invite_message,
    build_team_invite_private_message,
)
from .session_service import DungeonSessionService
from .purchase_service import DungeonPurchaseService
from .explore_operation_service import DungeonExploreOperationService
from .team_transaction_service import (
    DungeonTeamTransactionService,
    TeamExitResult,
    TeamMutationResult,
    TeamStateSnapshot,
)
from .team_exit_service import DungeonTeamExitService
from ...paths import get_paths

sql_message = XiuxianDateManage()
player_data = PlayerDataManager()
items = Items()
dungeon_session_service = DungeonSessionService(get_paths().player_db)
dungeon_purchase_service = DungeonPurchaseService(get_paths().game_db)
dungeon_explore_operation_service = DungeonExploreOperationService(
    get_paths().game_db, get_paths().player_db
)
dungeon_team_transaction_service = DungeonTeamTransactionService(get_paths().player_db)
dungeon_team_exit_service = DungeonTeamExitService(get_paths().player_db)
DUNGEON_SHOP = {
    1999: {"name": "渡厄丹", "cost": 100000},
    20012: {"name": "秘境加速券", "cost": 500000},
    20004: {"name": "蕴灵石", "cost": 1000000},
}

# 统一单例 DungeonManager
dungeon_manager = DungeonManager()

# =========================
# 组队冷却配置
# =========================
TEAM_JOIN_CD_HOURS = 3


def _now_dt():
    return datetime.now()


def _team_cooldown_until(hours: int = TEAM_JOIN_CD_HOURS) -> str:
    return (_now_dt() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")


def _team_operation_id(event, action: str, user_id: str) -> str:
    event_id = getattr(event, "message_id", None)
    if event_id:
        return f"dungeon-team-{action}:{event_id}:{user_id}"
    return f"dungeon-team-{action}:{time.time_ns()}:{user_id}"


def format_seconds(sec: int):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h}小时{m}分{s}秒"
    if m > 0:
        return f"{m}分{s}秒"
    return f"{s}秒"


def _missing_team_snapshot(team_id: str = "") -> TeamStateSnapshot:
    return TeamStateSnapshot(str(team_id), "", "", (), (), (), 0)


def _team_mutation_message(action: str, result: TeamMutationResult) -> str:
    target_info = sql_message.get_user_info_with_id(result.target_id) or {}
    target_name = target_info.get("user_name", result.target_id or "指定用户")
    if action == "create":
        if result.status in {"applied", "duplicate"}:
            return (
                f"🎉 队伍【{result.team_name}】创建成功！\n队伍ID：{result.team_id}\n"
                f"📢 使用【邀请组队 道号】来邀请其他人加入！"
            )
        messages = {
            "group_required": "组队功能只能在群聊中使用！",
            "user_missing": "未找到道友数据，创建队伍失败！",
            "user_has_team": "你已经在一个队伍中了，请先退出当前队伍！",
            "session_active": "副本探索会话进行中，无法创建队伍！",
            "state_changed": "队伍状态已变化，请稍后重试。",
        }
        if result.status == "cooldown_active":
            return f"你当前处于组队冷却中，剩余：{format_seconds(result.cooldown_seconds)}，不可创建队伍。"
        return messages.get(result.status, "创建队伍失败！")
    if action == "invite":
        if result.status in {"applied", "duplicate"}:
            return f"📨 已向{target_name}发送组队邀请，等待对方回应..."
        messages = {
            "group_required": "组队功能只能在群聊中使用！",
            "target_missing": "未找到指定的用户，请检查道号或艾特是否正确！",
            "user_missing": "目标用户信息异常，无法发送邀请！",
            "team_disbanded": "你还没有创建或加入任何队伍！",
            "actor_not_leader": "只有队长才能邀请成员！",
            "team_full": "队伍已满，无法邀请新成员！",
            "user_has_team": f"{target_name}已有队伍！",
            "invite_pending": "对方已有待处理的组队邀请，请稍后再试！",
            "session_active": "副本探索会话进行中，无法变更队伍！",
            "state_changed": "队伍状态已变化，请稍后重试。",
        }
        if result.status == "cooldown_active":
            return f"{target_name}当前处于组队冷却中（剩余{format_seconds(result.cooldown_seconds)}），不可被邀请。"
        return messages.get(result.status, "发送组队邀请失败！")
    if action == "join":
        if result.status in {"applied", "duplicate"}:
            leader = sql_message.get_user_info_with_id(result.leader_id) or {}
            return build_invite_response_message(
                TeamInviteResponseResult(
                    "joined",
                    team_name=result.team_name,
                    leader_name=leader.get("user_name", result.leader_id),
                    member_count=result.member_count,
                    max_members=result.max_members,
                )
            )
        messages = {
            "invite_invalid": "没有有效的待处理组队邀请！",
            "wrong_group": f"此邀请是在群{result.group_id}发出的，请在该群或私聊中进行操作。",
            "team_disbanded": "该队伍已解散！",
            "user_has_team": "你已经在一个队伍中了，无法接受邀请！",
            "team_full": "该队伍已满员！",
            "session_active": "副本探索会话进行中，无法变更队伍！",
            "state_changed": "邀请或队伍状态已变化，加入队伍失败！",
        }
        if result.status == "cooldown_active":
            return "你当前处于组队冷却中，无法加入队伍！"
        return messages.get(result.status, "加入队伍失败！")
    if action == "reject":
        if result.status in {"applied", "duplicate"}:
            return build_invite_response_message(TeamInviteResponseResult("rejected"))
        if result.status == "wrong_group":
            return f"此邀请是在群{result.group_id}发出的，请在该群或私聊中进行操作。"
        return build_invite_response_message(TeamInviteResponseResult("no_invite"))
    if action == "transfer":
        if result.status in {"applied", "duplicate"}:
            return build_transfer_team_success_message(target_name)
        return {
            "team_missing": "你不在任何队伍中！",
            "actor_not_leader": "只有队长才能转移队长职位！",
            "self_target": build_transfer_team_self_message(),
            "target_not_member": build_transfer_team_not_member_message(),
            "session_active": "副本探索会话进行中，无法变更队伍！",
            "state_changed": "队伍状态已变化，请稍后重试。",
        }.get(result.status, "转移队长失败！")
    return "队伍操作失败！"


def _team_exit_message(action: str, result: TeamExitResult) -> str:
    if result.status not in {"applied", "duplicate"}:
        return {
            "team_missing": "你不在任何队伍中！",
            "actor_not_member": "你不在任何队伍中！",
            "actor_not_leader": "只有队长才能执行此操作！",
            "target_not_member": "该成员不在你的队伍中！",
            "self_target": "不能踢出自己！",
            "session_active": "副本探索会话进行中，无法变更队伍！",
            "state_changed": "队伍状态已变化，请稍后重试。",
        }.get(result.status, "队伍操作失败！")
    if action == "leave":
        if result.disbanded:
            return f"你已离开队伍【{result.team_name}】，队伍已解散。\n你进入了{TEAM_JOIN_CD_HOURS}小时组队冷却。"
        if result.new_leader_id:
            leader = sql_message.get_user_info_with_id(result.new_leader_id) or {}
            return f"你已离开队伍【{result.team_name}】，队长已转让给{leader.get('user_name', result.new_leader_id)}。\n你进入了{TEAM_JOIN_CD_HOURS}小时组队冷却。"
        return f"你已离开队伍【{result.team_name}】。\n你进入了{TEAM_JOIN_CD_HOURS}小时组队冷却。"
    if action == "kick":
        target = sql_message.get_user_info_with_id(result.target_id) or {}
        return f"已将成员{target.get('user_name', result.target_id)}踢出队伍。\n对方进入{TEAM_JOIN_CD_HOURS}小时组队冷却。"
    return f"队伍【{result.team_name}】已解散。\n全体成员进入{TEAM_JOIN_CD_HOURS}小时组队冷却。"


# =========================
# 指令注册
# =========================
create_team_cmd = on_command("创建队伍", aliases={"新建队伍"}, priority=5, block=True)
invite_team_cmd = on_command("邀请组队", aliases={"邀请入队", "组队邀请"}, priority=5, block=True)
agree_team_cmd = on_command("同意组队", aliases={"加入队伍", "接受组队", "组队同意"}, priority=5, block=True)
reject_team_cmd = on_command("拒绝组队", aliases={"拒绝入队"}, priority=5, block=True)
leave_team_cmd = on_command("离开队伍", aliases={"退出队伍"}, priority=5, block=True)
kick_team_cmd = on_command("踢出队伍", aliases={"移除队员"}, priority=5, block=True)
disband_team_cmd = on_command("解散队伍", aliases={"解散组队"}, priority=5, block=True)
view_team_cmd = on_command("查看队伍", aliases={"队伍信息", "我的队伍"}, priority=5, block=True)
help_team_cmd = on_command("队伍帮助", aliases={"组队帮助", "组队指令"}, priority=5, block=True)
transfer_team_cmd = on_command("转移队长", aliases={"队长转让", "转让队长"}, priority=5, block=True)

dungeon_info = on_command("副本信息", aliases={"今日副本"}, priority=5, block=True)
explore_dungeon = on_command("探索副本", aliases={"副本探索", "挑战副本"}, priority=5, block=True)
dungeon_exit = on_command("退出副本", aliases={"离开副本"}, priority=5, block=True)
dungeon_shop = on_command("副本商店", priority=5, block=True)
dungeon_purchase = on_command("副本兑换", aliases={"副本购买"}, priority=5, block=True)
dungeon_status = on_command("我的副本状态", aliases={"副本状态", "我的副本信息"}, priority=5, block=True)
reset_command = on_command("重置副本", aliases={"手动重置"}, priority=5, block=True, permission=SUPERUSER)
help_dungeon_cmd = on_command("副本帮助", aliases={"副本指令"}, priority=5, block=True)

scheduler = require("nonebot_plugin_apscheduler").scheduler

cache_team_help = {}
cache_dungeon_help = {}

__dungeon_help__ = f"""
【副本指令列表】📜
副本信息 - 查看今日开放的副本
探索副本 - 开始挑战副本
我的副本状态 - 查看个人副本进度
副本帮助 - 查看副本指令
""".strip()

__team_help__ = f"""
【组队指令列表】📜
创建队伍 [队伍名] - 创建新队伍
邀请组队 道号 - 邀请其他人加入队伍
同意组队 - 同意组队邀请
拒绝组队 - 拒绝组队邀请
离开队伍 - 离开当前队伍
踢出队伍 道号 - 踢出队员（队长权限）
解散队伍 - 解散队伍（队长权限）
查看队伍 - 查看队伍信息
转让队长 道号 - 将队长转移给队伍内成员（队长权限）
组队帮助 - 查看组队指令
""".strip()


# =========================
# 帮助
# =========================
@help_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def help_team_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __team_help__
    await send_help_message(bot, event, msg, k1="创建队伍", v1="创建队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await help_team_cmd.finish()


@help_dungeon_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def help_dungeon_cmd_(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __dungeon_help__
    await send_help_message(bot, event, msg, k1="副本信息", v1="副本信息", k2="探索副本", v2="探索副本", k3="副本状态", v3="我的副本状态")
    await help_dungeon_cmd.finish()


# =========================
# 组队逻辑
# =========================
@create_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def create_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await create_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "create", user_id)
    replay = dungeon_team_transaction_service.operation_result(operation_id, "create")
    if replay is not None:
        await handle_send(bot, event, _team_mutation_message("create", replay), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await create_team_cmd.finish()

    group_id = event.group_id if isinstance(event, GroupMessageEvent) else None

    team_name = args.extract_plain_text().strip()
    if not team_name:
        team_name = f"{user_info['user_name']}的队伍"

    team_id = f"{group_id or 'private'}_{operation_id.rsplit(':', 2)[-2]}"
    now = datetime.now()
    result = dungeon_team_transaction_service.create(
        operation_id, team_id, team_name, user_id, group_id,
        now.strftime("%Y-%m-%d %H:%M:%S"), now.timestamp(),
    )
    await handle_send(bot, event, _team_mutation_message("create", result), md_type="team", k1="邀请组队", v1="邀请组队", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await create_team_cmd.finish()


@invite_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def invite_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await invite_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "invite", user_id)
    replay = dungeon_team_transaction_service.operation_result(operation_id, "invite")
    if replay is not None:
        await handle_send(bot, event, _team_mutation_message("invite", replay), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    arg = args.extract_plain_text().strip()
    target_user_id = get_at_user_id(args)
    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])
    target_user_id = str(target_user_id or "")
    team_id = get_user_team(user_id) or ""
    group_id = str(getattr(event, "group_id", "") or "")
    now = datetime.now().timestamp()
    invite_id = f"{operation_id}:{target_user_id or 'missing'}"
    result = dungeon_team_transaction_service.invite(
        operation_id,
        invite_id,
        team_id,
        user_id,
        target_user_id,
        group_id,
        now + 60,
        now,
    )
    await handle_send(bot, event, _team_mutation_message("invite", result), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")

    if result.status == "applied":
        asyncio.create_task(expire_team_invite(target_user_id, invite_id, bot, event))
    try:
        if result.status == "applied" and isinstance(event, GroupMessageEvent):
            await delivery_service.send_to_user(
                bot,
                str(target_user_id),
                build_team_invite_private_message(
                    group_id=str(event.group_id),
                    inviter_name=user_info['user_name'],
                ),
            )
    except Exception as e:
        logger.warning(f"私聊通知被邀请者失败: {e}")

    await invite_team_cmd.finish()


@agree_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def agree_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await agree_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "join", user_id)
    replay = dungeon_team_transaction_service.operation_result(operation_id, "join")
    if replay is not None:
        await handle_send(bot, event, _team_mutation_message("join", replay), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await agree_team_cmd.finish()

    now = datetime.now().timestamp()
    invite = dungeon_team_transaction_service.pending_invite(user_id, now)
    invite_id = invite.invite_id if invite else ""
    team_id = invite.team_id if invite else ""
    inviter_id = invite.inviter_id if invite else ""
    invite_group_id = invite.group_id if invite else ""
    request_group_id = (
        str(event.group_id) if isinstance(event, GroupMessageEvent) else invite_group_id
    )
    join_result = dungeon_team_transaction_service.join(
        operation_id,
        invite_id,
        team_id,
        inviter_id,
        user_id,
        request_group_id,
        now,
    )
    await handle_send(bot, event, _team_mutation_message("join", join_result), md_type="team", k1="查看队伍", v1="查看队伍", k2="副本信息", v2="副本信息", k3="队伍帮助", v3="队伍帮助")

    await agree_team_cmd.finish()


@reject_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def reject_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await reject_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "reject", user_id)
    replay = dungeon_team_transaction_service.operation_result(operation_id, "reject")
    if replay is not None:
        await handle_send(bot, event, _team_mutation_message("reject", replay), md_type="team", k1="队伍帮助", v1="队伍帮助")
        await reject_team_cmd.finish()

    now = datetime.now().timestamp()
    invite = dungeon_team_transaction_service.pending_invite(user_id, now)
    group_id = str(event.group_id) if isinstance(event, GroupMessageEvent) else ""
    result = dungeon_team_transaction_service.reject(
        operation_id,
        invite.invite_id if invite else "",
        user_id,
        group_id,
        now,
    )
    await handle_send(bot, event, _team_mutation_message("reject", result), md_type="team", k1="队伍帮助", v1="队伍帮助")
    await reject_team_cmd.finish()


@leave_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def leave_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await leave_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "leave", user_id)
    replay = dungeon_team_exit_service.exit_operation_result(
        operation_id, "leave", user_id
    )
    if replay is not None:
        await handle_send(bot, event, _team_exit_message("leave", replay), md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await leave_team_cmd.finish()

    team_id = get_user_team(user_id) or f"missing:{user_id}"
    team_snapshot = dungeon_team_exit_service.snapshot(team_id)
    exit_result = dungeon_team_exit_service.leave(
        operation_id,
        user_id,
        team_snapshot or _missing_team_snapshot(team_id),
        _team_cooldown_until(),
    )
    await handle_send(bot, event, _team_exit_message("leave", exit_result), md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
    await leave_team_cmd.finish()


@kick_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def kick_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await kick_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "kick", user_id)
    replay = dungeon_team_exit_service.exit_operation_result(
        operation_id, "kick", user_id
    )
    if replay is not None:
        await handle_send(bot, event, _team_exit_message("kick", replay), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()

    arg = args.extract_plain_text().strip()
    target_user_id = get_at_user_id(args)
    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])
    target_user_id = str(target_user_id or "")
    team_id = get_user_team(user_id) or f"missing:{user_id}"
    team_snapshot = dungeon_team_exit_service.snapshot(team_id)
    exit_result = dungeon_team_exit_service.kick(
        operation_id,
        user_id,
        target_user_id,
        team_snapshot or _missing_team_snapshot(team_id),
        _team_cooldown_until(),
    )
    await handle_send(bot, event, _team_exit_message("kick", exit_result), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
    await kick_team_cmd.finish()


@disband_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def disband_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await disband_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "disband", user_id)
    replay = dungeon_team_exit_service.exit_operation_result(
        operation_id, "disband", user_id
    )
    if replay is not None:
        await handle_send(bot, event, _team_exit_message("disband", replay), md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await disband_team_cmd.finish()

    team_id = get_user_team(user_id) or f"missing:{user_id}"
    team_snapshot = dungeon_team_exit_service.snapshot(team_id)
    exit_result = dungeon_team_exit_service.disband(
        operation_id,
        user_id,
        team_snapshot or _missing_team_snapshot(team_id),
        _team_cooldown_until(),
    )
    await handle_send(bot, event, _team_exit_message("disband", exit_result), md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
    await disband_team_cmd.finish()


@view_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def view_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await view_team_cmd.finish()

    user_id = str(user_info['user_id'])

    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！\n📢 使用【创建队伍 队伍名】来创建队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await view_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info:
        msg = "队伍信息异常！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await view_team_cmd.finish()

    view_result = build_team_view(
        team_info,
        lambda member_id: (
            sql_message.get_user_info_with_id(member_id) or {}
        ).get("user_name", f"未知用户({member_id})"),
    )
    msg = build_team_view_message(view_result)

    await handle_send(bot, event, msg, md_type="team", k1="探索副本", v1="探索副本", k2="离开队伍", v2="离开队伍", k3="队伍帮助", v3="队伍帮助")
    await view_team_cmd.finish()


@transfer_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def transfer_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await transfer_team_cmd.finish()

    user_id = str(user_info['user_id'])
    operation_id = _team_operation_id(event, "transfer", user_id)
    replay = dungeon_team_transaction_service.operation_result(
        operation_id, "transfer"
    )
    if replay is not None:
        await handle_send(bot, event, _team_mutation_message("transfer", replay), md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()

    arg = args.extract_plain_text().strip()
    target_user_id = get_at_user_id(args)
    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])
    target_user_id = str(target_user_id or "")
    team_id = get_user_team(user_id) or f"missing:{user_id}"
    snapshot = dungeon_team_transaction_service.snapshot(team_id)
    result = dungeon_team_transaction_service.transfer(
        operation_id,
        user_id,
        target_user_id,
        snapshot or _missing_team_snapshot(team_id),
    )
    await handle_send(bot, event, _team_mutation_message("transfer", result), md_type="team", k1="查看队伍", v1="查看队伍", k2="探索副本", v2="探索副本", k3="队伍帮助", v3="队伍帮助")
    await transfer_team_cmd.finish()


# =========================
# 奖励结算
# =========================
def _get_level_initial_exp(user_level: str) -> int:
    if user_level == "江湖好手":
        return int(jsondata.level_data().get("江湖好手", {}).get("power", 100))
    if user_level == "至高":
        return int(jsondata.level_data().get("至高", {}).get("power", 100))

    if user_level.endswith("初期") or user_level.endswith("中期") or user_level.endswith("圆满"):
        main = user_level[:-2]
    else:
        main = user_level

    key = f"{main}初期"
    return int(jsondata.level_data().get(key, {}).get("power", 100))


def _calc_team_distribution(team_member_ids: list, leader_id: int, dmg_map: dict):
    member_ids = [str(x) for x in team_member_ids]
    n = len(member_ids)
    if n <= 0:
        return {}

    if n == 1:
        return {member_ids[0]: Fraction(1, 2)}

    fixed = {}
    fixed_pool = Fraction(0)
    for uid in member_ids:
        if uid == str(leader_id):
            fixed[uid] = Fraction(1, 5)
        else:
            fixed[uid] = Fraction(1, 10)
        fixed_pool += fixed[uid]

    remain_pool = max(Fraction(0), Fraction(1) - fixed_pool)

    contrib = {}
    contrib_sum = 0
    for uid in member_ids:
        d = max(0, int(dmg_map.get(uid, 0)))
        contrib[uid] = d
        contrib_sum += d

    dist = dict(fixed)

    if remain_pool > 0:
        if contrib_sum <= 0:
            avg = remain_pool / n
            for uid in member_ids:
                dist[uid] += avg
        else:
            for uid in member_ids:
                dist[uid] += remain_pool * Fraction(contrib[uid], contrib_sum)

    cap = Fraction(1, 2)
    for uid in member_ids:
        if dist[uid] > cap:
            dist[uid] = cap

    return dist


def _get_reward_caps(user_level: str, is_boss: bool):
    level_initial_exp = _get_level_initial_exp(user_level)
    stone_cap = 10_000_000 if is_boss else 5_000_000
    exp_cap_ratio = 0.03 if is_boss else 0.01
    exp_cap = int(level_initial_exp * exp_cap_ratio)
    return stone_cap, exp_cap


def build_battle_rewards(
    user_info, members_info, monsters_list, status_list, operation_id
):
    is_boss = any(m.get("monster_type") == "boss" for m in monsters_list)

    total_stone_pool = sum(int(monster.get("stone", 0)) for monster in monsters_list)
    total_exp_pool = sum(int(monster.get("experience", 0)) for monster in monsters_list)

    dmg_map = {}
    for d in status_list:
        for _, stats in d.items():
            if stats.get('team_id') == 0 and str(stats.get('user_id', 0)) != 0:
                uid = str(stats['user_id'])
                dmg_map[uid] = max(0, int(stats.get("total_dmg", 0)))

    real_members = []
    member_info_map = {}
    for m in members_info:
        uid = str(m["user_id"])
        if uid in dmg_map:
            real_members.append(uid)
            member_info_map[uid] = m

    if not real_members:
        return "\n副本奖励：无，本次战斗无人获得奖励。", []

    leader_id = str(user_info["user_id"])
    dist = _calc_team_distribution(real_members, leader_id, dmg_map)

    item_ids = [m["item_id"] for m in monsters_list if int(m.get("item_id", 0)) != 0]

    msg = "\n副本奖励："
    rewards = []
    reward_rng = random.Random(operation_id)

    for uid in real_members:
        m_info = member_info_map.get(uid)
        if not m_info:
            continue

        ratio = dist.get(uid, 0.0)

        stone_raw = int(total_stone_pool * ratio)
        exp_raw = int(total_exp_pool * ratio)

        stone_cap, exp_cap = _get_reward_caps(m_info["level"], is_boss)
        stone_final = min(stone_raw, stone_cap)
        exp_final = min(exp_raw, exp_cap)

        rewards_msg = []

        if stone_final > 0:
            rewards_msg.append(f"灵石{number_to(stone_final)}")

        if exp_final > 0:
            rewards_msg.append(f"修为{number_to(exp_final)}")

        reward_items = []
        if uid == leader_id and item_ids:
            item_id = reward_rng.choice(item_ids)
            item_info = items.get_data_by_item_id(item_id)
            if item_info:
                reward_items.append(
                    {
                        "id": item_id,
                        "name": item_info["name"],
                        "type": item_info["type"],
                        "amount": 1,
                    }
                )
                rewards_msg.append(f"{item_info['name']}")

        for item in reward_items:
            item["expected_num"] = int(sql_message.goods_num(uid, item["id"]))
            item["expected_bind_num"] = int(
                sql_message.goods_num(uid, item["id"], "bind")
            )
        rewards.append({
            "user_id": uid,
            "expected_stone": int(m_info["stone"]),
            "expected_exp": int(m_info["exp"]),
            "stone": stone_final,
            "exp": exp_final,
            "items": reward_items,
        })

        total_dmg = sum(dmg_map.values()) if sum(dmg_map.values()) > 0 else 1
        contrib_percent = dmg_map.get(uid, 0) / total_dmg * 100
        alloc_percent = float(ratio * 100)

        rewards_msg_str = "无" if not rewards_msg else "、".join(rewards_msg)
        msg += f"\n{m_info['user_name']}（贡献{contrib_percent:.2f}% / 分配{alloc_percent:.2f}%）获得：{rewards_msg_str}"

    return msg, rewards


def _get_user_cd_type(user_id: str) -> int:
    with db_backend.connection(get_paths().game_db) as conn:
        if not conn.table_exists("user_cd"):
            return 0
        row = conn.execute(
            "SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s "
            "ORDER BY rowid DESC LIMIT 1",
            (str(user_id),),
        ).fetchone()
    return int(row[0]) if row else 0


def _explore_response(message: str, battle_messages=None) -> dict:
    return {
        "battle_messages": list(battle_messages or []),
        "message": str(message),
    }


async def _send_explore_response(bot: Bot, event, response: dict) -> None:
    battle_messages = response.get("battle_messages") or []
    if battle_messages:
        try:
            await send_msg_handler(bot, event, battle_messages)
        except Exception:
            logger.exception("副本战报发送失败，结算 operation 已保留完整战报")
    await handle_send(
        bot,
        event,
        str(response.get("message", "副本结算完成。")),
        md_type="副本",
        k1="探索副本",
        v1="探索副本",
        k2="副本状态",
        v2="我的副本状态",
        k3="副本帮助",
        v3="副本帮助",
    )


def _progress_message(current_layer: int, total_layers: int, *, advance: bool, complete: bool) -> str:
    if complete:
        final_layer = int(total_layers)
    elif advance:
        final_layer = min(int(current_layer) + 1, int(total_layers))
    else:
        final_layer = int(current_layer)
    if final_layer >= int(total_layers):
        return "恭喜你已完成今日副本！"
    return f"当前：第{final_layer + 1}层\n使用「探索副本」进入下一层！"


def _base_member_plan(user_info: dict, cd_type: int) -> dict:
    return {
        "user_id": str(user_info["user_id"]),
        "expected": {
            "hp": int(user_info["hp"]),
            "mp": int(user_info["mp"]),
            "stone": int(user_info["stone"]),
            "exp": int(user_info["exp"]),
            "cd_type": int(cd_type),
        },
        "final_hp": int(user_info["hp"]),
        "final_mp": int(user_info["mp"]),
        "stone_delta": 0,
        "exp_delta": 0,
        "items": [],
    }


def check_user_state(user_info):
    user_id = user_info["user_id"]
    state_msg = f"{user_info['user_name']}"
    is_type, msg = check_user_type(user_id, 0)
    if not is_type:
        state_msg += f"：{msg}\n"
        return True, state_msg

    if user_info['hp'] <= user_info['exp'] / 8:
        time = leave_harm_time(user_id)
        state_msg += f"：重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！\n"
        return True, state_msg

    return False, "正常"


# =========================
# 副本定时任务
# =========================
@scheduler.scheduled_job(
    "cron",
    hour=0,
    minute=1,
    id="daily_dungeon_reset",
    coalesce=True,
    max_instances=1,
    misfire_grace_time=300,
)
async def daily_dungeon_reset():
    """每日自动重置副本和玩家状态"""
    try:
        logger.info("开始执行每日副本重置任务")
        result = dungeon_manager.reset_dungeon(source="daily")
        dungeon_info = dungeon_manager.get_dungeon_progress()
        logger.info(
            f"每日副本重置完成: {dungeon_info.get('name', '未知副本')} | "
            f"层数={dungeon_info.get('total_layers', 0)} | "
            f"日期={dungeon_info.get('date', '未知')} | "
            f"generation={result.generation} | status={result.status}"
        )
    except Exception as e:
        logger.exception(f"每日副本重置任务执行失败: {e}")
        raise


# =========================
# 手动重置
# =========================
@reset_command.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_manual_reset(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    try:
        event_id = getattr(event, "message_id", None)
        user_id = str(event.get_user_id())
        operation_id = (
            f"dungeon-reset:manual:{event_id}:{user_id}"
            if event_id
            else f"dungeon-reset:manual:{time.time_ns()}:{user_id}"
        )
        result = dungeon_manager.reset_dungeon(operation_id, source="manual")
        dungeon_info = result.dungeon_snapshot
        msg = (
            f"✅ 副本和所有玩家的副本进度已重置。\n"
            f"当前副本：{dungeon_info.get('dungeon_name', '未知副本')}\n"
            f"总层数：{dungeon_info.get('total_layers', 0)}\n"
            f"重置代次：{result.generation}"
        )
        logger.info(f"管理员手动重置副本成功: {dungeon_info}")
    except Exception as e:
        logger.exception(f"管理员手动重置副本失败: {e}")
        msg = f"❌ 副本重置失败：{e}"

    await handle_send(bot, event, msg, md_type="副本", k1="副本信息", v1="副本信息", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
    await reset_command.finish()


# =========================
# 副本信息
# =========================
@dungeon_info.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_dungeon_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    dungeon_data = dungeon_manager.get_dungeon_progress()

    msg = (
        "【今日副本】\n"
        f"名称：{dungeon_data['name']}\n"
        f"简介：{dungeon_data['description']}\n"
        f"副本类型：{dungeon_data.get('type', 'explore')}\n"
        f"总层数：{dungeon_data['total_layers']}层\n"
        f"副本日期：{dungeon_data['date']}\n"
        "操作：使用「探索副本」开始冒险。"
    )

    await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
    await dungeon_info.finish()


@dungeon_shop.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_dungeon_shop(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    lines = ["【副本商店】"]
    for item_id, item in DUNGEON_SHOP.items():
        lines.append(f"{item_id} | {item['name']} | {number_to(item['cost'])}灵石")
    lines.append("使用：副本兑换 物品ID 数量")
    await handle_send(bot, event, "\n".join(lines), md_type="副本", k1="兑换", v1="副本兑换", k2="探索", v2="探索副本", k3="状态", v3="我的副本状态")
    await dungeon_shop.finish()


@dungeon_purchase.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_dungeon_purchase(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await dungeon_purchase.finish()
    parts = args.extract_plain_text().strip().split()
    if not parts or not parts[0].isdigit() or (len(parts) > 1 and not parts[1].isdigit()):
        await handle_send(bot, event, "格式：副本兑换 物品ID 数量")
        await dungeon_purchase.finish()
    item_id = int(parts[0])
    quantity = int(parts[1]) if len(parts) > 1 else 1
    if quantity <= 0:
        await handle_send(bot, event, "副本商店中没有该商品，或数量无效。")
        await dungeon_purchase.finish()
    event_id = getattr(event, "message_id", None)
    operation_id = f"dungeon-purchase:{event_id}:{user_info['user_id']}" if event_id else f"dungeon-purchase:{time.time_ns()}:{user_info['user_id']}"
    try:
        replay = dungeon_purchase_service.operation_result(
            operation_id,
            user_info["user_id"],
            item_id,
            quantity,
            1,
        )
    except Exception:
        logger.exception("读取副本商店兑换 operation 失败")
        await handle_send(bot, event, "副本兑换失败，请稍后重试。")
        await dungeon_purchase.finish()
    if replay is not None:
        await handle_send(bot, event, replay.response or "兑换失败。")
        await dungeon_purchase.finish()
    shop_item = DUNGEON_SHOP.get(item_id)
    item_info = items.get_data_by_item_id(item_id)
    if shop_item is None or item_info is None:
        await handle_send(bot, event, "副本商店中没有该商品，或数量无效。")
        await dungeon_purchase.finish()
    try:
        result = dungeon_purchase_service.purchase(
            operation_id,
            user_info["user_id"],
            item_id,
            item_info["name"],
            item_info.get("type", item_info.get("item_type", "道具")),
            quantity,
            shop_item["cost"],
            user_info["stone"],
            XiuConfig().max_goods_num,
            1,
        )
        await handle_send(bot, event, result.response or "兑换失败。")
    except Exception:
        logger.exception("副本商店兑换事务失败")
        await handle_send(bot, event, "副本兑换失败，请稍后重试。")
    await dungeon_purchase.finish()


@dungeon_exit.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_dungeon_exit(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await dungeon_exit.finish()
    user_id = user_info["user_id"]
    event_id = getattr(event, "message_id", None)
    operation_id = f"dungeon-exit:{event_id}:{user_id}" if event_id else f"dungeon-exit:{time.time_ns()}:{user_id}"
    result = dungeon_session_service.operation_result(operation_id, user_id, "exit")
    if result is None:
        status = dungeon_manager.get_player_status(user_id)
        dungeon = dungeon_manager.get_dungeon_progress()
        result = dungeon_session_service.exit(
            operation_id,
            user_id,
            status,
            {"dungeon_id": status["dungeon_id"], "date": dungeon["date"]},
        )
    if result.status == "not_exploring":
        await handle_send(bot, event, "当前未在副本探索中。")
    elif result.status == "completed":
        await handle_send(bot, event, "今日副本已完成，无需退出。")
    elif result.status == "state_changed":
        await handle_send(bot, event, "副本状态已变化，请稍后重试。")
    else:
        await handle_send(bot, event, "已退出当前副本，进度将保留。")
    await dungeon_exit.finish()


# =========================
# 探索副本
# =========================
@explore_dungeon.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_explore_dungeon(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await explore_dungeon.finish()

    user_id = str(user_info["user_id"])
    event_id = getattr(event, "message_id", None)
    operation_id = (
        f"dungeon-explore:{event_id}:{user_id}"
        if event_id
        else f"dungeon-explore:{time.time_ns()}:{user_id}"
    )

    try:
        replay = dungeon_explore_operation_service.replay(operation_id, user_id)
    except Exception:
        logger.exception("读取副本探索 operation 失败")
        await handle_send(bot, event, "副本探索结算失败，请稍后重试。")
        await explore_dungeon.finish()

    if replay.status == "operation_conflict":
        await handle_send(bot, event, "该探索事件身份冲突，无法重放。")
        await explore_dungeon.finish()
    if replay.phase == "completed":
        await _send_explore_response(bot, event, replay.response or {})
        await explore_dungeon.finish()
    if replay.phase == "prepared":
        try:
            resumed = dungeon_explore_operation_service.settle(
                operation_id, user_id, XiuConfig().max_goods_num
            )
        except Exception:
            logger.exception("恢复副本探索 operation 失败")
            await handle_send(bot, event, "副本探索结算失败，请稍后重试。")
            await explore_dungeon.finish()
        if resumed.phase == "completed":
            await _send_explore_response(bot, event, resumed.response or {})
        else:
            await handle_send(bot, event, "副本结算任务恢复失败，请稍后重试。")
        await explore_dungeon.finish()

    async def reject(result_status: str, message: str, status=None) -> None:
        status = status or {}
        response = _explore_response(message)
        try:
            stored = dungeon_explore_operation_service.resolve_rejection(
                operation_id,
                user_id,
                result_status,
                response,
                XiuConfig().max_goods_num,
                current_layer=int(status.get("current_layer", 0) or 0),
                dungeon_status=str(status.get("dungeon_status", "")),
            )
            await _send_explore_response(bot, event, stored.response or response)
        except Exception:
            logger.exception("持久化副本探索拒绝响应失败")
            await handle_send(bot, event, "副本探索校验失败，请稍后重试。")

    player_status = dungeon_manager.get_player_status(user_id)
    if player_status["dungeon_status"] == "completed":
        await reject("completed", "今日副本已完成，请等待明日刷新！", player_status)
        await explore_dungeon.finish()
    if player_status["dungeon_status"] not in {"not_started", "exited", "exploring"}:
        await reject("invalid_status", "当前副本状态异常，请稍后重试。", player_status)
        await explore_dungeon.finish()

    dungeon = dungeon_manager.get_dungeon_progress()
    if (
        str(player_status.get("dungeon_id", "")) != str(dungeon.get("dungeon_id", player_status.get("dungeon_id", "")))
        or str(player_status.get("last_reset_date", "")) != str(dungeon.get("date", ""))
    ):
        await reject("state_changed", "副本状态已变化，请稍后重试。", player_status)
        await explore_dungeon.finish()

    current_layer = int(player_status["current_layer"])
    total_layers = int(player_status["total_layers"])
    if total_layers <= 0 or current_layer < 0 or current_layer >= total_layers:
        await reject("invalid_progress", "当前副本进度异常，请稍后重试。", player_status)
        await explore_dungeon.finish()

    user_ids_in_battle = [str(user_id)]
    exp_ratios = None
    mentor_attack_buffs = {}
    mentor_buff_msg = ""

    team_id = get_user_team(user_id)
    team_snapshot = None
    members_info = [user_info]
    if team_id:
        team_info = get_team_info(team_id)
        if not team_info:
            await reject("team_missing", "队伍信息异常，请先处理队伍状态。", player_status)
            await explore_dungeon.finish()
        if str(team_info.get("leader", "")) != user_id:
            await reject("not_leader", "你不是队长，无法带领队伍探索副本！", player_status)
            await explore_dungeon.finish()
        member_ids = [str(member) for member in team_info.get("members", [])]
        if (
            not member_ids
            or user_id not in member_ids
            or len(member_ids) != len(set(member_ids))
        ):
            await reject("team_invalid", "队伍成员数据异常，请先处理队伍状态。", player_status)
            await explore_dungeon.finish()
        members_info = []
        for member_id in member_ids:
            member_info = sql_message.get_user_info_with_id(member_id)
            if member_info is None:
                await reject(
                    "member_missing",
                    f"队伍成员 {member_id} 数据不存在，无法开始探索。",
                    player_status,
                )
                await explore_dungeon.finish()
            if int(member_info.get("is_ban", 0) or 0) == 1:
                await reject(
                    "member_banned",
                    f"{member_info.get('user_name', member_id)}当前无法参与副本探索。",
                    player_status,
                )
                await explore_dungeon.finish()
            members_info.append(member_info)
        user_ids_in_battle = member_ids
        team_snapshot = {
            "team_id": str(team_id),
            "leader": str(team_info["leader"]),
            "members": member_ids,
        }
        if "version" in team_info:
            team_snapshot["version"] = int(team_info.get("version", 0) or 0)

    member_cd_types = {}
    for member in members_info:
        member_id = str(member["user_id"])
        cd_type = _get_user_cd_type(member_id)
        member_cd_types[member_id] = cd_type
        if cd_type != 0:
            await reject(
                "member_busy",
                f"{member['user_name']}正处于其他修炼状态，无法参与副本探索。",
                player_status,
            )
            await explore_dungeon.finish()
        if int(member["hp"]) <= int(member["exp"]) / 8:
            harm_minutes = leave_harm_time(member_id)
            await reject(
                "member_injured",
                f"{member['user_name']}：重伤未愈，动弹不得！距离脱离危险还需要{harm_minutes}分钟！",
                player_status,
            )
            await explore_dungeon.finish()

    user_exp = int(user_info["exp"])
    if len(members_info) > 1:
        exp_ratios = {
            str(member["user_id"]): (
                max(0.5, min(1.0, user_exp / int(member["exp"])))
                if int(member["exp"]) > 0 and user_exp > 0
                else 1.0
            )
            for member in members_info
        }
        from ..xiuxian_buff.partner import get_mentor_team_attack_buffs

        mentor_attack_buffs = get_mentor_team_attack_buffs(user_ids_in_battle)
        if mentor_attack_buffs:
            buff_names = [
                member["user_name"]
                for member in members_info
                if str(member["user_id"]) in mentor_attack_buffs
            ]
            if buff_names:
                mentor_buff_msg = (
                    f"\n师徒羁绊「薪火相承」触发：{', '.join(buff_names)}攻击提升10%。"
                )

    member_plans = [
        _base_member_plan(member, member_cd_types[str(member["user_id"])])
        for member in members_info
    ]
    member_plan_map = {member["user_id"]: member for member in member_plans}
    attack_buffs = {
        str(uid): data["attack_multiplier"]
        for uid, data in mentor_attack_buffs.items()
    }
    advance = True
    complete = False
    resolved = {}

    if current_layer == total_layers - 1:
        boss_info = dungeon_manager.get_boss_data(user_info["level"], user_exp)
        battle_messages, winner, status = await pve_fight(
            user_ids_in_battle,
            boss_info,
            type_in=0,
            bot_id=bot.self_id,
            level_ratios=exp_ratios,
            attack_buffs=attack_buffs,
        )
        final_statuses = resolve_final_user_statuses(
            status, bot.self_id, exp_ratios
        )
        for member_id, final in final_statuses.items():
            if member_id in member_plan_map:
                member_plan_map[member_id]["final_hp"] = int(final["hp"])
                member_plan_map[member_id]["final_mp"] = int(final["mp"])
        if winner == 0:
            reward_message, rewards = build_battle_rewards(
                user_info, members_info, boss_info, status, operation_id
            )
            for reward in rewards:
                plan_member = member_plan_map.get(str(reward["user_id"]))
                if plan_member:
                    plan_member["stone_delta"] = int(reward["stone"])
                    plan_member["exp_delta"] = int(reward["exp"])
                    plan_member["items"] = list(reward.get("items", []))
            complete = True
            summary = (
                f"恭喜道友击败【{boss_info[0]['name']}】！"
                f"{mentor_buff_msg}{reward_message}\n"
                f"{_progress_message(current_layer, total_layers, advance=True, complete=True)}"
            )
        else:
            advance = False
            summary = (
                f"道友不敌【{boss_info[0]['name']}】，重伤逃遁。\n"
                f"{_progress_message(current_layer, total_layers, advance=False, complete=False)}"
            )
        response = _explore_response(summary, battle_messages)
        resolved = {"kind": "boss", "winner": int(winner), "monsters": boss_info}
    else:
        event_result = dungeon_manager.trigger_event(user_info["level"], user_exp)
        event_type = str(event_result.get("type", "nothing"))
        battle_messages = []
        if event_type == "trap":
            message_parts = [str(event_result.get("description", ""))]
            damage = float(event_result.get("damage", 0.1))
            for member in members_info:
                member_id = str(member["user_id"])
                cost_hp = int((int(member["exp"]) / 2) * damage)
                member_plan_map[member_id]["final_hp"] = max(
                    1, int(member["hp"]) - cost_hp
                )
                message_parts.append(
                    f"{member['user_name']}气血减少：{number_to(cost_hp)}"
                )
            summary = "，".join(message_parts)
        elif event_type == "monster":
            battle_messages, winner, status = await pve_fight(
                user_ids_in_battle,
                event_result["monster_data"],
                type_in=0,
                bot_id=bot.self_id,
                level_ratios=exp_ratios,
                attack_buffs=attack_buffs,
            )
            final_statuses = resolve_final_user_statuses(
                status, bot.self_id, exp_ratios
            )
            for member_id, final in final_statuses.items():
                if member_id in member_plan_map:
                    member_plan_map[member_id]["final_hp"] = int(final["hp"])
                    member_plan_map[member_id]["final_mp"] = int(final["mp"])
            summary = f"{event_result.get('description', '')}！"
            if winner == 0:
                reward_message, rewards = build_battle_rewards(
                    user_info,
                    members_info,
                    event_result["monster_data"],
                    status,
                    operation_id,
                )
                for reward in rewards:
                    plan_member = member_plan_map.get(str(reward["user_id"]))
                    if plan_member:
                        plan_member["stone_delta"] = int(reward["stone"])
                        plan_member["exp_delta"] = int(reward["exp"])
                        plan_member["items"] = list(reward.get("items", []))
                summary += f"\n恭喜道友击败敌人。{mentor_buff_msg}{reward_message}"
            else:
                summary += "\n道友不敌，重伤逃遁。"
            resolved = {
                "kind": "monster",
                "winner": int(winner),
                "monsters": event_result["monster_data"],
            }
        elif event_type == "treasure":
            item_id = int(event_result.get("drop_items", 0) or 0)
            item_info = items.get_data_by_item_id(item_id) if item_id else None
            if item_info:
                member_plan_map[user_id]["items"] = [
                    {
                        "id": item_id,
                        "name": item_info["name"],
                        "type": item_info["type"],
                        "amount": 1,
                        "expected_num": int(sql_message.goods_num(user_id, item_id)),
                        "expected_bind_num": int(
                            sql_message.goods_num(user_id, item_id, "bind")
                        ),
                    }
                ]
                summary = (
                    f"{event_result.get('description', '')}，凑近一看居然是{item_info['name']}"
                )
            else:
                summary = f"{event_result.get('description', '')}，宝箱中空空如也"
        elif event_type == "spirit_stone":
            stones = int(event_result.get("stones", 0))
            member_plan_map[user_id]["stone_delta"] = stones
            summary = (
                f"{event_result.get('description', '')}，获得{number_to(stones)}灵石"
            )
        else:
            event_type = "nothing"
            summary = str(event_result.get("description", "无事发生"))

        summary += (
            "！\n"
            + _progress_message(
                current_layer, total_layers, advance=advance, complete=complete
            )
        )
        response = _explore_response(summary, battle_messages)
        if not resolved:
            resolved = {"kind": event_type, "event": event_result}

    plan = {
        "expected_status": dict(player_status),
        "team": team_snapshot,
        "members": member_plans,
        "advance": bool(advance),
        "complete": bool(complete),
        "resolved": resolved,
        "response": response,
    }
    try:
        prepared = dungeon_explore_operation_service.prepare(
            operation_id, user_id, plan
        )
        settled = None
        if prepared.status != "operation_conflict" and prepared.phase != "completed":
            settled = dungeon_explore_operation_service.settle(
                operation_id, user_id, XiuConfig().max_goods_num
            )
    except Exception:
        logger.exception("副本探索事务结算失败")
        await handle_send(bot, event, "副本探索结算失败，请稍后重试。")
        await explore_dungeon.finish()

    if prepared.status == "operation_conflict":
        await handle_send(bot, event, "该探索事件身份冲突，无法结算。")
        await explore_dungeon.finish()
    if prepared.phase == "completed":
        await _send_explore_response(bot, event, prepared.response or {})
        await explore_dungeon.finish()
    if settled is None or settled.phase != "completed":
        await handle_send(bot, event, "副本探索结算失败，请稍后重试。")
        await explore_dungeon.finish()
    await _send_explore_response(bot, event, settled.response or response)
    await explore_dungeon.finish()


# =========================
# 我的副本状态
# =========================
@dungeon_status.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_dungeon_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await dungeon_status.finish()

    user_id = user_info["user_id"]
    player_status = dungeon_manager.get_player_status(user_id)

    name = player_status.get('dungeon_name', '未知')
    status_text = {
        'not_started': '未开始',
        'exploring': '探索中',
        'completed': '已完成',
        'exited': '已退出',
    }.get(player_status.get('dungeon_status', 'not_started'), '未知')
    total = player_status.get('total_layers', 0)
    current = player_status.get('current_layer', 0)

    msg = (
        f"【副本信息】\n"
        f"副本：{name}\n"
        f"状态：{status_text}\n"
        f"层数：{current}/{total}层\n"
        f"进度：{(current / total * 100) if total > 0 else 0:.1f}%"
    )

    await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本信息", v2="副本信息", k3="副本帮助", v3="副本帮助")
    await dungeon_status.finish()
