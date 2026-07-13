import asyncio
import random
import time
from datetime import datetime, timedelta
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
from ..xiuxian_utils.player_fight import pve_fight
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata

from .dungeon_manager import DungeonManager
from .team_manager import (
    remove_member_from_team, disband_team, get_user_team,
    get_team_info, team_invite_cache, expire_team_invite,
    load_teams, save_team
)
from .team_command_service import (
    TeamInviteResponseResult,
    build_invite_response_message,
    build_team_view,
    build_kick_team_message,
    build_kick_team_result,
    build_leave_team_message,
    build_leave_team_result,
    build_team_view_message,
    build_transfer_team_not_member_message,
    build_transfer_team_self_message,
    build_transfer_team_success_message,
    build_team_invite_message,
    build_team_invite_private_message,
    resolve_invite_response,
    resolve_kick_target,
    resolve_team_invite,
    resolve_transfer_target,
)
from .session_service import DungeonSessionService
from .purchase_service import DungeonPurchaseService
from .explore_event_service import DungeonExploreEventService
from .battle_progress_service import DungeonBattleProgressService
from .team_transaction_service import DungeonTeamTransactionService
from ...paths import get_paths

sql_message = XiuxianDateManage()
player_data = PlayerDataManager()
items = Items()
dungeon_session_service = DungeonSessionService(get_paths().player_db)
dungeon_purchase_service = DungeonPurchaseService(get_paths().game_db)
dungeon_explore_event_service = DungeonExploreEventService(get_paths().game_db, get_paths().player_db)
dungeon_battle_progress_service = DungeonBattleProgressService(get_paths().game_db, get_paths().player_db)
dungeon_team_transaction_service = DungeonTeamTransactionService(get_paths().player_db)
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
TEAM_CD_TABLE = "team_cd"
TEAM_JOIN_CD_HOURS = 3


def _now_dt():
    return datetime.now()


def _parse_dt(s: str):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_team_cd_info(user_id: str):
    """获取用户组队冷却信息"""
    uid = str(user_id)
    data = player_data.get_fields(uid, TEAM_CD_TABLE)
    if not data:
        data = {
            "user_id": uid,
            "join_cd_until": "",
            "had_first_join": 0
        }
        for k, v in data.items():
            player_data.update_or_write_data(uid, TEAM_CD_TABLE, k, v)
    return data


def set_team_cd(user_id: str, hours: int = TEAM_JOIN_CD_HOURS):
    """设置组队冷却"""
    uid = str(user_id)
    until = (_now_dt() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    player_data.update_or_write_data(uid, TEAM_CD_TABLE, "join_cd_until", until)
    return until


def set_first_join_flag(user_id: str):
    """标记首次已入队"""
    uid = str(user_id)
    player_data.update_or_write_data(uid, TEAM_CD_TABLE, "had_first_join", 1)


def is_in_team_cd(user_id: str):
    """检查是否在组队冷却中"""
    info = get_team_cd_info(user_id)
    until_str = info.get("join_cd_until", "")
    if not until_str:
        return False, 0
    until_dt = _parse_dt(until_str)
    if not until_dt:
        return False, 0
    now = _now_dt()
    if now >= until_dt:
        return False, 0
    remain = int((until_dt - now).total_seconds())
    return True, remain


def format_seconds(sec: int):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    if h > 0:
        return f"{h}小时{m}分{s}秒"
    if m > 0:
        return f"{m}分{s}秒"
    return f"{s}秒"


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
    group_id = event.group_id if isinstance(event, GroupMessageEvent) else None

    in_cd, remain = is_in_team_cd(user_id)
    if in_cd:
        msg = f"你当前处于组队冷却中，剩余：{format_seconds(remain)}，不可创建队伍。"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await create_team_cmd.finish()

    if not group_id:
        msg = "组队功能只能在群聊中使用！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
        await create_team_cmd.finish()

    existing_team_id = get_user_team(user_id)
    if existing_team_id:
        msg = "你已经在一个队伍中了，请先退出当前队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="离开队伍", v1="离开队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
        await create_team_cmd.finish()

    team_name = args.extract_plain_text().strip()
    if not team_name:
        team_name = f"{user_info['user_name']}的队伍"

    event_id = getattr(event, "message_id", None)
    operation_id = f"dungeon-team-create:{event_id}:{user_id}" if event_id else f"dungeon-team-create:{time.time_ns()}:{user_id}"
    team_id = f"{group_id}_{operation_id.rsplit(':', 2)[-2]}"
    result = dungeon_team_transaction_service.create(
        operation_id, team_id, team_name, user_id, group_id,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    if result.status not in {"applied", "duplicate"}:
        messages = {
            "user_missing": "未找到道友数据，创建队伍失败！",
            "user_has_team": "你已经在一个队伍中了，请先退出当前队伍！",
            "session_active": "副本探索会话进行中，无法创建队伍！",
            "state_changed": "队伍状态已变化，请稍后重试。",
        }
        await handle_send(bot, event, messages.get(result.status, "创建队伍失败！"), md_type="team", k1="队伍帮助", v1="队伍帮助")
        await create_team_cmd.finish()
    team_id = result.team_id

    msg = f"🎉 队伍【{team_name}】创建成功！\n队伍ID：{team_id}\n👑 队长：{user_info['user_name']}\n📢 使用【邀请组队 道号】来邀请其他人加入！"
    await handle_send(bot, event, msg, md_type="team", k1="邀请组队", v1="邀请组队", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await create_team_cmd.finish()


@invite_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def invite_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await invite_team_cmd.finish()

    user_id = str(user_info['user_id'])
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你还没有创建或加入任何队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info:
        msg = "队伍信息异常！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await invite_team_cmd.finish()

    if team_info['leader'] != user_id:
        msg = "只有队长才能邀请成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    if len(team_info['members']) >= team_info['max_members']:
        msg = f"队伍已满（{len(team_info['members'])}/{team_info['max_members']}），无法邀请新成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    arg = args.extract_plain_text().strip()
    target_user_id = None

    target_user_id = get_at_user_id(args)

    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    target_user_info = None
    if target_user_id:
        is_target_user, target_user_info, target_msg = check_user(target_user_id)
        if not is_target_user:
            await handle_send(bot, event, target_msg)
            await invite_team_cmd.finish()

    in_cd, remain = is_in_team_cd(target_user_id) if target_user_id else (False, 0)
    target_team = get_user_team(target_user_id) if target_user_id else None
    pending_inviter_name = None
    pending_remaining_time = 0
    if target_user_id and target_user_id in team_invite_cache:
        inviter_id = team_invite_cache[target_user_id]['inviter']
        inviter_info = sql_message.get_user_info_with_id(inviter_id)
        pending_inviter_name = inviter_info['user_name'] if inviter_info else "未知用户"
        pending_remaining_time = int(
            60 - (datetime.now().timestamp() - team_invite_cache[target_user_id]['timestamp'])
        )

    invite_result = resolve_team_invite(
        target_user_id=target_user_id,
        target_user_name=(target_user_info or {}).get("user_name"),
        cooldown_seconds=remain if in_cd else 0,
        target_team_id=target_team,
        pending_inviter_name=pending_inviter_name,
        pending_remaining_seconds=pending_remaining_time,
    )
    if invite_result.status != "ready":
        msg = build_team_invite_message(invite_result, format_seconds)
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await invite_team_cmd.finish()

    invite_id = f"{team_id}_{target_user_id}_{datetime.now().timestamp()}"
    team_invite_cache[target_user_id] = {
        'team_id': team_id,
        'inviter': user_id,
        'timestamp': datetime.now().timestamp(),
        'invite_id': invite_id,
        'group_id': event.group_id
    }
    dungeon_team_transaction_service.record_invite(
        invite_id, team_id, user_id, target_user_id, event.group_id,
        team_invite_cache[target_user_id]['timestamp'] + 60,
    )

    asyncio.create_task(expire_team_invite(target_user_id, invite_id, bot, event))

    target_name = invite_result.target_user_name
    msg = build_team_invite_message(invite_result, format_seconds)
    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")

    try:
        if isinstance(event, GroupMessageEvent):
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

    invite_data = team_invite_cache.get(user_id)
    if invite_data is None:
        msg = build_invite_response_message(
            resolve_invite_response(
                has_invite=False,
                invite_group_id=None,
                current_group_id=None,
                team_exists=False,
                user_has_team=False,
                member_count=0,
                max_members=0,
            )
        )
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    team_id = invite_data['team_id']
    inviter_id = invite_data['inviter']
    invite_group_id = invite_data['group_id']

    team_info = get_team_info(team_id)
    response_result = resolve_invite_response(
        has_invite=True,
        invite_group_id=str(invite_group_id),
        current_group_id=(str(event.group_id) if isinstance(event, GroupMessageEvent) else None),
        team_exists=bool(team_info),
        user_has_team=bool(get_user_team(user_id)),
        member_count=len(team_info['members']) if team_info else 0,
        max_members=int(team_info['max_members']) if team_info else 0,
    )
    if response_result.status != "ready":
        msg = build_invite_response_message(response_result)
        if response_result.status in {"team_disbanded", "user_has_team", "team_full"}:
            del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    if not team_info:
        del team_invite_cache[user_id]
        await agree_team_cmd.finish()

    event_id = getattr(event, "message_id", None)
    operation_id = f"dungeon-team-join:{event_id}:{user_id}" if event_id else f"dungeon-team-join:{time.time_ns()}:{user_id}"
    join_result = dungeon_team_transaction_service.join(
        operation_id, invite_data['invite_id'], team_id, inviter_id, user_id,
        invite_group_id, datetime.now().timestamp(),
    )

    if join_result.status in {"applied", "duplicate"}:
        cd_info = get_team_cd_info(user_id)
        if int(cd_info.get("had_first_join", 0)) == 0:
            set_first_join_flag(user_id)

        del team_invite_cache[user_id]

        inviter_info = sql_message.get_user_info_with_id(inviter_id)

        msg = build_invite_response_message(
            TeamInviteResponseResult(
                "joined",
                team_name=join_result.team_name or team_info['team_name'],
                leader_name=inviter_info['user_name'],
                member_count=join_result.member_count or len(team_info['members']) + 1,
                max_members=join_result.max_members or team_info['max_members'],
            )
        )
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="副本信息", v2="副本信息", k3="队伍帮助", v3="队伍帮助")

        try:
            if team_info['group_id'] and team_info['group_id'] != str(getattr(event, "group_id", "")):
                await delivery_service.send_to_group(
                    bot,
                    str(team_info['group_id']),
                    f"你的队伍【{team_info['team_name']}】加入了新成员：{user_info['user_name']}！",
                )
        except Exception as e:
            logger.warning(f"通知队长失败: {e}")
    else:
        status_results = {
            "team_disbanded": TeamInviteResponseResult("team_disbanded"),
            "user_has_team": TeamInviteResponseResult("user_has_team"),
            "team_full": TeamInviteResponseResult("team_full"),
        }
        if join_result.status in {"invite_invalid", "user_missing", "session_active", "state_changed"}:
            msg = "邀请、成员或副本会话状态已变化，加入队伍失败！"
        else:
            msg = build_invite_response_message(status_results.get(join_result.status, TeamInviteResponseResult("join_failed")))
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")

    await agree_team_cmd.finish()


@reject_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def reject_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await reject_team_cmd.finish()

    user_id = str(user_info['user_id'])

    invite_data = team_invite_cache.get(user_id)
    if invite_data is None:
        msg = build_invite_response_message(TeamInviteResponseResult("no_invite"))
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await reject_team_cmd.finish()

    invite_group_id = invite_data['group_id']

    if isinstance(event, GroupMessageEvent) and event.group_id != invite_group_id:
        msg = build_invite_response_message(
            TeamInviteResponseResult("wrong_group", invite_group_id=str(invite_group_id))
        )
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await reject_team_cmd.finish()

    del team_invite_cache[user_id]

    msg = build_invite_response_message(TeamInviteResponseResult("rejected"))
    await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
    await reject_team_cmd.finish()


@leave_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def leave_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await leave_team_cmd.finish()

    user_id = str(user_info['user_id'])

    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await leave_team_cmd.finish()

    team_info = get_team_info(team_id)
    success = remove_member_from_team(team_id, user_id)

    if success:
        cd_info = get_team_cd_info(user_id)
        if int(cd_info.get("had_first_join", 0)) == 1:
            set_team_cd(user_id, TEAM_JOIN_CD_HOURS)

        new_leader_name = None
        if user_id == team_info['leader']:
            new_team_info = get_team_info(team_id)
            if new_team_info:
                new_leader_name = sql_message.get_user_info_with_id(new_team_info['leader'])['user_name']
        leave_result = build_leave_team_result(
            team_info=team_info,
            leaver_user_id=user_id,
            success=True,
            cooldown_hours=TEAM_JOIN_CD_HOURS,
            new_leader_name=new_leader_name,
        )
    else:
        leave_result = build_leave_team_result(
            team_info=team_info,
            leaver_user_id=user_id,
            success=False,
            cooldown_hours=TEAM_JOIN_CD_HOURS,
            new_leader_name=None,
        )
    msg = build_leave_team_message(leave_result)

    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v3="队伍帮助")
    await leave_team_cmd.finish()


@kick_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def kick_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await kick_team_cmd.finish()

    user_id = str(user_info['user_id'])

    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info or team_info['leader'] != user_id:
        msg = "只有队长才能踢出成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()

    arg = args.extract_plain_text().strip()
    target_user_id = None

    target_user_id = get_at_user_id(args)

    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    kick_result = resolve_kick_target(
        actor_user_id=user_id,
        team_info=team_info,
        at_target_user_id=target_user_id,
        arg_target_user_id=None,
        lookup_user_name=lambda candidate_user_id: (
            (sql_message.get_user_info_with_id(candidate_user_id) or {}).get("user_name")
        ),
    )

    if kick_result.status == "target_not_found":
        msg = "未找到指定的成员！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()
    if kick_result.status == "self_target":
        msg = "不能踢出自己！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()
    if kick_result.status == "target_not_member":
        msg = "该成员不在你的队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()
    if kick_result.status == "target_info_missing":
        msg = "目标成员信息异常，无法踢出。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()

    target_user_id = kick_result.target_user_id
    success = remove_member_from_team(team_id, target_user_id)

    if success:
        cd_info = get_team_cd_info(target_user_id)
        if int(cd_info.get("had_first_join", 0)) == 1:
            set_team_cd(target_user_id, TEAM_JOIN_CD_HOURS)
    kick_result = build_kick_team_result(
        target_user_id=target_user_id,
        target_user_name=kick_result.target_user_name,
        success=success,
        cooldown_hours=TEAM_JOIN_CD_HOURS,
    )
    msg = build_kick_team_message(kick_result)

    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
    await kick_team_cmd.finish()


@disband_team_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def disband_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await disband_team_cmd.finish()

    user_id = str(user_info['user_id'])

    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await disband_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info or team_info['leader'] != user_id:
        msg = "只有队长才能解散队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await disband_team_cmd.finish()

    members = team_info.get("members", [])[:]
    for mid in members:
        cd_info = get_team_cd_info(mid)
        if int(cd_info.get("had_first_join", 0)) == 1:
            set_team_cd(mid, TEAM_JOIN_CD_HOURS)

    success = disband_team(team_id)

    if success:
        msg = f"队伍【{team_info['team_name']}】已解散。\n全体成员进入{TEAM_JOIN_CD_HOURS}小时组队冷却。"
    else:
        msg = "解散队伍失败！"

    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
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

    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info:
        msg = "队伍信息异常！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await transfer_team_cmd.finish()

    if team_info['leader'] != user_id:
        msg = "只有队长才能转移队长职位！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()

    arg = args.extract_plain_text().strip()
    target_user_id = None

    target_user_id = get_at_user_id(args)

    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    transfer_result = resolve_transfer_target(
        actor_user_id=user_id,
        team_info=team_info,
        at_target_user_id=target_user_id,
        arg_target_user_id=None,
        lookup_user_name=lambda candidate_user_id: (
            (sql_message.get_user_info_with_id(candidate_user_id) or {}).get("user_name")
        ),
    )

    if transfer_result.status == "target_not_found":
        msg = "未找到指定成员，请检查道号或艾特是否正确！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()
    if transfer_result.status == "self_target":
        msg = build_transfer_team_self_message()
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()
    if transfer_result.status == "target_not_member":
        msg = build_transfer_team_not_member_message()
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="邀请组队", v2="邀请组队", k3="队伍帮助", v3="队伍帮助")
        await transfer_team_cmd.finish()
    if transfer_result.status == "target_info_missing":
        msg = "目标成员信息异常，无法转移。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await transfer_team_cmd.finish()

    target_user_id = transfer_result.target_user_id
    team_info["leader"] = target_user_id
    save_team(team_info)

    msg = build_transfer_team_success_message(transfer_result.target_user_name)
    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="探索副本", v2="探索副本", k3="队伍帮助", v3="队伍帮助")
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
        return {member_ids[0]: 0.5}

    fixed = {}
    fixed_pool = 0.0
    for uid in member_ids:
        if uid == str(leader_id):
            fixed[uid] = 0.20
        else:
            fixed[uid] = 0.10
        fixed_pool += fixed[uid]

    remain_pool = max(0.0, 1.0 - fixed_pool)

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
                dist[uid] += remain_pool * (contrib[uid] / contrib_sum)

    cap = 0.5
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


def battle_settlement(user_info, members_info, monsters_list, status_list, operation_id, player_status, complete=False):
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
        return "\n副本奖励：无，本次战斗无人获得奖励。"

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
            reward_items.append({"id": item_id, "name": item_info['name'], "type": item_info['type'], "amount": 1})
            rewards_msg.append(f"{item_info['name']}")

        for item in reward_items:
            item["expected_num"] = int(sql_message.goods_num(uid, item["id"]))
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
        alloc_percent = ratio * 100

        rewards_msg_str = "无" if not rewards_msg else "、".join(rewards_msg)
        msg += f"\n{m_info['user_name']}（贡献{contrib_percent:.2f}% / 分配{alloc_percent:.2f}%）获得：{rewards_msg_str}"

    settlement = dungeon_battle_progress_service.settle(
        operation_id, user_info["user_id"], player_status, rewards, complete, XiuConfig().max_goods_num
    )
    if not settlement.succeeded:
        return "\n副本奖励结算失败，请稍后重试。"
    return msg


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
        dungeon_manager.reset_dungeon()
        dungeon_info = dungeon_manager.get_dungeon_progress()
        logger.info(
            f"每日副本重置完成: {dungeon_info.get('name', '未知副本')} | "
            f"层数={dungeon_info.get('total_layers', 0)} | "
            f"日期={dungeon_info.get('date', '未知')}"
        )
    except Exception as e:
        logger.exception(f"每日副本重置任务执行失败: {e}")


# =========================
# 手动重置
# =========================
@reset_command.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_manual_reset(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    try:
        dungeon_manager.reset_dungeon()
        dungeon_info = dungeon_manager.get_dungeon_progress()
        msg = (
            f"✅ 副本和所有玩家的副本进度已重置。\n"
            f"当前副本：{dungeon_info.get('name', '未知副本')}\n"
            f"总层数：{dungeon_info.get('total_layers', 0)}"
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
    shop_item = DUNGEON_SHOP.get(item_id)
    item_info = items.get_data_by_item_id(item_id)
    if shop_item is None or item_info is None or quantity <= 0:
        await handle_send(bot, event, "副本商店中没有该商品，或数量无效。")
        await dungeon_purchase.finish()
    event_id = getattr(event, "message_id", None)
    operation_id = f"dungeon-purchase:{event_id}:{user_info['user_id']}" if event_id else f"dungeon-purchase:{time.time_ns()}:{user_info['user_id']}"
    result = dungeon_purchase_service.purchase(operation_id, user_info["user_id"], item_id, item_info["name"], item_info.get("type", item_info.get("item_type", "道具")), quantity, shop_item["cost"], user_info["stone"], XiuConfig().max_goods_num, 1)
    messages = {"stone_insufficient": "灵石不足，无法兑换。", "inventory_full": "背包中该物品数量已达上限。", "state_changed": "兑换状态已变化，请稍后重试。", "user_missing": "未找到道友数据，兑换失败。"}
    if result.status not in {"applied", "duplicate"}:
        await handle_send(bot, event, messages.get(result.status, "兑换失败。"))
        await dungeon_purchase.finish()
    await handle_send(bot, event, f"成功兑换{item_info['name']}×{result.quantity}，消耗{number_to(result.cost)}灵石。")
    await dungeon_purchase.finish()


@dungeon_exit.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_dungeon_exit(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await dungeon_exit.finish()
    user_id = user_info["user_id"]
    status = dungeon_manager.get_player_status(user_id)
    dungeon = dungeon_manager.get_dungeon_progress()
    event_id = getattr(event, "message_id", None)
    operation_id = f"dungeon-exit:{event_id}:{user_id}" if event_id else f"dungeon-exit:{time.time_ns()}:{user_id}"
    result = dungeon_session_service.exit(operation_id, user_id, status, {"dungeon_id": status["dungeon_id"], "date": dungeon["date"]})
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

    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await explore_dungeon.finish()

    user_id = user_info["user_id"]
    event_id = getattr(event, "message_id", None)
    operation_base = f"dungeon-reward:{event_id}:{user_id}" if event_id else f"dungeon-reward:{time.time_ns()}:{user_id}"
    user_exp = user_info["exp"]

    player_status = dungeon_manager.get_player_status(user_id)
    if player_status["dungeon_status"] == "completed":
        msg = f"今日副本已完成，请等待明日刷新！"
        await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
        await explore_dungeon.finish()

    dungeon = dungeon_manager.get_dungeon_progress()
    entry_operation = f"dungeon-entry:{event_id}:{user_id}" if event_id else f"dungeon-entry:{time.time_ns()}:{user_id}"
    entry_result = dungeon_session_service.enter(entry_operation, user_id, player_status, {"dungeon_id": player_status["dungeon_id"], "date": dungeon["date"]})
    if entry_result.status == "state_changed":
        await handle_send(bot, event, "副本进入状态已变化，请稍后重试。")
        await explore_dungeon.finish()
    if entry_result.status == "completed":
        await handle_send(bot, event, "今日副本已完成，请等待明日刷新！")
        await explore_dungeon.finish()
    player_status = dict(player_status)
    player_status["dungeon_status"] = "exploring"

    user_ids_in_battle = [user_id]
    exp_ratios = None
    mentor_attack_buffs = {}
    mentor_buff_msg = ""

    team_id = get_user_team(str(user_id))
    members_info = [user_info]
    if team_id and (team_info := get_team_info(team_id)):
        if team_info['leader'] != str(user_id):
            msg = "你不是队长，无法带领队伍探索副本！"
            await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
            await explore_dungeon.finish()

        members_info = [
            sql_message.get_user_info_with_id(member_id)
            for member_id in team_info["members"]
        ]
        members_info = [m for m in members_info if m is not None]
        user_ids_in_battle = [member["user_id"] for member in members_info]

        if user_exp > 0:
            exp_ratios = {
                member["user_id"]: max(0.5, min(1.0, user_exp / member["exp"])) if member["exp"] > 0 else 1.0
                for member in members_info
            }
        else:
            exp_ratios = {member["user_id"]: 1.0 for member in members_info}

        from ..xiuxian_buff.partner import get_mentor_team_attack_buffs
        mentor_attack_buffs = get_mentor_team_attack_buffs(user_ids_in_battle)
        if mentor_attack_buffs:
            buff_names = []
            for member in members_info:
                if str(member["user_id"]) in mentor_attack_buffs:
                    buff_names.append(member["user_name"])
            mentor_buff_msg = f"\n师徒羁绊「薪火相承」触发：{', '.join(buff_names)}攻击提升10%。"

    for user in members_info:
        passed, message = check_user_state(user)
        if passed:
            await handle_send(bot, event, message, md_type="副本", k1="我的修仙信息", v1="我的修仙信息")
            await explore_dungeon.finish()

    current_layer = player_status["current_layer"]
    total_layers = player_status["total_layers"]

    if current_layer == total_layers - 1:
        boss_info = dungeon_manager.get_boss_data(user_info['level'], user_exp)
        result, winner, status = await pve_fight(
            user_ids_in_battle,
            boss_info,
            bot_id=bot.self_id,
            level_ratios=exp_ratios,
            attack_buffs={
                str(uid): data["attack_multiplier"]
                for uid, data in mentor_attack_buffs.items()
            },
        )

        if winner == 0:
            msg = f"恭喜道友击败【{boss_info[0]['name']}】！"
            msg += mentor_buff_msg
            msg += battle_settlement(user_info, members_info, boss_info, status, f"{operation_base}:boss:{current_layer}", player_status, complete=True)
        else:
            msg = f"道友不敌【{boss_info[0]['name']}】，重伤逃遁。"

        try:
            await send_msg_handler(bot, event, result)
        except Exception:
            msg += f"\n对战消息发送错误，可能被风控！"

        await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
        await explore_dungeon.finish()

    event_result = dungeon_manager.trigger_event(user_info['level'], user_exp)

    non_combat_event = None
    non_combat_members = []
    if event_result["type"] == "trap":
        msg_parts = [f"{event_result.get('description', '')}"]
        for user in members_info:
            costhp = int((user['exp'] / 2) * event_result.get('damage', 0.1))
            non_combat_members.append({"user_id": user["user_id"], "expected_hp": user["hp"], "expected_mp": user["mp"], "hp_delta": -costhp})
            msg_parts.append(f"{user['user_name']}气血减少：{number_to(costhp)}")
        non_combat_event = {"type": "trap"}
        msg = "，".join(msg_parts)

    elif event_result["type"] == "monster":
        msg = f"{event_result.get('description', '')}！"
        result, winner, status = await pve_fight(
            user_ids_in_battle,
            event_result["monster_data"],
            bot_id=bot.self_id,
            level_ratios=exp_ratios,
            attack_buffs={
                str(uid): data["attack_multiplier"]
                for uid, data in mentor_attack_buffs.items()
            },
        )

        if winner == 0:
            msg += f"\n恭喜道友击败敌人。"
            msg += mentor_buff_msg
            msg += battle_settlement(user_info, members_info, event_result["monster_data"], status, f"{operation_base}:monster:{current_layer}", player_status)
        else:
            msg += f"\n道友不敌，重伤逃遁。"
            settlement = dungeon_battle_progress_service.settle(
                f"{operation_base}:monster:{current_layer}:loss", user_id, player_status,
                [{"user_id": user_id, "expected_stone": int(user_info["stone"]), "expected_exp": int(user_info["exp"]), "stone": 0, "exp": 0, "items": []}],
                False, XiuConfig().max_goods_num,
            )
            if not settlement.succeeded:
                msg += "\n副本进度结算失败，请稍后重试。"

        try:
            await send_msg_handler(bot, event, result)
        except Exception:
            msg += f"\n对战消息发送错误，可能被风控！"

    elif event_result["type"] == "treasure":
        item_id = event_result.get('drop_items', 9001)
        item_info = items.get_data_by_item_id(item_id)
        non_combat_event = {"type": "treasure", "item": {"id": item_id, "name": item_info["name"], "type": item_info["type"], "amount": 1, "expected_num": int(sql_message.goods_num(user_id, item_id))}}
        non_combat_members = [{"user_id": user_id, "expected_hp": user_info["hp"], "expected_mp": user_info["mp"]}]
        msg = f"{event_result.get('description', '')}，凑近一看居然是{item_info['name']}"

    elif event_result["type"] == "spirit_stone":
        stones = int(event_result.get('stones', 0))
        msg = f"{event_result.get('description', '')}，获得{number_to(stones)}灵石"
        non_combat_event = {"type": "spirit_stone", "stone": stones, "expected_stone": int(user_info["stone"])}
        non_combat_members = [{"user_id": user_id, "expected_hp": user_info["hp"], "expected_mp": user_info["mp"]}]

    else:
        msg = f"{event_result.get('description', '')}"
        non_combat_event = {"type": "nothing"}
        non_combat_members = [{"user_id": user_id, "expected_hp": user_info["hp"], "expected_mp": user_info["mp"]}]

    msg += "！\n"
    if non_combat_event is not None:
        settlement = dungeon_explore_event_service.settle(f"{operation_base}:event:{current_layer}", user_id, player_status, non_combat_event, non_combat_members, XiuConfig().max_goods_num)
        if not settlement.succeeded:
            await handle_send(bot, event, "副本事件结算状态已变化，请稍后重试。")
            await explore_dungeon.finish()

    updated_player_status = dungeon_manager.get_player_status(user_id)
    current_layer_after_update = updated_player_status["current_layer"]
    total_layers_after_update = updated_player_status["total_layers"]

    if updated_player_status["dungeon_status"] == "completed":
        msg += f"恭喜你已完成今日副本！"
    else:
        msg += f"当前：第{current_layer_after_update + 1}层\n"
        msg += "使用「探索副本」进入下一层！"

    await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
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
        'completed': '已完成'
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
