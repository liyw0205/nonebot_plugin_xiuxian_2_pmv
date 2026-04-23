import asyncio
import random
from datetime import datetime, timedelta
from typing import Union, Any

from nonebot import on_command, require
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.log import logger

from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    Message,
    MessageSegment
)

from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, leave_harm_time, OtherSet, PlayerDataManager
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, number_to, check_user_type, _impersonating_users
from ..xiuxian_utils.player_fight import pve_fight
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata

from .dungeon_manager import DungeonManager
from .team_manager import (
    create_team, add_member_to_team,
    remove_member_from_team, disband_team, get_user_team,
    get_team_info, team_invite_cache, expire_team_invite,
    load_teams, save_team
)

sql_message = XiuxianDateManage()
player_data = PlayerDataManager()
items = Items()

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
副本帮助 - 显示本帮助信息
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
组队帮助 - 查看指令
""".strip()


# =========================
# 帮助
# =========================
@help_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def help_team_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __team_help__
    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await help_team_cmd.finish()


@help_dungeon_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def help_dungeon_cmd_(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __dungeon_help__
    await handle_send(bot, event, msg, md_type="副本", k1="副本信息", v1="副本信息", k2="探索副本", v2="探索副本", k3="副本状态", v3="我的副本状态")
    await help_dungeon_cmd.finish()


# =========================
# 组队逻辑
# =========================
@create_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    team_id = create_team(team_name, user_id, group_id)

    msg = f"🎉 队伍【{team_name}】创建成功！\n队伍ID：{team_id}\n👑 队长：{user_info['user_name']}\n📢 使用【邀请组队 道号】来邀请其他人加入！"
    await handle_send(bot, event, msg, md_type="team", k1="邀请组队", v1="邀请组队", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await create_team_cmd.finish()


@invite_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    for arg_item in args:
        if arg_item.type == "at":
            target_user_id = str(arg_item.data.get("qq", ""))
            break

    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    if not target_user_id:
        msg = "未找到指定的用户，请检查道号或艾特是否正确！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await invite_team_cmd.finish()

    is_target_user, target_user_info, target_msg = check_user(target_user_id)
    if not is_target_user:
        await handle_send(bot, event, target_msg)
        await invite_team_cmd.finish()

    in_cd, remain = is_in_team_cd(target_user_id)
    if in_cd:
        target_name = target_user_info['user_name']
        msg = f"{target_name}当前处于组队冷却中（剩余{format_seconds(remain)}），不可被邀请。"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    target_team = get_user_team(target_user_id)
    if target_team:
        target_name = target_user_info['user_name']
        msg = f"{target_name}已有队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    if target_user_id in team_invite_cache:
        inviter_id = team_invite_cache[target_user_id]['inviter']
        inviter_info = sql_message.get_user_info_with_id(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - team_invite_cache[target_user_id]['timestamp'])
        msg = f"对方已有来自{inviter_info['user_name']}的组队邀请（剩余{int(remaining_time)}秒），请稍后再试！"
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

    asyncio.create_task(expire_team_invite(target_user_id, invite_id, bot, event))

    target_name = target_user_info['user_name']
    msg = f"📨 已向{target_name}发送组队邀请，等待对方回应..."
    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")

    try:
        if isinstance(event, GroupMessageEvent):
            await bot.send_private_msg(
                user_id=str(target_user_id),
                message=f"你在群{event.group_id}收到了来自{user_info['user_name']}的组队邀请，请在1分钟内回复【同意组队】或【拒绝组队】。"
            )
    except Exception as e:
        logger.warning(f"私聊通知被邀请者失败: {e}")

    await invite_team_cmd.finish()


@agree_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def agree_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await agree_team_cmd.finish()

    user_id = str(user_info['user_id'])

    if user_id not in team_invite_cache:
        msg = "没有待处理的组队邀请！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    invite_data = team_invite_cache[user_id]
    team_id = invite_data['team_id']
    inviter_id = invite_data['inviter']
    invite_group_id = invite_data['group_id']

    if isinstance(event, GroupMessageEvent) and event.group_id != invite_group_id:
        msg = f"此邀请是在群{invite_group_id}发出的，请在该群或私聊中进行操作。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info:
        msg = "该队伍已解散！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    if get_user_team(user_id):
        msg = "你已经在一个队伍中了，无法接受邀请！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="离开队伍", v1="离开队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
        await agree_team_cmd.finish()

    if len(team_info['members']) >= team_info['max_members']:
        msg = "该队伍已满员！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await agree_team_cmd.finish()

    success = add_member_to_team(team_id, user_id)

    if success:
        cd_info = get_team_cd_info(user_id)
        if int(cd_info.get("had_first_join", 0)) == 0:
            set_first_join_flag(user_id)

        del team_invite_cache[user_id]

        inviter_info = sql_message.get_user_info_with_id(inviter_id)

        msg = f"✅ 你已成功加入队伍【{team_info['team_name']}】！\n👑 队长：{inviter_info['user_name']}\n👥 当前成员：{len(team_info['members'])}/{team_info['max_members']}"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="副本信息", v2="副本信息", k3="队伍帮助", v3="队伍帮助")

        try:
            if team_info['group_id'] and team_info['group_id'] != str(getattr(event, "group_id", "")):
                await bot.send_group_msg(group_id=str(team_info['group_id']), message=f"你的队伍【{team_info['team_name']}】加入了新成员：{user_info['user_name']}！")
        except Exception as e:
            logger.warning(f"通知队长失败: {e}")
    else:
        msg = "加入队伍失败！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")

    await agree_team_cmd.finish()


@reject_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def reject_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await reject_team_cmd.finish()

    user_id = str(user_info['user_id'])

    if user_id not in team_invite_cache:
        msg = "没有待处理的组队邀请！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await reject_team_cmd.finish()

    invite_data = team_invite_cache[user_id]
    invite_group_id = invite_data['group_id']

    if isinstance(event, GroupMessageEvent) and event.group_id != invite_group_id:
        msg = f"此邀请是在群{invite_group_id}发出的，请在该群或私聊中进行操作。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await reject_team_cmd.finish()

    del team_invite_cache[user_id]

    msg = "已拒绝组队邀请。"
    await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
    await reject_team_cmd.finish()


@leave_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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

        if user_id == team_info['leader']:
            new_team_info = get_team_info(team_id)
            if new_team_info:
                new_leader_name = sql_message.get_user_info_with_id(new_team_info['leader'])['user_name']
                msg = f"你已离开队伍【{team_info['team_name']}】，队长已转让给{new_leader_name}。\n你进入了{TEAM_JOIN_CD_HOURS}小时组队冷却。"
            else:
                msg = f"你已离开队伍【{team_info['team_name']}】，队伍已解散。\n你进入了{TEAM_JOIN_CD_HOURS}小时组队冷却。"
        else:
            msg = f"你已离开队伍【{team_info['team_name']}】。\n你进入了{TEAM_JOIN_CD_HOURS}小时组队冷却。"
    else:
        msg = "离开队伍失败！"

    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v3="队伍帮助")
    await leave_team_cmd.finish()


@kick_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    for arg_item in args:
        if arg_item.type == "at":
            target_user_id = str(arg_item.data.get("qq", ""))
            break

    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    if not target_user_id:
        msg = "未找到指定的成员！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()

    if target_user_id == user_id:
        msg = "不能踢出自己！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()

    if target_user_id not in team_info['members']:
        msg = "该成员不在你的队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()

    success = remove_member_from_team(team_id, target_user_id)

    if success:
        cd_info = get_team_cd_info(target_user_id)
        if int(cd_info.get("had_first_join", 0)) == 1:
            set_team_cd(target_user_id, TEAM_JOIN_CD_HOURS)

        target_info = sql_message.get_user_info_with_id(target_user_id)
        msg = f"已将成员{target_info['user_name']}踢出队伍。\n对方进入{TEAM_JOIN_CD_HOURS}小时组队冷却。"
    else:
        msg = "踢出成员失败！"

    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
    await kick_team_cmd.finish()


@disband_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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


@view_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    members_info_str = []
    for member_id in team_info['members']:
        member_db_info = sql_message.get_user_info_with_id(member_id)
        member_name = member_db_info['user_name'] if member_db_info else f"未知用户({member_id})"
        if member_id == team_info['leader']:
            members_info_str.append(f"👑 {member_name}")
        else:
            members_info_str.append(f"👤 {member_name}")

    members_str_formatted = "\n".join(members_info_str)

    msg = (
        f"══════ 队伍信息 ════\n"
        f"🏷️ 队伍名：{team_info['team_name']}\n"
        f"🆔 队伍ID：{team_info['team_id']}\n"
        f"📅 创建时间：{team_info['create_time']}\n"
        f"👥 成员 ({len(team_info['members'])}/{team_info['max_members']})：\n"
        f"{members_str_formatted}\n"
        f"═════════════"
    )

    await handle_send(bot, event, msg, md_type="team", k1="探索副本", v1="探索副本", k2="离开队伍", v2="离开队伍", k3="队伍帮助", v3="队伍帮助")
    await view_team_cmd.finish()


@transfer_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    for arg_item in args:
        if arg_item.type == "at":
            target_user_id = str(arg_item.data.get("qq", ""))
            break

    if not target_user_id and arg:
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    if not target_user_id:
        msg = "未找到指定成员，请检查道号或艾特是否正确！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()

    if target_user_id == user_id:
        msg = "你已经是队长了，无需转移给自己。"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await transfer_team_cmd.finish()

    if target_user_id not in team_info.get("members", []):
        msg = "只能将队长转移给当前队伍内的成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="邀请组队", v2="邀请组队", k3="队伍帮助", v3="队伍帮助")
        await transfer_team_cmd.finish()

    target_info = sql_message.get_user_info_with_id(target_user_id)
    if not target_info:
        msg = "目标成员信息异常，无法转移。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await transfer_team_cmd.finish()

    team_info["leader"] = target_user_id
    save_team(team_info)

    msg = f"👑 队长已成功转移给 {target_info['user_name']}！"
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


def battle_settlement(user_info, members_info, monsters_list, status_list):
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
            sql_message.update_ls(uid, stone_final, 1)
            rewards_msg.append(f"灵石{number_to(stone_final)}")

        if exp_final > 0:
            sql_message.update_exp(uid, exp_final)
            sql_message.update_power2(uid)
            rewards_msg.append(f"修为{number_to(exp_final)}")

        if uid == leader_id and item_ids:
            item_id = random.choice(item_ids)
            item_info = items.get_data_by_item_id(item_id)
            sql_message.send_back(uid, item_id, item_info['name'], item_info['type'], 1)
            rewards_msg.append(f"{item_info['name']}")

        total_dmg = sum(dmg_map.values()) if sum(dmg_map.values()) > 0 else 1
        contrib_percent = dmg_map.get(uid, 0) / total_dmg * 100
        alloc_percent = ratio * 100

        rewards_msg_str = "无" if not rewards_msg else "、".join(rewards_msg)
        msg += f"\n{m_info['user_name']}（贡献{contrib_percent:.2f}% / 分配{alloc_percent:.2f}%）获得：{rewards_msg_str}"

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
@scheduler.scheduled_job("cron", hour=0, minute=1)
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
@reset_command.handle(parameterless=[Cooldown(cd_time=1.4)])
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
@dungeon_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_dungeon_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    dungeon_data = dungeon_manager.get_dungeon_progress()

    msg = (
        "═══  ✨ 今日副本 ✨  ═══\n"
        f"{dungeon_data['name']}\n"
        f"\n> {dungeon_data['description']}\n\n"
        f"副本类型：{dungeon_data.get('type', 'explore')}\n"
        f"总层数：{dungeon_data['total_layers']}层\n"
        f"副本日期：{dungeon_data['date']}\n"
        "═════════════\n"
        "🎮 使用「探索副本」指令开始冒险！"
    )

    await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
    await dungeon_info.finish()


# =========================
# 探索副本
# =========================
@explore_dungeon.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_explore_dungeon(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await explore_dungeon.finish()

    user_id = user_info["user_id"]
    user_exp = user_info["exp"]

    player_status = dungeon_manager.get_player_status(user_id)
    if player_status["dungeon_status"] == "completed":
        msg = f"今日副本已完成，请等待明日刷新！"
        await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
        await explore_dungeon.finish()

    user_ids_in_battle = [user_id]
    exp_ratios = None

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

    for user in members_info:
        passed, message = check_user_state(user)
        if passed:
            await handle_send(bot, event, message, md_type="副本", k1="我的修仙信息", v1="我的修仙信息")
            await explore_dungeon.finish()

    current_layer = player_status["current_layer"]
    total_layers = player_status["total_layers"]

    if current_layer == total_layers - 1:
        boss_info = dungeon_manager.get_boss_data(user_info['level'], user_exp)
        result, winner, status = await pve_fight(user_ids_in_battle, boss_info, bot_id=bot.self_id, level_ratios=exp_ratios)

        if winner == 0:
            msg = f"恭喜道友击败【{boss_info[0]['name']}】！"
            msg += battle_settlement(user_info, members_info, boss_info, status)
            dungeon_manager.update_player_progress(user_id, status="completed")
        else:
            msg = f"道友不敌【{boss_info[0]['name']}】，重伤逃遁。"

        try:
            await send_msg_handler(bot, event, result)
        except Exception:
            msg += f"\n对战消息发送错误，可能被风控！"

        await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
        await explore_dungeon.finish()

    event_result = dungeon_manager.trigger_event(user_info['level'], user_exp)

    if event_result["type"] == "trap":
        msg_parts = [f"{event_result.get('description', '')}"]
        for user in members_info:
            costhp = int((user['exp'] / 2) * event_result.get('damage', 0.1))
            sql_message.update_user_hp_mp(user['user_id'], user['hp'] - costhp, user['mp'])
            msg_parts.append(f"{user['user_name']}气血减少：{number_to(costhp)}")
        msg = "，".join(msg_parts)

    elif event_result["type"] == "monster":
        msg = f"{event_result.get('description', '')}！"
        result, winner, status = await pve_fight(user_ids_in_battle, event_result["monster_data"], bot_id=bot.self_id, level_ratios=exp_ratios)

        if winner == 0:
            msg += f"\n恭喜道友击败敌人。"
            msg += battle_settlement(user_info, members_info, event_result["monster_data"], status)
        else:
            msg += f"\n道友不敌，重伤逃遁。"

        try:
            await send_msg_handler(bot, event, result)
        except Exception:
            msg += f"\n对战消息发送错误，可能被风控！"

    elif event_result["type"] == "treasure":
        item_id = event_result.get('drop_items', 9001)
        item_info = items.get_data_by_item_id(item_id)
        sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], 1)
        msg = f"{event_result.get('description', '')}，凑近一看居然是{item_info['name']}"

    elif event_result["type"] == "spirit_stone":
        stones = int(event_result.get('stones', 0))
        msg = f"{event_result.get('description', '')}，获得{number_to(stones)}灵石"
        sql_message.update_ls(user_id, stones, 1)

    else:
        msg = f"{event_result.get('description', '')}"

    msg += "！\n"
    dungeon_manager.update_player_progress(user_id)

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
@dungeon_status.handle(parameterless=[Cooldown(cd_time=1.4)])
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
        f"═══  副本信息  ════\n"
        f"副本：{name}\n"
        f"状态：{status_text}\n"
        f"层数：{current}/{total}层\n"
        f"进度：{(current / total * 100) if total > 0 else 0:.1f}%\n"
        f"═════════════"
    )

    await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本信息", v2="副本信息", k3="副本帮助", v3="副本帮助")
    await dungeon_status.finish()