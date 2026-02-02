import asyncio
import random
from datetime import datetime
from typing import Union
from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    Message,
    ActionFailed,
    MessageSegment
)

from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, leave_harm_time, OtherSet
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, number_to, check_user_type, CommandObjectID
from ..xiuxian_utils.player_fight import pve_fight
from ..xiuxian_utils.lay_out import assign_bot
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig

from .dungeon_manager import DungeonManager
from pathlib import Path
from nonebot import require

sql_message = XiuxianDateManage()  # sql类

# 导入组队管理器
from .team_manager import (
    load_teams, save_teams, create_team, add_member_to_team,
    remove_member_from_team, disband_team, get_user_team,
    get_team_info, team_invite_cache, expire_team_invite
)

# 组队
create_team_cmd = on_command("创建队伍", aliases={"新建队伍"}, priority=5)
invite_team_cmd = on_command("邀请组队", aliases={"邀请入队"}, priority=5)
agree_team_cmd = on_command("同意组队", aliases={"加入队伍", "接受组队"}, priority=5)
reject_team_cmd = on_command("拒绝组队", aliases={"拒绝入队"}, priority=5)
leave_team_cmd = on_command("离开队伍", aliases={"退出队伍"}, priority=5)
kick_team_cmd = on_command("踢出队伍", aliases={"移除队员"}, priority=5)
disband_team_cmd = on_command("解散队伍", aliases={"解散组队"}, priority=5)
view_team_cmd = on_command("查看队伍", aliases={"队伍信息", "我的队伍"}, priority=5)
help_team_cmd = on_command("队伍帮助", aliases={"组队帮助", "组队指令"}, priority=5)

cache_team_help = {}

__team_help__ = f"""
【组队指令列表】📜
创建队伍 [队伍名] - 创建新队伍
邀请组队 @某人 - 邀请成员加入
同意组队 - 同意组队邀请
拒绝组队 - 拒绝组队邀请
离开队伍 - 离开当前队伍
踢出队伍 @某人 - 踢出队员（队长权限）
解散队伍 - 解散队伍（队长权限）
查看队伍 - 查看队伍信息
组队帮助 - 查看指令
""".strip()


@help_team_cmd.handle()
async def help_team_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_team_help:
        msg = cache_team_help[session_id]
        await handle_send(bot, event, msg)
        await help_team_cmd.finish()
    else:
        msg = __team_help__
        await handle_send(bot, event, msg)
    await help_team_cmd.finish()


@create_team_cmd.handle()
async def create_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent],
                              args: Message = CommandArg()):
    """创建队伍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await create_team_cmd.finish()

    user_id = str(user_info['user_id'])
    group_id = event.group_id if isinstance(event, GroupMessageEvent) else None

    if not group_id:
        msg = "组队功能只能在群聊中使用！"
        await handle_send(bot, event, msg)
        await create_team_cmd.finish()

    # 检查是否已在队伍中
    existing_team = get_user_team(user_id)
    if existing_team:
        msg = "你已经在一个队伍中了，请先退出当前队伍！"
        await handle_send(bot, event, msg)
        await create_team_cmd.finish()

    # 获取队伍名称
    team_name = args.extract_plain_text().strip()
    if not team_name:
        team_name = f"{user_info['user_name']}的队伍"

    # 创建队伍
    team_id = create_team(team_name, user_id, group_id)

    msg = f"🎉 队伍【{team_name}】创建成功！\n队伍ID：{team_id}\n👑 队长：{user_info['user_name']}\n📢 使用【邀请组队 @成员】来邀请其他人加入！"
    await handle_send(bot, event, msg)
    await create_team_cmd.finish()


@invite_team_cmd.handle()
async def invite_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent],
                              args: Message = CommandArg()):
    """邀请成员组队"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查用户是否在队伍中且是队长
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你还没有创建或加入任何队伍！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info:
        msg = "队伍信息异常！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    # 检查是否是队长
    if team_info['leader'] != user_id:
        msg = "只有队长才能邀请成员！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    # 检查队伍是否已满
    if len(team_info['members']) >= team_info['max_members']:
        msg = f"队伍已满（{len(team_info['members'])}/{team_info['max_members']}），无法邀请新成员！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    # 解析被邀请人
    arg = args.extract_plain_text().strip()
    target_user_id = None

    # 优先解析艾特
    for arg_item in args:
        if arg_item.type == "at":
            target_user_id = str(arg_item.data.get("qq", ""))
            break

    # 如果没有艾特，再尝试解析道号/用户名
    if not target_user_id and arg:
        target_info = sql_message.get_user_info_with_name(arg)
        if target_info:
            target_user_id = str(target_info['user_id'])

    if not target_user_id:
        msg = "未找到指定的用户，请检查道号或艾特是否正确！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    # 检查目标用户是否已在队伍中
    target_team = get_user_team(target_user_id)
    if target_team:
        target_info = sql_message.get_user_info_with_id(target_user_id)
        msg = f"{target_info['user_name']}已经在队伍中了！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    # 检查是否已有未处理的邀请
    if target_user_id in team_invite_cache:
        inviter_id = team_invite_cache[target_user_id]['inviter']
        inviter_info = sql_message.get_user_info_with_id(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - team_invite_cache[target_user_id]['timestamp'])
        msg = f"对方已有来自{inviter_info['user_name']}的组队邀请（剩余{int(remaining_time)}秒），请稍后再试！"
        await handle_send(bot, event, msg)
        await invite_team_cmd.finish()

    # 创建邀请
    invite_id = f"{team_id}_{target_user_id}_{datetime.now().timestamp()}"
    team_invite_cache[target_user_id] = {
        'team_id': team_id,
        'inviter': user_id,
        'timestamp': datetime.now().timestamp(),
        'invite_id': invite_id
    }

    # 设置60秒过期
    asyncio.create_task(expire_team_invite(target_user_id, invite_id, bot, event))

    target_info = sql_message.get_user_info_with_id(target_user_id)
    msg = f"📨 已向{target_info['user_name']}发送组队邀请，等待对方回应..."
    await handle_send(bot, event, msg)
    await invite_team_cmd.finish()


@agree_team_cmd.handle()
async def agree_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """同意组队邀请"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await agree_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查是否有邀请
    if user_id not in team_invite_cache:
        msg = "没有待处理的组队邀请！"
        await handle_send(bot, event, msg)
        await agree_team_cmd.finish()

    invite_data = team_invite_cache[user_id]
    team_id = invite_data['team_id']
    inviter_id = invite_data['inviter']

    # 检查队伍是否还存在
    team_info = get_team_info(team_id)
    if not team_info:
        msg = "该队伍已解散！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg)
        await agree_team_cmd.finish()

    # 检查队伍是否已满
    if len(team_info['members']) >= team_info['max_members']:
        msg = "该队伍已满员！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg)
        await agree_team_cmd.finish()

    # 添加用户到队伍
    success = add_member_to_team(team_id, user_id)

    if success:
        # 删除邀请
        del team_invite_cache[user_id]

        # 获取邀请者信息
        inviter_info = sql_message.get_user_info_with_id(inviter_id)

        msg = f"✅ 你已成功加入队伍【{team_info['team_name']}】！\n👑 队长：{inviter_info['user_name']}\n👥 当前成员：{len(team_info['members']) + 1}/{team_info['max_members']}"
        await handle_send(bot, event, msg)
    else:
        msg = "加入队伍失败！"
        await handle_send(bot, event, msg)

    await agree_team_cmd.finish()


@reject_team_cmd.handle()
async def reject_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """拒绝组队邀请"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await reject_team_cmd.finish()

    user_id = str(user_info['user_id'])

    if user_id not in team_invite_cache:
        msg = "没有待处理的组队邀请！"
        await handle_send(bot, event, msg)
        await reject_team_cmd.finish()

    # 删除邀请
    del team_invite_cache[user_id]

    msg = "已拒绝组队邀请。"
    await handle_send(bot, event, msg)
    await reject_team_cmd.finish()


@leave_team_cmd.handle()
async def leave_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """离开队伍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await leave_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查是否在队伍中
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg)
        await leave_team_cmd.finish()

    team_info = get_team_info(team_id)

    # 离开队伍
    success = remove_member_from_team(team_id, user_id)

    if success:
        if user_id == team_info['leader']:
            if len(team_info['members']) > 1:
                msg = f"你已离开队伍【{team_info['team_name']}】，队长已转让给其他成员。"
            else:
                msg = f"你已离开队伍【{team_info['team_name']}】，队伍已解散。"
        else:
            msg = f"你已离开队伍【{team_info['team_name']}】。"
    else:
        msg = "离开队伍失败！"

    await handle_send(bot, event, msg)
    await leave_team_cmd.finish()


@kick_team_cmd.handle()
async def kick_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent],
                            args: Message = CommandArg()):
    """踢出队伍成员"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await kick_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查用户是否在队伍中且是队长
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg)
        await kick_team_cmd.finish()

    team_info = get_team_info(team_id)
    if team_info['leader'] != user_id:
        msg = "只有队长才能踢出成员！"
        await handle_send(bot, event, msg)
        await kick_team_cmd.finish()

    # 解析要踢出的成员
    arg = args.extract_plain_text().strip()
    target_user_id = None

    # 优先解析艾特
    for arg_item in args:
        if arg_item.type == "at":
            target_user_id = str(arg_item.data.get("qq", ""))
            break

    # 如果没有艾特，再尝试解析道号/用户名
    if not target_user_id and arg:
        target_info = sql_message.get_user_info_with_name(arg)
        if target_info:
            target_user_id = str(target_info['user_id'])

    if not target_user_id:
        msg = "未找到指定的成员！"
        await handle_send(bot, event, msg)
        await kick_team_cmd.finish()

    # 不能踢出自己
    if target_user_id == user_id:
        msg = "不能踢出自己！"
        await handle_send(bot, event, msg)
        await kick_team_cmd.finish()

    # 检查目标是否在队伍中
    if target_user_id not in team_info['members']:
        msg = "该成员不在你的队伍中！"
        await handle_send(bot, event, msg)
        await kick_team_cmd.finish()

    # 踢出成员
    success = remove_member_from_team(team_id, target_user_id)

    if success:
        target_info = sql_message.get_user_info_with_id(target_user_id)
        msg = f"已将成员{target_info['user_name']}踢出队伍。"
    else:
        msg = "踢出成员失败！"

    await handle_send(bot, event, msg)
    await kick_team_cmd.finish()


@disband_team_cmd.handle()
async def disband_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """解散队伍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await disband_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查用户是否在队伍中且是队长
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg)
        await disband_team_cmd.finish()

    team_info = get_team_info(team_id)
    if team_info['leader'] != user_id:
        msg = "只有队长才能解散队伍！"
        await handle_send(bot, event, msg)
        await disband_team_cmd.finish()

    # 确认解散
    success = disband_team(team_id)

    if success:
        msg = f"队伍【{team_info['team_name']}】已解散。"
    else:
        msg = "解散队伍失败！"

    await handle_send(bot, event, msg)
    await disband_team_cmd.finish()


@view_team_cmd.handle()
async def view_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """查看队伍信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await view_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查是否在队伍中
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！\n📢 使用【创建队伍 队伍名】来创建队伍！"
        await handle_send(bot, event, msg)
        await view_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info:
        msg = "队伍信息异常！"
        await handle_send(bot, event, msg)
        await view_team_cmd.finish()

    # 构建队伍信息
    members_info = []
    for member_id in team_info['members']:
        member_info = sql_message.get_user_info_with_id(member_id)
        if member_id == team_info['leader']:
            members_info.append(f"👑 {member_info['user_name']}")
        else:
            members_info.append(f"👤 {member_info['user_name']}")

    members_str = "\n".join(members_info)

    msg = (
        f"══════ 队伍信息 ══════\n"
        f"🏷️ 队伍名：{team_info['team_name']}\n"
        f"🆔 队伍ID：{team_info['team_id']}\n"
        f"📅 创建时间：{team_info['create_time']}\n"
        f"👥 成员 ({len(team_info['members'])}/{team_info['max_members']})：\n"
        f"{members_str}\n"
        f"══════════════════════"
    )

    await handle_send(bot, event, msg)
    await view_team_cmd.finish()


# ----------副本----------
# 副本
dungeon_info = on_command("副本信息", aliases={"今日副本"}, priority=5, block=True)
explore_dungeon = on_command("探索副本", aliases={"副本探索"}, priority=5, block=True)
dungeon_status = on_command("我的副本状态", aliases={"副本状态", "我的副本信息"}, priority=5, block=True)
reset_command = on_command("重置副本", aliases={"手动重置"}, priority=5, block=True, permission=SUPERUSER)
help_dungeon_cmd = on_command("副本帮助", aliases={"副本指令"}, priority=5)

scheduler = require("nonebot_plugin_apscheduler").scheduler
# 初始化副本管理器
dungeon_manager = DungeonManager()
items = Items()

cache_dungeon_help = {}

__dungeon_help__ = f"""
【副本指令列表】📜
副本信息 - 查看今日开放的副本
探索副本 - 开始挑战副本
我的副本状态 - 查看个人副本进度
副本帮助 - 显示本帮助信息
""".strip()


# 每日零点自动重置副本
@scheduler.scheduled_job("cron", hour=0, minute=1)
async def daily_dungeon_reset():
    """每日自动重置副本和玩家状态"""
    dungeon_manager.reset_dungeon()
    dungeon_manager.clear_all_player_status()


@reset_command.handle()
async def handle_manual_reset(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """手动重置副本和玩家状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    # 直接调用定时任务的逻辑
    dungeon_manager.reset_dungeon()
    dungeon_manager.clear_all_player_status()
    msg = "✅ 副本和玩家状态已重置"
    await handle_send(bot, event, msg)
    await reset_command.finish()


@help_dungeon_cmd.handle()
async def help_dungeon_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent,
                            session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_dungeon_help:
        msg = cache_dungeon_help[session_id]
        await handle_send(bot, event, msg)
        await help_dungeon_cmd.finish()
    else:
        msg = __dungeon_help__
        await handle_send(bot, event, msg)
    await help_dungeon_cmd.finish()


@dungeon_info.handle()
async def handle_dungeon_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看副本信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    dungeon_data = dungeon_manager.get_dungeon_progress()

    msg = (
        "==========✨ 今日副本 ✨ ==========\n"
        f"副本名称：{dungeon_data['name']}\n"
        f"副本描述：{dungeon_data['description']}\n"
        f"总层数：{dungeon_data['total_layers']}层\n"
        f"副本日期：{dungeon_data['date']}\n"
        "===================================\n"
        "🎮 使用「探索副本」指令开始冒险！"
    )

    await handle_send(bot, event, msg)
    await dungeon_info.finish()


def battle_settlement(user_info, members_info, monsters_list, status_list):
    """战斗结算函数"""
    sum_stone = sum(monster.get("stone", 0) for monster in monsters_list)
    sum_experience = sum(monster.get("experience", 0) for monster in monsters_list) * user_info["exp"]
    item_ids = [
        monster["item_id"]
        for monster in monsters_list
        if monster.get("item_id", 0) != 0
    ]

    team_0_data = [(stats['user_id'], stats['total_dmg']) for d in status_list for name, stats in d.items() if
                   stats['team_id'] == 0]  # 筛选team_id=0的成员并计算伤害占比
    total_dmg = sum(dmg for _, dmg in team_0_data)  # 计算总伤害
    damage_share = {user_id: round(dmg / total_dmg, 2) for user_id, dmg in team_0_data}  # 计算每个user_id的伤害占比

    if len(members_info) == 1:
        sum_stone = sum_stone / 2
        sum_experience = sum_experience / 2

    msg = "\n副本奖励："
    for user in members_info:
        user_id = user["user_id"]
        share = damage_share.get(user_id, 0) + 1
        print(damage_share, share)
        rewards_msg = []
        total_stone = int(sum_stone * share)
        if total_stone > 0:
            sql_message.update_ls(user_id, total_stone, 1)
            rewards_msg.append(f"灵石{number_to(total_stone)}")

        total_experience = int(sum_experience * share)
        if total_experience > 0:
            max_exp = int(OtherSet().set_closing_type(user['level'])) * XiuConfig().closing_exp_upper_limit
            user_get_exp_max = min(int(user['exp'] * 0.1), max(0, int(max_exp) - user['exp']))
            if user_get_exp_max < 0:
                user_get_exp_max = 0
            # 分配修为
            if total_experience >= user_get_exp_max:
                exp_msg = user_get_exp_max
                sql_message.update_exp(user_id, user_get_exp_max)
            else:
                exp_msg = total_experience
                sql_message.update_exp(user_id, total_experience)

            sql_message.update_power2(user_id)  # 更新战力
            rewards_msg.append(f"修为{number_to(exp_msg)}")

        if item_ids and user_id == user_info["user_id"]:  # 物品奖励挑战者
            item_id = random.choice(item_ids)
            item_info = items.get_data_by_item_id(item_id)
            sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], 1)
            rewards_msg.append(f"{item_info['name']}")

        rewards_msg_str = "无"
        if rewards_msg:
            rewards_msg_str = "、".join(rewards_msg)
        msg += f"\n{user['user_name']}获得：{rewards_msg_str}"

    return msg


def check_user_state(user_info):
    user_id = user_info["user_id"]
    state_msg = f"{user_info['user_name']}"
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        state_msg += f"：{msg}\n"
        return True, state_msg

    if user_info['hp'] <= user_info['exp'] / 8:  # 检测气血
        time = leave_harm_time(user_id)
        state_msg += f"：重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！\n"
        return True, state_msg

    return False, "正常"


@explore_dungeon.handle()
async def handle_explore_dungeon(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """探索副本"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await explore_dungeon.finish()

    user_id = user_info["user_id"]
    user_exp = user_info["exp"]

    player_data = dungeon_manager.get_player_status(user_id)
    if player_data["dungeon_status"] == "completed":
        msg = f"今日副本已完成，请等待明日刷新！"
        await handle_send(bot, event, msg)
        await explore_dungeon.finish()

    user_data = [user_id]
    exp_ratios = None

    team_id = get_user_team(str(user_id))  # 检查是否在队伍中
    members_info = [user_info]  # 队伍成员信息
    if team_id and (team_info := get_team_info(team_id)):
        if team_info['leader'] != str(user_id):
            msg = "你不是队长！"
            await handle_send(bot, event, msg)
            await disband_team_cmd.finish()
        members_info = [
            sql_message.get_user_info_with_id(int(member_id))
            for member_id in team_info["members"]
        ]

    if len(members_info) > 1:
        user_data = [member["user_id"] for member in members_info]
        exp_ratios = {
            member["user_id"]: 1.0 if user_exp * 1.2 / member["exp"] > 0.9 else user_exp * 1.2 / member["exp"]
            for member in members_info
        }

    for user in members_info:
        passed, message = check_user_state(user)  # 检测玩家状态
        if passed:
            await handle_send(bot, event, message)
            await explore_dungeon.finish()

    if player_data["current_layer"] == player_data["total_layers"] - 1:  # boss层
        boss_info = dungeon_manager.get_boss_data(user_info['level'], user_exp)  # 获取boss层怪兽信息
        result, winner, status = await pve_fight(user_data, boss_info, bot_id=bot.self_id, level_ratios=exp_ratios)

        if winner == 0:
            msg = f"恭喜道友击败【{boss_info[0]['name']}】！"
            msg += battle_settlement(user_info, members_info, boss_info, status)
            dungeon_manager.update_player_progress(user_id)  # 更新副本状态
        else:
            msg = f"道友不敌【{boss_info[0]['name']}】，重伤逃遁。"
        try:
            await send_msg_handler(bot, event, result)
        except ActionFailed:
            msg += f"\nBoss战消息发送错误,可能被风控!"
        await handle_send(bot, event, msg)
        await explore_dungeon.finish()

    # 触发事件
    event_result = dungeon_manager.trigger_event(user_info['level'], user_exp)

    if event_result["type"] == "trap":
        msg = f"{event_result.get('description', '')}"
        for user in members_info:
            costhp = int((user['exp'] / 2) * event_result.get('damage', 0.1))
            sql_message.update_user_hp_mp(user['user_id'], user['hp'] - costhp, user['mp'])
            msg += f"，{user['user_name']}气血减少：{number_to(costhp)}"

    elif event_result["type"] == "monster":
        msg = f"{event_result.get('description', '')}！"
        # 执行战斗并获取结果
        result, winner, status = \
            await pve_fight(user_data, event_result["monster_data"], bot_id=bot.self_id, level_ratios=exp_ratios)

        if winner == 0:
            msg += f"\n恭喜道友击败敌人。"
            msg += battle_settlement(user_info, members_info, event_result["monster_data"], status)
        else:
            msg += f"\n道友不敌，重伤逃遁。"
        try:
            await send_msg_handler(bot, event, result)
        except ActionFailed:
            msg += f"\n对战消息发送错误,可能被风控!"

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
    msg += f"当前：第{player_data['current_layer'] + 1}层\n"
    msg += "使用'探索副本'进入下一层！"
    dungeon_manager.update_player_progress(user_id)  # 更新副本状态

    await handle_send(bot, event, msg)
    await explore_dungeon.finish()


@dungeon_status.handle()
async def handle_dungeon_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """副本状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await dungeon_status.finish()

    user_id = user_info["user_id"]
    player_data = dungeon_manager.get_player_status(user_id)

    # 一行完成所有数据获取
    name, status, total, current = (
        player_data.get('dungeon_name', '未知'),
        {'not_started': '未开始', 'exploring': '探索中', 'completed': '已完成'}.get(
            player_data.get('dungeon_status', 'not_started'), '未知'),
        player_data.get('total_layers', 0),
        player_data.get('current_layer', 0)
    )

    msg = (
        f"========== 副本信息 ==========\n"
        f"副本：{name}\n"
        f"状态：{status}\n"
        f"层数：{current}/{total}层\n"
        f"进度：{(current / total * 100) if total > 0 else 0:.1f}%\n"
        f"============================="
    )

    await handle_send(bot, event, msg)
    await dungeon_status.finish()
