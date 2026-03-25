import asyncio
import random
from datetime import datetime
from typing import Union, Any
from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    Message,
    MessageSegment
)

from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, leave_harm_time, OtherSet
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, number_to, check_user_type, _impersonating_users
from ..xiuxian_utils.player_fight import pve_fight
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata
from .dungeon_manager import DungeonManager
from pathlib import Path
from nonebot import require

sql_message = XiuxianDateManage()  # sql类

# 导入组队管理器
from .team_manager import (
    create_team, add_member_to_team,
    remove_member_from_team, disband_team, get_user_team,
    get_team_info, team_invite_cache, expire_team_invite,
    load_teams # load_teams现在从数据库加载，但这里不需要cache_team_data
)

# 组队
create_team_cmd = on_command("创建队伍", aliases={"新建队伍"}, priority=5)
invite_team_cmd = on_command("邀请组队", aliases={"邀请入队", "组队邀请"}, priority=5)
agree_team_cmd = on_command("同意组队", aliases={"加入队伍", "接受组队", "组队同意"}, priority=5)
reject_team_cmd = on_command("拒绝组队", aliases={"拒绝入队"}, priority=5)
leave_team_cmd = on_command("离开队伍", aliases={"退出队伍"}, priority=5)
kick_team_cmd = on_command("踢出队伍", aliases={"移除队员"}, priority=5)
disband_team_cmd = on_command("解散队伍", aliases={"解散组队"}, priority=5)
view_team_cmd = on_command("查看队伍", aliases={"队伍信息", "我的队伍"}, priority=5)
help_team_cmd = on_command("队伍帮助", aliases={"组队帮助", "组队指令"}, priority=5)
# ----------副本----------
# 副本
dungeon_info = on_command("副本信息", aliases={"今日副本"}, priority=5, block=True)
explore_dungeon = on_command("探索副本", aliases={"副本探索", "挑战副本"}, priority=5, block=True)
dungeon_status = on_command("我的副本状态", aliases={"副本状态", "我的副本信息"}, priority=5, block=True)
reset_command = on_command("重置副本", aliases={"手动重置"}, priority=5, block=True, permission=SUPERUSER)
help_dungeon_cmd = on_command("副本帮助", aliases={"副本指令"}, priority=5)

scheduler = require("nonebot_plugin_apscheduler").scheduler
# 初始化副本管理器
dungeon_manager = DungeonManager()
items = Items()

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
组队帮助 - 查看指令
""".strip()


@help_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def help_team_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __team_help__
    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await help_team_cmd.finish()


@create_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def create_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    """创建队伍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await create_team_cmd.finish()

    user_id = str(user_info['user_id'])
    group_id = event.group_id if isinstance(event, GroupMessageEvent) else None

    if not group_id:
        msg = "组队功能只能在群聊中使用！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
        await create_team_cmd.finish()

    # 检查是否已在队伍中
    existing_team_id = get_user_team(user_id)
    if existing_team_id:
        msg = "你已经在一个队伍中了，请先退出当前队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="离开队伍", v1="离开队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
        await create_team_cmd.finish()

    # 获取队伍名称
    team_name = args.extract_plain_text().strip()
    if not team_name:
        team_name = f"{user_info['user_name']}的队伍"

    # 创建队伍
    team_id = create_team(team_name, user_id, group_id)

    msg = f"🎉 队伍【{team_name}】创建成功！\n队伍ID：{team_id}\n👑 队长：{user_info['user_name']}\n📢 使用【邀请组队 道号】来邀请其他人加入！"
    await handle_send(bot, event, msg, md_type="team", k1="邀请组队", v1="邀请组队", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
    await create_team_cmd.finish()


@invite_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def invite_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    """邀请成员组队"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await invite_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查用户是否在队伍中且是队长
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

    # 检查是否是队长
    if team_info['leader'] != user_id:
        msg = "只有队长才能邀请成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    # 检查队伍是否已满
    if len(team_info['members']) >= team_info['max_members']:
        msg = f"队伍已满（{len(team_info['members'])}/{team_info['max_members']}），无法邀请新成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
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
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    if not target_user_id:
        msg = "未找到指定的用户，请检查道号或艾特是否正确！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await invite_team_cmd.finish()
    
    # 检查目标用户是否已注册修仙
    is_target_user, target_user_info, target_msg = check_user(target_user_id)
    if not is_target_user:
        await handle_send(bot, event, target_msg)
        await invite_team_cmd.finish()


    # 检查目标用户是否已在队伍中
    target_team = get_user_team(target_user_id)
    if target_team:
        target_name = target_user_info['user_name']
        msg = f"{target_name}已有队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await invite_team_cmd.finish()

    # 检查是否已有未处理的邀请
    if target_user_id in team_invite_cache:
        inviter_id = team_invite_cache[target_user_id]['inviter']
        inviter_info = sql_message.get_user_info_with_id(inviter_id)
        remaining_time = 60 - (datetime.now().timestamp() - team_invite_cache[target_user_id]['timestamp'])
        msg = f"对方已有来自{inviter_info['user_name']}的组队邀请（剩余{int(remaining_time)}秒），请稍后再试！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await invite_team_cmd.finish()

    # 创建邀请
    invite_id = f"{team_id}_{target_user_id}_{datetime.now().timestamp()}"
    team_invite_cache[target_user_id] = {
        'team_id': team_id,
        'inviter': user_id,
        'timestamp': datetime.now().timestamp(),
        'invite_id': invite_id,
        'group_id': event.group_id # 记录邀请发生群组
    }

    # 设置60秒过期
    asyncio.create_task(expire_team_invite(target_user_id, invite_id, bot, event))

    target_name = target_user_info['user_name']
    msg = f"📨 已向{target_name}发送组队邀请，等待对方回应..."
    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
    
    # 私聊通知被邀请者
    try:
        if isinstance(event, GroupMessageEvent): # 如果是群聊邀请，则尝试私聊通知被邀请者
            await bot.send_private_msg(user_id=int(target_user_id), message=f"你在群{event.group_id}收到了来自{user_info['user_name']}的组队邀请，请在1分钟内回复【同意组队】或【拒绝组队】。")
    except Exception as e:
        print(f"私聊通知被邀请者失败: {e}")

    await invite_team_cmd.finish()


@agree_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def agree_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """同意组队邀请"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await agree_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查是否有邀请
    if user_id not in team_invite_cache:
        msg = "没有待处理的组队邀请！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    invite_data = team_invite_cache[user_id]
    team_id = invite_data['team_id']
    inviter_id = invite_data['inviter']
    invite_group_id = invite_data['group_id'] # 获取邀请发生群组

    # 检查当前消息是否来自邀请发生的群组，或者私聊
    if isinstance(event, GroupMessageEvent) and event.group_id != invite_group_id:
        msg = f"此邀请是在群{invite_group_id}发出的，请在该群或私聊中进行操作。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()

    # 检查队伍是否还存在
    team_info = get_team_info(team_id)
    if not team_info:
        msg = "该队伍已解散！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await agree_team_cmd.finish()
    
    # 再次检查被邀请者是否已在队伍中
    if get_user_team(user_id):
        msg = "你已经在一个队伍中了，无法接受邀请！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="离开队伍", v1="离开队伍", k2="查看队伍", v2="查看队伍", k3="队伍帮助", v3="队伍帮助")
        await agree_team_cmd.finish()


    # 检查队伍是否已满
    if len(team_info['members']) >= team_info['max_members']:
        msg = "该队伍已满员！"
        del team_invite_cache[user_id]
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await agree_team_cmd.finish()

    # 添加用户到队伍
    success = add_member_to_team(team_id, user_id)

    if success:
        # 删除邀请
        del team_invite_cache[user_id]

        # 获取邀请者信息
        inviter_info = sql_message.get_user_info_with_id(inviter_id)

        msg = f"✅ 你已成功加入队伍【{team_info['team_name']}】！\n👑 队长：{inviter_info['user_name']}\n👥 当前成员：{len(team_info['members'])}/{team_info['max_members']}"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="副本信息", v2="副本信息", k3="队伍帮助", v3="队伍帮助")
        # 通知队长有人加入了队伍
        try:
            if team_info['group_id'] and team_info['group_id'] != str(event.group_id): # 如果队长在不同群，team_info['group_id']是str
                await bot.send_group_msg(group_id=int(team_info['group_id']), message=f"你的队伍【{team_info['team_name']}】加入了新成员：{user_info['user_name']}！")
            elif team_info['group_id'] and team_info['group_id'] == str(event.group_id): # 如果在同一个群，则直接在当前群通知
                pass # 已经在当前群发送了，不需要重复
            else: # 如果队长在私聊，或者其他情况
                await bot.send_private_msg(user_id=int(inviter_id), message=f"你的队伍【{team_info['team_name']}】加入了新成员：{user_info['user_name']}！")

        except Exception as e:
            print(f"通知队长失败: {e}")
            
    else:
        msg = "加入队伍失败！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")

    await agree_team_cmd.finish()


@reject_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def reject_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """拒绝组队邀请"""
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

    # 检查当前消息是否来自邀请发生的群组，或者私聊
    if isinstance(event, GroupMessageEvent) and event.group_id != invite_group_id:
        msg = f"此邀请是在群{invite_group_id}发出的，请在该群或私聊中进行操作。"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await reject_team_cmd.finish()

    # 删除邀请
    del team_invite_cache[user_id]

    msg = "已拒绝组队邀请。"
    await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
    await reject_team_cmd.finish()


@leave_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def leave_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """离开队伍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await leave_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查是否在队伍中
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await leave_team_cmd.finish()

    team_info = get_team_info(team_id)

    # 离开队伍
    success = remove_member_from_team(team_id, user_id)

    if success:
        if user_id == team_info['leader']: # 如果是队长离开
            new_team_info = get_team_info(team_id) # 获取更新后的队伍信息
            if new_team_info: # 队伍还有其他成员
                new_leader_name = sql_message.get_user_info_with_id(new_team_info['leader'])['user_name']
                msg = f"你已离开队伍【{team_info['team_name']}】，队长已转让给{new_leader_name}。"
            else: # 队伍可能被解散了
                msg = f"你已离开队伍【{team_info['team_name']}】，队伍已解散。"
        else: # 普通成员离开
            msg = f"你已离开队伍【{team_info['team_name']}】。"
    else:
        msg = "离开队伍失败！"

    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v3="队伍帮助")
    await leave_team_cmd.finish()


@kick_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def kick_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent], args: Message = CommandArg()):
    """踢出队伍成员"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await kick_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查用户是否在队伍中且是队长
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info or team_info['leader'] != user_id: # 增加team_info判空
        msg = "只有队长才能踢出成员！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
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
        target_db_info = sql_message.get_user_info_with_name(arg)
        if target_db_info:
            target_user_id = str(target_db_info['user_id'])

    if not target_user_id:
        msg = "未找到指定的成员！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()

    # 不能踢出自己
    if target_user_id == user_id:
        msg = "不能踢出自己！"
        await handle_send(bot, event, msg, md_type="team", k1="队伍帮助", v1="队伍帮助")
        await kick_team_cmd.finish()

    # 检查目标是否在队伍中
    if target_user_id not in team_info['members']:
        msg = "该成员不在你的队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await kick_team_cmd.finish()

    # 踢出成员
    success = remove_member_from_team(team_id, target_user_id)

    if success:
        target_info = sql_message.get_user_info_with_id(target_user_id)
        msg = f"已将成员{target_info['user_name']}踢出队伍。"
    else:
        msg = "踢出成员失败！"

    await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
    await kick_team_cmd.finish()


@disband_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def disband_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """解散队伍"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await disband_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查用户是否在队伍中且是队长
    team_id = get_user_team(user_id)
    if not team_id:
        msg = "你不在任何队伍中！"
        await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
        await disband_team_cmd.finish()

    team_info = get_team_info(team_id)
    if not team_info or team_info['leader'] != user_id: # 增加team_info判空
        msg = "只有队长才能解散队伍！"
        await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
        await disband_team_cmd.finish()

    # 确认解散
    success = disband_team(team_id)

    if success:
        msg = f"队伍【{team_info['team_name']}】已解散。"
    else:
        msg = "解散队伍失败！"

    await handle_send(bot, event, msg, md_type="team", k1="创建队伍", v1="创建队伍", k2="队伍帮助", v2="队伍帮助")
    await disband_team_cmd.finish()


@view_team_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def view_team_handler(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    """查看队伍信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await view_team_cmd.finish()

    user_id = str(user_info['user_id'])

    # 检查是否在队伍中
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

    # 构建队伍信息
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


# 每日零点自动重置副本
@scheduler.scheduled_job("cron", hour=0, minute=1)
async def daily_dungeon_reset():
    """每日自动重置副本和玩家状态"""
    print("执行每日副本重置任务...")
    dungeon_manager.reset_dungeon()
    print("每日副本重置任务完成。")


@reset_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_manual_reset(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """手动重置副本和玩家状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    # 直接调用定时任务的逻辑
    dungeon_manager.reset_dungeon()
    msg = "✅ 副本和所有玩家的副本进度已重置。"
    await handle_send(bot, event, msg, md_type="副本", k1="副本信息", v1="副本信息", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
    await reset_command.finish()


@help_dungeon_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def help_dungeon_cmd_(bot: Bot, event: Union[GroupMessageEvent, PrivateMessageEvent]):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __dungeon_help__
    await handle_send(bot, event, msg, md_type="副本", k1="副本信息", v1="副本信息", k2="探索副本", v2="探索副本", k3="副本状态", v3="我的副本状态")
    await help_dungeon_cmd.finish()


@dungeon_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_dungeon_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看副本信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    dungeon_data = dungeon_manager.get_dungeon_progress()

    msg = (
        "═══  ✨ 今日副本 ✨  ═══\n"
        f"{dungeon_data['name']}\n"
        f"\n> {dungeon_data['description']}\n\n"
        f"总层数：{dungeon_data['total_layers']}层\n"
        f"副本日期：{dungeon_data['date']}\n"
        "═════════════\n"
        "🎮 使用「探索副本」指令开始冒险！"
    )

    await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
    await dungeon_info.finish()


def battle_settlement(user_info, members_info, monsters_list, status_list):
    """
    战斗结算函数。
    :param user_info: 队长/发起挑战者的 user_info。
    :param members_info: 队伍成员的完整用户数据列表。
    :param monsters_list: 怪物数据列表。
    :param status_list: 战斗结束时所有玩家的最终状态列表，包含 'total_dmg' 字段。
    :return: 结算消息字符串。
    """
    sum_stone = sum(monster.get("stone", 0) for monster in monsters_list)
    sum_experience = sum(monster.get("experience", 0) for monster in monsters_list)
    item_ids = [
        monster["item_id"]
        for monster in monsters_list
        if monster.get("item_id", 0) != 0
    ]

    # 从 status_list 中提取团队0（玩家队伍）成员的user_id和total_dmg
    team_0_data = []
    for d in status_list:
        for name, stats in d.items():
            if stats.get('team_id') == 0 and stats.get('user_id') != 0: # 确保是玩家且不是机器人ID
                team_0_data.append({'user_id': stats['user_id'], 'total_dmg': stats['total_dmg']})

    total_player_dmg = sum(member_stats['total_dmg'] for member_stats in team_0_data)

    damage_share = {}
    
    # 建立一个成员ID到其在members_info中的完整信息的映射
    member_id_to_info = {member['user_id']: member for member in members_info}

    # 根据伤害占比分配奖励
    if total_player_dmg > 0:
        for member_stats in team_0_data:
            user_id = member_stats['user_id']
            dmg = member_stats['total_dmg']
            damage_share[user_id] = round(dmg / total_player_dmg, 2)
    else:
        # 如果总伤害为0，但有成员参与，则平均分配
        if team_0_data:
            num_members_in_team_0 = len(team_0_data)
            share_per_member = 1 / num_members_in_team_0 if num_members_in_team_0 > 0 else 0
            for member_stats in team_0_data:
                user_id = member_stats['user_id']
                damage_share[user_id] = share_per_member
        # 如果 team_0_data 也为空，damage_share 将保持为空，后续不会分配奖励

    msg = "\n副本奖励："
    
    # 如果damage_share依然为空，说明没有有效的成员或战斗数据来分配奖励
    if not damage_share:
        msg += "无，本次战斗无人获得奖励。"
        return msg

    # 判断是否是单人模式
    is_single_player_mode = (len(team_0_data) == 1) # 只有1个玩家参与战斗视为单人模式
    
    # 单人模式奖励调整乘数，这里直接设定为 1.0，即单人和组队奖励相同，不再惩罚
    single_player_reward_multiplier = 1.0 
    
    # 根据伤害占比分配奖励
    for user_id_int, share in damage_share.items():
        user = member_id_to_info.get(user_id_int) # 获取完整的成员信息
        if not user:
            continue # 如果找不到对应的成员信息，跳过

        rewards_msg = []
        
        # 灵石奖励
        # 鉴于 dungeon_manager 已经优化了灵石的基数，这里直接按 share 分配即可
        total_stone_for_member = int(sum_stone * share)
        if is_single_player_mode:
            total_stone_for_member = int(total_stone_for_member * single_player_reward_multiplier) # 单人奖励调整

        if total_stone_for_member > 0:
            sql_message.update_ls(user['user_id'], total_stone_for_member, 1)
            rewards_msg.append(f"灵石{number_to(total_stone_for_member)}")

        # 经验奖励
        total_experience_for_member = int(sum_experience * share)
        if is_single_player_mode:
            total_experience_for_member = int(total_experience_for_member * single_player_reward_multiplier) # 单人奖励调整

        # 队员只给灵石，自己才有灵石、修为、物品
        if total_experience_for_member > 0 and user['user_id'] == user_info["user_id"]:
            # 确保获取到的 user_info 的 level 是最新的
            current_user_info = sql_message.get_user_info_with_id(user['user_id'])
            current_user_rank = max(convert_rank(current_user_info['level'])[0] // 3, 1)
            max_exp = int(jsondata.level_data()[current_user_info['level']]["power"] * XiuConfig().closing_exp_upper_limit)
            current_exp = int(max_exp * min(0.1 * current_user_rank, 1))
            max_obtainable_exp_soft_cap = int(current_exp * 0.01)
            
            exp_to_award = total_experience_for_member
            if max_obtainable_exp_soft_cap > 0:
                 exp_to_award = min(total_experience_for_member, max_obtainable_exp_soft_cap)
            
            if exp_to_award > 0:
                sql_message.update_exp(user['user_id'], exp_to_award)
                sql_message.update_power2(user['user_id'])
                rewards_msg.append(f"修为{number_to(exp_to_award)}")

        # 物品奖励 (只给发起挑战者或队长)
        # 物品只给发起挑战者（即user_info["user_id"]），且只有队长才有资格获取物品，避免多人同时获得
        if item_ids and user['user_id'] == user_info["user_id"]:  
            item_id = random.choice(item_ids)
            item_info = items.get_data_by_item_id(item_id)
            sql_message.send_back(user['user_id'], item_id, item_info['name'], item_info['type'], 1)
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


@explore_dungeon.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_explore_dungeon(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """探索副本"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await explore_dungeon.finish()

    user_id = user_info["user_id"]
    user_exp = user_info["exp"]

    # 从数据库获取玩家副本状态
    player_status = dungeon_manager.get_player_status(user_id) # Renamed to player_status to avoid conflict with player_data_manager global
    if player_status["dungeon_status"] == "completed":
        msg = f"今日副本已完成，请等待明日刷新！"
        await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
        await explore_dungeon.finish()

    user_ids_in_battle = [user_id] # 实际参与战斗的用户ID列表
    exp_ratios = None

    team_id = get_user_team(str(user_id))  # 检查是否在队伍中
    members_info = [user_info]  # 队伍成员的详细信息
    if team_id and (team_info := get_team_info(team_id)):
        if team_info['leader'] != str(user_id):
            msg = "你不是队长，无法带领队伍探索副本！"
            await handle_send(bot, event, msg, md_type="team", k1="查看队伍", v1="查看队伍", k2="队伍帮助", v2="队伍帮助")
            await explore_dungeon.finish()
        
        # 获取所有成员的最新信息
        members_info = [
            sql_message.get_user_info_with_id(member_id) # member_id 已经是 str
            for member_id in team_info["members"]
        ]
        # 过滤掉无法获取到信息的成员（例如已删除的用户）
        members_info = [m for m in members_info if m is not None]
        
        # 重新构建user_ids_in_battle
        user_ids_in_battle = [member["user_id"] for member in members_info]
        
        # 计算经验比例，确保所有成员都在列表中
        if user_exp > 0: # 避免除以0
            # 这里调整经验比例的计算方式，使其更合理。
            # 如果是组队，每个队员的基础属性会根据这个比例缩放，
            # 确保等级较低的队员不会因为等级差过大而属性过低。
            exp_ratios = {
                member["user_id"]: max(0.5, min(1.0, user_exp / member["exp"])) if member["exp"] > 0 else 1.0 # 经验比例上限1.0，下限0.5
                for member in members_info
            }
        else:
            exp_ratios = {member["user_id"]: 1.0 for member in members_info} # 默认1.0

    for user in members_info:
        passed, message = check_user_state(user)  # 检测玩家状态
        if passed:
            await handle_send(bot, event, message, md_type="副本", k1="我的修仙信息", v1="我的修仙信息")
            await explore_dungeon.finish()

    current_layer = player_status["current_layer"]
    total_layers = player_status["total_layers"]

    if current_layer == total_layers - 1:  # 最后一层是boss层
        boss_info = dungeon_manager.get_boss_data(user_info['level'], user_exp)  # 获取boss层怪兽信息
        # pve_fight现在接受多个user_id
        result, winner, status = await pve_fight(user_ids_in_battle, boss_info, bot_id=bot.self_id, level_ratios=exp_ratios)

        if winner == 0:
            msg = f"恭喜道友击败【{boss_info[0]['name']}】！"
            msg += battle_settlement(user_info, members_info, boss_info, status)
            dungeon_manager.update_player_progress(user_id, status="completed")  # 更新副本状态为已完成
        else:
            msg = f"道友不敌【{boss_info[0]['name']}】，重伤逃遁。"
        try:
            await send_msg_handler(bot, event, result)
        except Exception:
            msg += f"\n对战消息发送错误,可能被风控!"
        await handle_send(bot, event, msg, md_type="副本", k1="探索副本", v1="探索副本", k2="副本状态", v2="我的副本状态", k3="副本帮助", v3="副本帮助")
        await explore_dungeon.finish()

    # 触发事件
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
        # 执行战斗并获取结果
        result, winner, status = \
            await pve_fight(user_ids_in_battle, event_result["monster_data"], bot_id=bot.self_id, level_ratios=exp_ratios)

        if winner == 0:
            msg += f"\n恭喜道友击败敌人。"
            msg += battle_settlement(user_info, members_info, event_result["monster_data"], status)
        else:
            msg += f"\n道友不敌，重伤逃遁。"
        try:
            await send_msg_handler(bot, event, result)
        except Exception:
            msg += f"\n对战消息发送错误,可能被风控!"

    elif event_result["type"] == "treasure":
        item_id = event_result.get('drop_items', 9001)
        item_info = items.get_data_by_item_id(item_id)
        # 物品只给发起挑战的队长
        sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], 1) 
        msg = f"{event_result.get('description', '')}，凑近一看居然是{item_info['name']}"

    elif event_result["type"] == "spirit_stone":
        stones = int(event_result.get('stones', 0))
        # 灵石只给发起挑战的队长
        msg = f"{event_result.get('description', '')}，获得{number_to(stones)}灵石"
        sql_message.update_ls(user_id, stones, 1)

    else:
        msg = f"{event_result.get('description', '')}"

    msg += "！\n"
    dungeon_manager.update_player_progress(user_id)  # 更新副本状态

    # 重新获取更新后的玩家状态
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


@dungeon_status.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_dungeon_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """副本状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await dungeon_status.finish()

    user_id = user_info["user_id"]
    player_status = dungeon_manager.get_player_status(user_id)

    # 一行完成所有数据获取
    name, status_text, total, current = (
        player_status.get('dungeon_name', '未知'),
        {'not_started': '未开始', 'exploring': '探索中', 'completed': '已完成'}.get(
            player_status.get('dungeon_status', 'not_started'), '未知'),
        player_status.get('total_layers', 0),
        player_status.get('current_layer', 0)
    )

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