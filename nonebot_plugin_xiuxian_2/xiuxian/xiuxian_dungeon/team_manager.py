import json
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from nonebot import Bot
from ..adapter_compat import GroupMessageEvent, PrivateMessageEvent, Message

# 导入你的现有函数（如果存在）
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.utils import check_user, handle_send
from ..xiuxian_utils.lay_out import assign_bot

sql_message = XiuxianDateManage()  # sql类
player_data = PlayerDataManager() # PlayerDataManager实例

# 表名常量
TEAM_TABLE = "teams" # 队伍信息表
# TEAM_MEMBER_TABLE = "team_members" # 如果需要独立成员表，但目前成员列表会直接存入 TEAM_TABLE

# 邀请缓存 - 仍然在内存中，不涉及数据库
team_invite_cache: Dict[str, Dict] = {}


def load_teams() -> Dict[str, Dict]:
    """
    从数据库加载所有队伍数据。
    :return: 队伍ID -> 队伍信息的字典。
    """
    teams = {}
    records = player_data.get_all_records(TEAM_TABLE)
    for record in records:
        team_id = record.get("user_id") # 'user_id'字段在这里存储'team_id'
        if team_id:
            # PlayerDataManager.get_fields 已经处理了JSON反序列化
            teams[team_id] = record
            # 确保成员列表等字段是list，leader是str
            if not isinstance(teams[team_id].get("members"), list):
                teams[team_id]["members"] = json.loads(teams[team_id].get("members", "[]")) # 再次确保是列表
            if not isinstance(teams[team_id].get("leader"), str):
                teams[team_id]["leader"] = str(teams[team_id].get("leader"))
    return teams


def save_team(team_info: Dict):
    """
    将单个队伍信息保存到数据库。
    :param team_info: 队伍信息字典。
    """
    team_id = team_info["team_id"]
    # PlayerDataManager.update_or_write_data 会自动处理dict/list到JSON string的序列化
    for key, value in team_info.items():
        player_data.update_or_write_data(team_id, TEAM_TABLE, key, value)


def get_user_team(user_id: str) -> Optional[str]:
    """
    获取用户所在的队伍ID。
    :param user_id: 用户QQ号（字符串）。
    :return: 队伍ID字符串或None。
    """
    teams = load_teams() # 从数据库加载所有队伍
    for team_id, team in teams.items():
        # members 字段在load_teams时已经被反序列化为list
        if user_id in team.get('members', []):
            return team_id
    return None


def get_team_info(team_id: str) -> Optional[Dict]:
    """
    获取指定队伍的详细信息。
    :param team_id: 队伍ID字符串。
    :return: 队伍信息字典或None。
    """
    # PlayerDataManager.get_fields 已经处理了JSON反序列化
    team_info = player_data.get_fields(team_id, TEAM_TABLE)
    if team_info:
        # 再次确保成员列表是list，leader是str，以防get_fields未完全处理或存储格式不一致
        if not isinstance(team_info.get("members"), list):
            try:
                team_info["members"] = json.loads(team_info.get("members", "[]"))
            except (json.JSONDecodeError, TypeError):
                team_info["members"] = [] # 如果解析失败，则设为空列表
        if not isinstance(team_info.get("leader"), str):
            team_info["leader"] = str(team_info.get("leader"))
    return team_info


def create_team(team_name: str, leader_id: str, group_id: int) -> str:
    """
    创建新队伍。
    :param team_name: 队伍名称。
    :param leader_id: 队长QQ号（字符串）。
    :param group_id: 创建队伍的群号。
    :return: 新创建队伍的ID字符串。
    """
    # 生成队伍ID，以group_id和时间戳组合，确保唯一性
    team_id = f"{group_id}_{int(datetime.now().timestamp())}"

    team_info = {
        "team_id": team_id,
        "team_name": team_name,
        "group_id": str(group_id), # group_id也存为字符串
        "leader": leader_id,
        "members": [leader_id], # 成员列表作为JSON字符串存储
        "create_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "max_members": 4,  # 默认最大4人
        "description": ""
    }

    save_team(team_info) # 保存到数据库

    return team_id


def add_member_to_team(team_id: str, user_id: str) -> bool:
    """
    添加成员到队伍。
    :param team_id: 队伍ID。
    :param user_id: 要加入的成员QQ号（字符串）。
    :return: True如果成功，False否则。
    """
    team = get_team_info(team_id)
    if not team:
        return False

    if user_id in team['members']:
        return False

    if len(team['members']) >= team['max_members']:
        return False

    team['members'].append(user_id)
    save_team(team) # 更新到数据库

    return True


def remove_member_from_team(team_id: str, user_id: str) -> bool:
    """
    从队伍移除成员。
    :param team_id: 队伍ID。
    :param user_id: 要移除的成员QQ号（字符串）。
    :return: True如果成功，False否则。
    """
    team = get_team_info(team_id)
    if not team:
        return False

    if user_id not in team['members']:
        return False

    if user_id == team['leader']:
        if len(team['members']) > 1:
            team['members'].remove(user_id)
            team['leader'] = team['members'][0] # 转移队长给第一个成员
            save_team(team) # 更新到数据库
        else:
            disband_team(team_id) # 最后一个成员离开，解散队伍
    else:
        team['members'].remove(user_id)
        save_team(team) # 更新到数据库

    return True


def disband_team(team_id: str) -> bool:
    """
    解散队伍。
    :param team_id: 队伍ID。
    :return: True如果成功，False否则。
    """
    team = get_team_info(team_id)
    if not team:
        return False

    # 从数据库中删除队伍记录
    player_data.delete_record(team_id, TEAM_TABLE)

    return True


async def expire_team_invite(user_id: str, invite_id: str, bot: Bot, event):
    """
    组队邀请过期处理。
    :param user_id: 被邀请者QQ号（字符串）。
    :param invite_id: 邀请ID。
    :param bot: Bot实例。
    :param event: 事件对象。
    """
    await asyncio.sleep(60)

    # 检查邀请是否仍然存在且未被处理（通过 invite_id 确认）
    if user_id in team_invite_cache and team_invite_cache[user_id]['invite_id'] == invite_id:
        msg = f"组队邀请已过期！"
        # 由于这里是在一个异步任务中，handle_send 需要正确的 group_id 或 user_id 来发送消息
        # 对于群组邀请，直接发送到群里。对于私聊邀请，发送私聊
        # 这里假设 `event` 已经包含了足够的上下文
        await handle_send(bot, event, msg)
        del team_invite_cache[user_id] # 删除过期的邀请