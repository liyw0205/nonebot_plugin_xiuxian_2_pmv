import json
import asyncio
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


def _normalize_team_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize legacy database fields without trusting arbitrary JSON shapes."""
    team = dict(record)
    members = team.get("members", [])
    if not isinstance(members, list):
        try:
            members = json.loads(members or "[]")
        except (json.JSONDecodeError, TypeError, ValueError):
            members = []
    if not isinstance(members, list):
        members = []
    team["members"] = [
        str(member)
        for member in members
        if isinstance(member, (str, int)) and str(member).strip()
    ]

    leader = team.get("leader")
    if leader is None or isinstance(leader, (dict, list)):
        leader = team["members"][0] if team["members"] else ""
    team["leader"] = str(leader)

    try:
        team["max_members"] = max(int(team.get("max_members", 4)), 1)
    except (TypeError, ValueError):
        team["max_members"] = 4
    return team


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
            teams[str(team_id)] = _normalize_team_record(record)
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
        team_info = _normalize_team_record(team_info)
    return team_info


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
