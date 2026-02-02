import json
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from nonebot import Bot
from nonebot.adapters.onebot.v11 import GroupMessageEvent, PrivateMessageEvent, Message

# 导入你的现有函数（如果存在）
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.utils import check_user,  handle_send
from ..xiuxian_utils.lay_out import assign_bot
sql_message = XiuxianDateManage()  # sql类


# 数据文件路径
TEAM_DATA_PATH = Path(__file__).parent / "data" / "teams.json"

# 邀请缓存
team_invite_cache: Dict[str, Dict] = {}
# 队伍数据缓存
team_data_cache: Dict[str, Dict] = {}


def load_teams() -> Dict:
    """加载队伍数据"""
    try:
        if TEAM_DATA_PATH.exists():
            with open(TEAM_DATA_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_teams(data: Dict):
    """保存队伍数据"""
    TEAM_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TEAM_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user_team(user_id: str) -> Optional[str]:
    """获取用户所在的队伍ID"""
    teams = load_teams()
    for team_id, team in teams.items():
        if user_id in team.get('members', []):
            return team_id
    return None


def get_team_info(team_id: str) -> Optional[Dict]:
    """获取队伍信息"""
    teams = load_teams()
    return teams.get(team_id)


def create_team(team_name: str, leader_id: str, group_id: int) -> str:
    """创建新队伍"""
    teams = load_teams()

    # 生成队伍ID
    team_id = f"{group_id}_{int(datetime.now().timestamp())}"

    team_info = {
        "team_id": team_id,
        "team_name": team_name,
        "group_id": group_id,
        "leader": leader_id,
        "members": [leader_id],
        "create_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "max_members": 4,  # 默认最大4人
        "description": ""
    }

    teams[team_id] = team_info
    save_teams(teams)

    # 更新缓存
    team_data_cache[team_id] = team_info

    return team_id


def add_member_to_team(team_id: str, user_id: str) -> bool:
    """添加成员到队伍"""
    teams = load_teams()

    if team_id not in teams:
        return False

    team = teams[team_id]

    # 检查是否已在队伍中
    if user_id in team['members']:
        return False

    # 检查队伍是否已满
    if len(team['members']) >= team['max_members']:
        return False

    team['members'].append(user_id)
    save_teams(teams)

    # 更新缓存
    team_data_cache[team_id] = team

    return True


def remove_member_from_team(team_id: str, user_id: str) -> bool:
    """从队伍移除成员"""
    teams = load_teams()

    if team_id not in teams:
        return False

    team = teams[team_id]

    if user_id not in team['members']:
        return False

    # 如果是队长离开
    if user_id == team['leader']:
        # 如果有其他成员，转让队长
        if len(team['members']) > 1:
            # 移除队长
            team['members'].remove(user_id)
            # 设置新队长（第一个成员）
            team['leader'] = team['members'][0]
        else:
            # 删除队伍
            del teams[team_id]
    else:
        # 普通成员离开
        team['members'].remove(user_id)

    save_teams(teams)

    # 更新缓存
    if team_id in teams:
        team_data_cache[team_id] = teams[team_id]
    elif team_id in team_data_cache:
        del team_data_cache[team_id]

    return True


def disband_team(team_id: str) -> bool:
    """解散队伍"""
    teams = load_teams()

    if team_id not in teams:
        return False

    del teams[team_id]
    save_teams(teams)

    # 清理缓存
    if team_id in team_data_cache:
        del team_data_cache[team_id]

    return True


async def expire_team_invite(user_id: str, invite_id: str, bot: Bot, event):
    """组队邀请过期处理"""
    await asyncio.sleep(60)

    if str(user_id) in team_invite_cache and team_invite_cache[str(user_id)]['invite_id'] == invite_id:
        msg = f"组队邀请已过期！"
        await handle_send(bot, event, msg)
        # 删除过期的邀请
        del team_invite_cache[str(user_id)]