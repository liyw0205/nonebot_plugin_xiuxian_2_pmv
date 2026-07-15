import asyncio
import json
import time
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from typing import Any, Dict, Optional

from nonebot import Bot

from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.utils import handle_send
from ...paths import get_paths
from .transaction_service import DungeonTeamTransactionService, TeamInviteSnapshot

sql_message = XiuxianDateManage()  # sql类
player_data = PlayerDataManager() # PlayerDataManager实例

# 表名常量
TEAM_TABLE = "teams" # 队伍信息表
# TEAM_MEMBER_TABLE = "team_members" # 如果需要独立成员表，但目前成员列表会直接存入 TEAM_TABLE


class PersistentTeamInviteMapping(MutableMapping[str, Dict[str, Any]]):
    """Compatibility mapping whose authoritative state is the invite table."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        service: DungeonTeamTransactionService | None = None,
    ) -> None:
        self._database = Path(database) if database is not None else None
        self._service_override = service

    def _service(self) -> DungeonTeamTransactionService:
        if self._service_override is not None:
            return self._service_override
        return DungeonTeamTransactionService(self._database or get_paths().player_db)

    @staticmethod
    def _as_dict(invite: TeamInviteSnapshot) -> Dict[str, Any]:
        return {
            "team_id": invite.team_id,
            "inviter": invite.inviter_id,
            "timestamp": invite.created_at,
            "invite_id": invite.invite_id,
            "group_id": invite.group_id,
            "expires_at": invite.expires_at,
        }

    def __getitem__(self, user_id: str) -> Dict[str, Any]:
        invite = self._service().pending_invite(str(user_id), time.time())
        if invite is None:
            raise KeyError(str(user_id))
        return self._as_dict(invite)

    def __setitem__(self, user_id: str, value: Dict[str, Any]) -> None:
        created_at = float(value.get("timestamp", time.time()))
        invite_id = str(value["invite_id"])
        self._service().record_invite(
            invite_id,
            str(value["team_id"]),
            str(value["inviter"]),
            str(user_id),
            str(value["group_id"]),
            float(value.get("expires_at", created_at + 60)),
        )

    def __delitem__(self, user_id: str) -> None:
        service = self._service()
        invite = service.pending_invite(str(user_id), time.time())
        if invite is None:
            return
        service.reject(
            f"dungeon-team-reject-compat:{invite.invite_id}",
            invite.invite_id,
            invite.invitee_id,
        )

    def __iter__(self) -> Iterator[str]:
        invites = self._service().list_pending_invites(time.time())
        return iter(tuple(invite.invitee_id for invite in invites))

    def __len__(self) -> int:
        return len(self._service().list_pending_invites(time.time()))


team_invite_cache: MutableMapping[str, Dict[str, Any]] = PersistentTeamInviteMapping()


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
    try:
        team["version"] = max(int(team.get("version", 0)), 0)
    except (TypeError, ValueError):
        team["version"] = 0
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

    service = (
        team_invite_cache._service()
        if isinstance(team_invite_cache, PersistentTeamInviteMapping)
        else DungeonTeamTransactionService(get_paths().player_db)
    )
    invite = service.invite_by_id(invite_id)
    if invite is None or invite.invitee_id != str(user_id):
        return
    result = service.expire(
        f"dungeon-team-expire:{invite_id}", invite_id, time.time()
    )
    if result.status == "applied":
        msg = "组队邀请已过期！"
        await handle_send(bot, event, msg)
