from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class TeamMemberView:
    user_id: str
    user_name: str
    is_leader: bool


@dataclass(frozen=True)
class TeamViewResult:
    status: str
    team_info: dict[str, Any] | None = None
    members: tuple[TeamMemberView, ...] = ()


@dataclass(frozen=True)
class TeamTransferResult:
    status: str
    target_user_id: str = ""
    target_user_name: str = ""


def build_team_view(team_info: dict[str, Any], lookup_user_name: Callable[[str], str]) -> TeamViewResult:
    members = []
    leader_id = str(team_info.get("leader", ""))
    for member_id in team_info.get("members", []):
        normalized_member_id = str(member_id)
        members.append(
            TeamMemberView(
                user_id=normalized_member_id,
                user_name=lookup_user_name(normalized_member_id),
                is_leader=normalized_member_id == leader_id,
            )
        )
    return TeamViewResult("ok", team_info=dict(team_info), members=tuple(members))


def build_team_view_message(result: TeamViewResult) -> str:
    if result.team_info is None:
        raise ValueError("team_info 不能为空")

    members_info = []
    for member in result.members:
        prefix = "👑" if member.is_leader else "👤"
        members_info.append(f"{prefix} {member.user_name}")

    members_str_formatted = "\n".join(members_info)
    team_info = result.team_info
    return (
        f"【队伍信息】\n"
        f"队伍名：{team_info['team_name']}\n"
        f"队伍ID：{team_info['team_id']}\n"
        f"创建时间：{team_info['create_time']}\n"
        f"成员：{len(team_info['members'])}/{team_info['max_members']}\n"
        f"{members_str_formatted}\n"
        f"操作：探索副本 / 离开队伍"
    )


def resolve_transfer_target(
    *,
    actor_user_id: str,
    team_info: dict[str, Any],
    at_target_user_id: str | None,
    arg_target_user_id: str | None,
    lookup_user_name: Callable[[str], str | None],
) -> TeamTransferResult:
    target_user_id = at_target_user_id or arg_target_user_id or ""
    if not target_user_id:
        return TeamTransferResult("target_not_found")
    if target_user_id == actor_user_id:
        return TeamTransferResult("self_target", target_user_id=target_user_id)
    if target_user_id not in team_info.get("members", []):
        return TeamTransferResult("target_not_member", target_user_id=target_user_id)

    target_user_name = lookup_user_name(target_user_id)
    if not target_user_name:
        return TeamTransferResult("target_info_missing", target_user_id=target_user_id)
    return TeamTransferResult(
        "ok",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
    )


def build_transfer_team_success_message(target_user_name: str) -> str:
    return f"👑 队长已成功转移给 {target_user_name}！"


def build_transfer_team_self_message() -> str:
    return "你已经是队长了，无需转移给自己。"


def build_transfer_team_not_member_message() -> str:
    return "只能将队长转移给当前队伍内的成员！"


__all__ = [
    "TeamMemberView",
    "TeamTransferResult",
    "TeamViewResult",
    "build_team_view",
    "build_team_view_message",
    "resolve_transfer_target",
    "build_transfer_team_success_message",
    "build_transfer_team_self_message",
    "build_transfer_team_not_member_message",
]
