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


@dataclass(frozen=True)
class TeamLeaveResult:
    status: str
    team_name: str = ""
    new_leader_name: str = ""
    cooldown_hours: int = 0


@dataclass(frozen=True)
class TeamKickResult:
    status: str
    target_user_id: str = ""
    target_user_name: str = ""
    cooldown_hours: int = 0


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


def build_leave_team_result(
    *,
    team_info: dict[str, Any],
    leaver_user_id: str,
    success: bool,
    cooldown_hours: int,
    new_leader_name: str | None,
) -> TeamLeaveResult:
    if not success:
        return TeamLeaveResult("leave_failed")

    if leaver_user_id != str(team_info.get("leader", "")):
        return TeamLeaveResult(
            "member_left",
            team_name=str(team_info.get("team_name", "")),
            cooldown_hours=cooldown_hours,
        )

    if new_leader_name:
        return TeamLeaveResult(
            "leader_left_transferred",
            team_name=str(team_info.get("team_name", "")),
            new_leader_name=new_leader_name,
            cooldown_hours=cooldown_hours,
        )

    return TeamLeaveResult(
        "leader_left_disbanded",
        team_name=str(team_info.get("team_name", "")),
        cooldown_hours=cooldown_hours,
    )


def build_leave_team_message(result: TeamLeaveResult) -> str:
    if result.status == "leader_left_transferred":
        return (
            f"你已离开队伍【{result.team_name}】，队长已转让给{result.new_leader_name}。\n"
            f"你进入了{result.cooldown_hours}小时组队冷却。"
        )
    if result.status == "leader_left_disbanded":
        return (
            f"你已离开队伍【{result.team_name}】，队伍已解散。\n"
            f"你进入了{result.cooldown_hours}小时组队冷却。"
        )
    if result.status == "member_left":
        return (
            f"你已离开队伍【{result.team_name}】。\n"
            f"你进入了{result.cooldown_hours}小时组队冷却。"
        )
    return "离开队伍失败！"


def resolve_kick_target(
    *,
    actor_user_id: str,
    team_info: dict[str, Any],
    at_target_user_id: str | None,
    arg_target_user_id: str | None,
    lookup_user_name: Callable[[str], str | None],
) -> TeamKickResult:
    target_user_id = at_target_user_id or arg_target_user_id or ""
    if not target_user_id:
        return TeamKickResult("target_not_found")
    if target_user_id == actor_user_id:
        return TeamKickResult("self_target", target_user_id=target_user_id)
    if target_user_id not in team_info.get("members", []):
        return TeamKickResult("target_not_member", target_user_id=target_user_id)

    target_user_name = lookup_user_name(target_user_id)
    if not target_user_name:
        return TeamKickResult("target_info_missing", target_user_id=target_user_id)

    return TeamKickResult(
        "ok",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
    )


def build_kick_team_result(
    *,
    target_user_id: str,
    target_user_name: str,
    success: bool,
    cooldown_hours: int,
) -> TeamKickResult:
    if not success:
        return TeamKickResult("kick_failed", target_user_id=target_user_id)
    return TeamKickResult(
        "kicked",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
        cooldown_hours=cooldown_hours,
    )


def build_kick_team_message(result: TeamKickResult) -> str:
    if result.status == "kicked":
        return (
            f"已将成员{result.target_user_name}踢出队伍。\n"
            f"对方进入{result.cooldown_hours}小时组队冷却。"
        )
    return "踢出成员失败！"


__all__ = [
    "TeamMemberView",
    "TeamTransferResult",
    "TeamViewResult",
    "TeamLeaveResult",
    "TeamKickResult",
    "build_team_view",
    "build_team_view_message",
    "build_leave_team_message",
    "build_leave_team_result",
    "build_kick_team_message",
    "build_kick_team_result",
    "resolve_transfer_target",
    "resolve_kick_target",
    "build_transfer_team_success_message",
    "build_transfer_team_self_message",
    "build_transfer_team_not_member_message",
]
