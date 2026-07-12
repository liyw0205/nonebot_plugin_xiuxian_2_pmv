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


@dataclass(frozen=True)
class TeamInviteResult:
    status: str
    target_user_id: str = ""
    target_user_name: str = ""
    inviter_name: str = ""
    group_id: str = ""
    remaining_seconds: int = 0


@dataclass(frozen=True)
class TeamInviteResponseResult:
    status: str
    team_name: str = ""
    leader_name: str = ""
    member_count: int = 0
    max_members: int = 0
    invite_group_id: str = ""


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


def resolve_team_invite(
    *,
    target_user_id: str | None,
    target_user_name: str | None,
    cooldown_seconds: int,
    target_team_id: str | None,
    pending_inviter_name: str | None,
    pending_remaining_seconds: int,
) -> TeamInviteResult:
    if not target_user_id:
        return TeamInviteResult("target_not_found")
    if not target_user_name:
        return TeamInviteResult("target_info_missing", target_user_id=target_user_id)
    if cooldown_seconds > 0:
        return TeamInviteResult(
            "target_in_cooldown",
            target_user_id=target_user_id,
            target_user_name=target_user_name,
            remaining_seconds=cooldown_seconds,
        )
    if target_team_id:
        return TeamInviteResult(
            "target_has_team",
            target_user_id=target_user_id,
            target_user_name=target_user_name,
        )
    if pending_inviter_name:
        return TeamInviteResult(
            "target_has_pending_invite",
            target_user_id=target_user_id,
            target_user_name=target_user_name,
            inviter_name=pending_inviter_name,
            remaining_seconds=max(0, pending_remaining_seconds),
        )
    return TeamInviteResult(
        "ready",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
    )


def build_team_invite_message(result: TeamInviteResult, format_duration: Callable[[int], str]) -> str:
    if result.status == "target_not_found":
        return "未找到指定的用户，请检查道号或艾特是否正确！"
    if result.status == "target_in_cooldown":
        return (
            f"{result.target_user_name}当前处于组队冷却中"
            f"（剩余{format_duration(result.remaining_seconds)}），不可被邀请。"
        )
    if result.status == "target_has_team":
        return f"{result.target_user_name}已有队伍！"
    if result.status == "target_has_pending_invite":
        return (
            f"对方已有来自{result.inviter_name}的组队邀请"
            f"（剩余{result.remaining_seconds}秒），请稍后再试！"
        )
    if result.status == "ready":
        return f"📨 已向{result.target_user_name}发送组队邀请，等待对方回应..."
    return "目标用户信息异常，无法发送邀请！"


def build_team_invite_private_message(*, group_id: str, inviter_name: str) -> str:
    return (
        f"你在群{group_id}收到了来自{inviter_name}的组队邀请，"
        "请在1分钟内回复【同意组队】或【拒绝组队】。"
    )


def resolve_invite_response(
    *,
    has_invite: bool,
    invite_group_id: str | None,
    current_group_id: str | None,
    team_exists: bool,
    user_has_team: bool,
    member_count: int,
    max_members: int,
) -> TeamInviteResponseResult:
    if not has_invite:
        return TeamInviteResponseResult("no_invite")
    normalized_invite_group = str(invite_group_id or "")
    if current_group_id is not None and str(current_group_id) != normalized_invite_group:
        return TeamInviteResponseResult(
            "wrong_group",
            invite_group_id=normalized_invite_group,
        )
    if not team_exists:
        return TeamInviteResponseResult("team_disbanded")
    if user_has_team:
        return TeamInviteResponseResult("user_has_team")
    if member_count >= max_members:
        return TeamInviteResponseResult("team_full")
    return TeamInviteResponseResult("ready")


def build_invite_response_message(result: TeamInviteResponseResult) -> str:
    if result.status == "no_invite":
        return "没有待处理的组队邀请！"
    if result.status == "wrong_group":
        return f"此邀请是在群{result.invite_group_id}发出的，请在该群或私聊中进行操作。"
    if result.status == "team_disbanded":
        return "该队伍已解散！"
    if result.status == "user_has_team":
        return "你已经在一个队伍中了，无法接受邀请！"
    if result.status == "team_full":
        return "该队伍已满员！"
    if result.status == "rejected":
        return "已拒绝组队邀请。"
    if result.status == "join_failed":
        return "加入队伍失败！"
    if result.status == "joined":
        return (
            f"✅ 你已成功加入队伍【{result.team_name}】！\n"
            f"👑 队长：{result.leader_name}\n"
            f"👥 当前成员：{result.member_count}/{result.max_members}"
        )
    raise ValueError(f"无法展示邀请响应状态: {result.status}")


__all__ = [
    "TeamMemberView",
    "TeamTransferResult",
    "TeamViewResult",
    "TeamLeaveResult",
    "TeamKickResult",
    "TeamInviteResult",
    "TeamInviteResponseResult",
    "build_team_view",
    "build_team_view_message",
    "build_leave_team_message",
    "build_leave_team_result",
    "build_kick_team_message",
    "build_kick_team_result",
    "resolve_team_invite",
    "build_team_invite_message",
    "build_team_invite_private_message",
    "resolve_invite_response",
    "build_invite_response_message",
    "resolve_transfer_target",
    "resolve_kick_target",
    "build_transfer_team_success_message",
    "build_transfer_team_self_message",
    "build_transfer_team_not_member_message",
]
