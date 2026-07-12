from __future__ import annotations

from typing import Any


def build_team_view_message(team_info: dict[str, Any], member_names: list[str]) -> str:
    members_info = []
    leader_id = str(team_info.get("leader", ""))
    for index, member_name in enumerate(member_names):
        member_id = ""
        members = team_info.get("members", [])
        if index < len(members):
            member_id = str(members[index])
        prefix = "👑" if member_id == leader_id else "👤"
        members_info.append(f"{prefix} {member_name}")

    members_str_formatted = "\n".join(members_info)
    return (
        f"【队伍信息】\n"
        f"队伍名：{team_info['team_name']}\n"
        f"队伍ID：{team_info['team_id']}\n"
        f"创建时间：{team_info['create_time']}\n"
        f"成员：{len(team_info['members'])}/{team_info['max_members']}\n"
        f"{members_str_formatted}\n"
        f"操作：探索副本 / 离开队伍"
    )


def build_transfer_team_success_message(target_user_name: str) -> str:
    return f"👑 队长已成功转移给 {target_user_name}！"


__all__ = [
    "build_team_view_message",
    "build_transfer_team_success_message",
]
