from __future__ import annotations

from pathlib import Path
from typing import Any

from .team_repository import DungeonTeamRepository, TeamInviteSnapshot, TeamMutationResult


class DungeonTeamApplication:
    def __init__(self, player_database: str | Path, *, repository: DungeonTeamRepository | None = None) -> None:
        self.repository = repository or DungeonTeamRepository(player_database)

    def create(self, operation_id: str, team_id: str, team_name: str, leader_id: str, group_id: str, created_at: str, now_timestamp: float) -> TeamMutationResult:
        return self.repository.create(operation_id, team_id, team_name, leader_id, group_id, created_at, now_timestamp)

    def invite(self, operation_id: str, invite_id: str, team_id: str, inviter_id: str, invitee_id: str, group_id: str, expires_at: float, now_timestamp: float) -> TeamMutationResult:
        return self.repository.invite(operation_id, invite_id, team_id, inviter_id, invitee_id, group_id, expires_at, now_timestamp)

    def join(self, operation_id: str, invite_id: str, team_id: str, inviter_id: str, user_id: str, group_id: str, now_timestamp: float) -> TeamMutationResult:
        return self.repository.join(operation_id, invite_id, team_id, inviter_id, user_id, group_id, now_timestamp)

    def operation_result(self, operation_id: str, action: str = "") -> TeamMutationResult | None:
        return self.repository.operation_result(operation_id, action)

    def pending_invite(self, user_id: str, now_timestamp: float) -> TeamInviteSnapshot | None:
        return self.repository.pending_invite(user_id, now_timestamp)


__all__ = ["DungeonTeamApplication"]
