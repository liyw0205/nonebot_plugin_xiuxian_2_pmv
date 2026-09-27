from __future__ import annotations

import time
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..paths import get_paths

if TYPE_CHECKING:
    from .legacy_dungeon_team_transactions import (
        DungeonTeamTransactionService,
        TeamInviteSnapshot,
    )


class PersistentTeamInviteMapping(MutableMapping[str, dict[str, Any]]):
    """Legacy mapping facade backed by the persistent invite table."""

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
        from .legacy_dungeon_team_transactions import DungeonTeamTransactionService

        return DungeonTeamTransactionService(self._database or get_paths().player_db)

    @staticmethod
    def _as_dict(invite: TeamInviteSnapshot) -> dict[str, Any]:
        return {
            "team_id": invite.team_id,
            "inviter": invite.inviter_id,
            "timestamp": invite.created_at,
            "invite_id": invite.invite_id,
            "group_id": invite.group_id,
            "expires_at": invite.expires_at,
        }

    def __getitem__(self, user_id: str) -> dict[str, Any]:
        invite = self._service().pending_invite(str(user_id), time.time())
        if invite is None:
            raise KeyError(str(user_id))
        return self._as_dict(invite)

    def __setitem__(self, user_id: str, value: dict[str, Any]) -> None:
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


team_invite_cache: MutableMapping[str, dict[str, Any]] = PersistentTeamInviteMapping()


__all__ = ["PersistentTeamInviteMapping", "team_invite_cache"]
