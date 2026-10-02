"""Application boundary for player avatar identity state."""

from __future__ import annotations

from pathlib import Path

from .avatar_repository import AvatarStateResult, AvatarStateSqlRepository


class PlayerAvatarApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: AvatarStateSqlRepository | None = None,
    ) -> None:
        self.repository = repository or AvatarStateSqlRepository(database)

    def get_active_user_id(self, user_id: str) -> str:
        return self.repository.get_active_id(str(user_id)) or str(user_id)

    def toggle_active(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_active_id: str,
    ) -> AvatarStateResult:
        return self.repository.toggle(
            operation_id=operation_id,
            user_id=user_id,
            expected_active_id=expected_active_id,
        )

    def restore_active(self, *, operation_id: str, user_id: str) -> AvatarStateResult:
        return self.repository.restore(operation_id=operation_id, user_id=user_id)


__all__ = ["PlayerAvatarApplication"]
