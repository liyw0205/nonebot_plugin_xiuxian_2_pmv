"""Application boundary for shared player profile reads."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .profile_repository import PlayerProfileSqlRepository


class PlayerProfileApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: PlayerProfileSqlRepository | None = None,
    ) -> None:
        self.repository = repository or PlayerProfileSqlRepository(database)

    def get_user_profile(
        self, user_id: int | str, *, raise_errors: bool = False
    ) -> dict[str, Any] | None:
        if raise_errors:
            return self.repository.get_user_profile(user_id, raise_errors=True)
        return self.repository.get_user_profile(user_id)

    def get_user_profile_by_name(self, user_name: str) -> dict[str, Any] | None:
        return self.repository.get_user_profile_by_name(user_name)

    def get_user_display_profiles_by_ids(
        self, user_ids: list[str]
    ) -> dict[str, dict[str, Any]]:
        return self.repository.get_user_display_profiles_by_ids(user_ids)

    def search_users(self, query: str) -> list[dict[str, Any]]:
        return self.repository.search_users(query)


__all__ = ["PlayerProfileApplication"]
