from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .repository import PlayerStateRepository, PlayerStateResult


class PlayerStateApplication:
    """Application boundary for initializing empty player HP/MP/ATK values."""

    def __init__(
        self,
        player_database: str | Path,
        *,
        repository: PlayerStateRepository | None = None,
    ) -> None:
        self.repository = repository or PlayerStateRepository(player_database)

    def initialize_if_empty(
        self,
        user_id: str,
        *,
        fallback: Callable[[str], Any] | None = None,
    ) -> PlayerStateResult:
        return self.repository.initialize_if_empty(user_id, fallback=fallback)

    def update_vitals(
        self,
        user_id: str,
        hp: int,
        mp: int,
        *,
        expected_hp: int | None = None,
        expected_mp: int | None = None,
        fallback: Callable[[str, int, int], Any] | None = None,
    ) -> PlayerStateResult:
        return self.repository.update_vitals(
            user_id,
            hp,
            mp,
            expected_hp=expected_hp,
            expected_mp=expected_mp,
            fallback=fallback,
        )


__all__ = ["PlayerStateApplication", "PlayerStateResult"]
