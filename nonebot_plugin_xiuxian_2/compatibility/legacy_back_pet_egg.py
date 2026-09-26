"""Explicit rollback wrapper for the historical pet-egg batch service."""

from __future__ import annotations

from pathlib import Path
from threading import RLock

from ..features.back.pet_egg_repository import BatchPetEggUseResult, PetEggUseSqlRepository


class BatchItemUseService(PetEggUseSqlRepository):
    """Compatibility wrapper that preserves request-time schema setup."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        super().__init__(
            game_database,
            player_database,
            lock=lock,
            ensure_schema=True,
        )


__all__ = ["BatchPetEggUseResult", "BatchItemUseService"]
