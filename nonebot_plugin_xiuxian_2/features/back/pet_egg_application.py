from __future__ import annotations

from pathlib import Path

from .pet_egg_repository import BatchPetEggUseResult, PetEggUseSqlRepository


class PetEggApplication:
    """Application boundary for atomic batch pet-egg consumption."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: PetEggUseSqlRepository | None = None,
    ) -> None:
        self.repository = repository or PetEggUseSqlRepository(game_database, player_database)

    def use(self, operation_id: str, user_id: str, **kwargs) -> BatchPetEggUseResult:
        return self.repository.use_pet_eggs(operation_id, user_id, **kwargs)


__all__ = ["BatchPetEggUseResult", "PetEggApplication"]
