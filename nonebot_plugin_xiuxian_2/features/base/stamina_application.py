from __future__ import annotations

from pathlib import Path
from typing import Any

from .stamina_repository import PlayerStaminaSqlRepository


class PlayerStaminaApplication:
    """Application boundary for command-level stamina consumption."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: PlayerStaminaSqlRepository | None = None,
    ) -> None:
        self.repository = repository or PlayerStaminaSqlRepository(database)

    def consume(
        self,
        user_id: str,
        amount: int,
        *,
        expected_stamina: int | None = None,
    ) -> dict[str, Any]:
        return self.repository.consume(
            user_id,
            amount,
            expected_stamina=expected_stamina,
        )

    def recover(
        self,
        max_stamina: int,
        points: int,
        *,
        batch_size: int = 1000,
    ) -> dict[str, Any]:
        return self.repository.recover(
            max_stamina,
            points,
            batch_size=batch_size,
        )


__all__ = ["PlayerStaminaApplication"]
