from __future__ import annotations

from pathlib import Path
from typing import Any

from .alchemy_repository import AlchemyResult, AlchemySqlRepository


class AlchemyApplication:
    """Feature-owned use case for single and batch alchemy commands."""

    def __init__(self, database: str | Path, *, repository: AlchemySqlRepository | None = None) -> None:
        self.repository = repository or AlchemySqlRepository(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        reward_stone: int,
        consume_items: list[tuple[int, int]] | tuple[tuple[int, int], ...],
    ) -> AlchemyResult:
        return self.repository.apply(operation_id, user_id, reward_stone, consume_items)


__all__ = ["AlchemyApplication", "AlchemyResult"]
