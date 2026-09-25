from __future__ import annotations

from pathlib import Path

from .breakthrough_rate_item_repository import (
    BreakthroughRateItemResult,
    BreakthroughRateItemSqlRepository,
)


class BreakthroughRateItemApplication:
    """Feature-owned use case for breakthrough-rate elixirs."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: BreakthroughRateItemSqlRepository | None = None,
    ) -> None:
        self.repository = repository or BreakthroughRateItemSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, *args, **kwargs) -> BreakthroughRateItemResult:
        return self.repository.apply(operation_id, user_id, *args, **kwargs)


__all__ = ["BreakthroughRateItemApplication", "BreakthroughRateItemResult"]
