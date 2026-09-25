from __future__ import annotations

from pathlib import Path

from .three_cultivation_pill_repository import (
    ThreeCultivationPillResult,
    ThreeCultivationPillSqlRepository,
)


class ThreeCultivationPillApplication:
    """Feature-owned use case for three-cultivation pills."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: ThreeCultivationPillSqlRepository | None = None,
    ) -> None:
        self.repository = repository or ThreeCultivationPillSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, *args, **kwargs) -> ThreeCultivationPillResult:
        return self.repository.apply(operation_id, user_id, *args, **kwargs)


__all__ = ["ThreeCultivationPillApplication", "ThreeCultivationPillResult"]
