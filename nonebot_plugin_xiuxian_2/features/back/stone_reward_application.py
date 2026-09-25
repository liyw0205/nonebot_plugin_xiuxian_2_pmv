from __future__ import annotations

from pathlib import Path

from .stone_reward_repository import StoneRewardResult, StoneRewardSqlRepository


class StoneRewardApplication:
    """Feature-owned use case for spirit-stone item rewards."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: StoneRewardSqlRepository | None = None,
    ) -> None:
        self.repository = repository or StoneRewardSqlRepository(database)

    def apply(self, operation_id: str, user_id: str, **kwargs) -> StoneRewardResult:
        return self.repository.apply(operation_id, user_id, **kwargs)


__all__ = ["StoneRewardApplication", "StoneRewardResult"]
