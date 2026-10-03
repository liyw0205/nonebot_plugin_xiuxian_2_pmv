from __future__ import annotations

from pathlib import Path

from .integral_repository import BossIntegralMutation, BossIntegralSnapshot, BossIntegralSqlRepository


class BossIntegralApplication:
    """Application boundary for world-boss point grants and ranking."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: BossIntegralSqlRepository | None = None,
    ) -> None:
        self.repository = repository or BossIntegralSqlRepository(database)

    def grant_integral(self, user_id: str, amount: int) -> BossIntegralMutation:
        return self.repository.grant_integral(user_id, amount)

    def top_integrals(self, limit: int = 50) -> list[tuple[str, int]]:
        return self.repository.top_integrals(limit)

    def get_integral(self, user_id: str) -> BossIntegralSnapshot:
        return self.repository.get_integral(user_id)


__all__ = ["BossIntegralApplication", "BossIntegralMutation", "BossIntegralSnapshot"]
