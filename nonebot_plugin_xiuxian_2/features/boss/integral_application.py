from __future__ import annotations

from pathlib import Path

from .integral_repository import BossIntegralMutation, BossIntegralSqlRepository


class BossIntegralApplication:
    """Application boundary for compatibility reward grants to boss points."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: BossIntegralSqlRepository | None = None,
    ) -> None:
        self.repository = repository or BossIntegralSqlRepository(database)

    def grant_integral(self, user_id: str, amount: int) -> BossIntegralMutation:
        return self.repository.grant_integral(user_id, amount)


__all__ = ["BossIntegralApplication", "BossIntegralMutation"]
