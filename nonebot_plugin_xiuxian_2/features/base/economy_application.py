from __future__ import annotations

from pathlib import Path

from .economy_repository import EconomyMutationResult, PlayerEconomySqlRepository


class PlayerEconomyApplication:
    """Application boundary for positive player economy rewards."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: PlayerEconomySqlRepository | None = None,
    ) -> None:
        self.repository = repository or PlayerEconomySqlRepository(database)

    def grant_stone(
        self,
        user_id: str,
        amount: int,
        *,
        operation_id: str = "",
    ) -> EconomyMutationResult:
        return self.repository.grant_stone(user_id, amount)

    def grant_experience(
        self,
        user_id: str,
        amount: int,
        *,
        max_exp: int,
        expected_exp: int | None = None,
        operation_id: str = "",
    ) -> EconomyMutationResult:
        return self.repository.grant_experience(
            user_id,
            amount,
            max_exp=max_exp,
            expected_exp=expected_exp,
        )

    def grant_sect_contribution(
        self,
        user_id: str,
        amount: int,
        *,
        expected_value: int | None = None,
        operation_id: str = "",
    ) -> EconomyMutationResult:
        return self.repository.grant_sect_contribution(
            user_id,
            amount,
            expected_value=expected_value,
        )


__all__ = ["EconomyMutationResult", "PlayerEconomyApplication"]
