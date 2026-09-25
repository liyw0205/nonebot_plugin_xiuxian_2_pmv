from __future__ import annotations

from pathlib import Path

from .partner_token_repository import PartnerTokenUseResult, PartnerTokenUseSqlRepository


class PartnerTokenUseApplication:
    """Application boundary for consuming tokens that restore partner uses."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: PartnerTokenUseSqlRepository | None = None,
    ) -> None:
        self.repository = repository or PartnerTokenUseSqlRepository(
            game_database, player_database
        )

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        *,
        requested_count: int,
        expected_item_count: int,
        expected_used_count: int,
    ) -> PartnerTokenUseResult:
        return self.repository.apply(
            operation_id,
            user_id,
            item_id,
            requested_count=requested_count,
            expected_item_count=expected_item_count,
            expected_used_count=expected_used_count,
        )


__all__ = ["PartnerTokenUseApplication", "PartnerTokenUseResult"]
