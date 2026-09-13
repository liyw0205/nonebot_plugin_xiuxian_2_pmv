from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class WorldEventClaimRepository(Protocol):
    def claim(
        self,
        operation_id: str,
        event_key: str,
        event_id: str,
        user_id: str,
        expected_claimed: Mapping[str, Any],
        stone: int,
        exp: int,
        items: Sequence[Mapping[str, Any]],
        max_goods_num: int,
    ) -> Any: ...


class LegacyWorldEventClaimRepository:
    """Lazy adapter around the existing ATTACH DATABASE transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def claim(
        self,
        operation_id: str,
        event_key: str,
        event_id: str,
        user_id: str,
        expected_claimed: Mapping[str, Any],
        stone: int,
        exp: int,
        items: Sequence[Mapping[str, Any]],
        max_goods_num: int,
    ) -> Any:
        from ...xiuxian.xiuxian_world_events.transaction_service import DemonClaimService

        return DemonClaimService(self.game_database, self.player_database).claim(
            operation_id,
            event_key,
            event_id,
            user_id,
            expected_claimed,
            stone,
            exp,
            items,
            max_goods_num,
        )


__all__ = ["LegacyWorldEventClaimRepository", "WorldEventClaimRepository"]
