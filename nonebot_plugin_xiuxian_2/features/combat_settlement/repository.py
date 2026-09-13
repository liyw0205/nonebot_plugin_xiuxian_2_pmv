from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class CombatSettlementRepository(Protocol):
    def settle(self, operation_id: str, user_id: str, expected_daily: dict[str, Any], snapshot: str,
               daily_limit: int, stone: int, items: tuple[dict[str, Any], ...], max_goods_num: int) -> Any: ...


class LegacyCombatSettlementRepository:
    """Lazy adapter around the historical attached-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def settle(self, operation_id: str, user_id: str, expected_daily: dict[str, Any], snapshot: str,
               daily_limit: int, stone: int, items: tuple[dict[str, Any], ...], max_goods_num: int) -> Any:
        try:
            from ...xiuxian.xiuxian_map.transaction_service import MapCombatSettlementService
        except (ImportError, RuntimeError, ValueError):
            return {"status": "not_ready", "stone": 0, "rewards": ()}
        return MapCombatSettlementService(self.game_database, self.player_database).settle(
            operation_id,
            user_id,
            expected_daily,
            snapshot,
            daily_limit,
            stone,
            items,
            max_goods_num,
        )


__all__ = ["CombatSettlementRepository", "LegacyCombatSettlementRepository"]
