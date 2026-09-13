from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class TowerRepository(Protocol):
    def purchase(self, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, weekly_limit: int, expected_score: int, expected_weekly_purchases: Mapping[str, Any], max_goods_num: int, bind_flag: int = 1, today: Any = None) -> Any: ...
    def settle(self, operation_id: str, user_id: str, expected_tower: Mapping[str, Any], floor: int, score: int, stone: int, exp: int, items: Sequence[Mapping[str, Any]], max_goods_num: int, *, expected_player: Mapping[str, Any] | None = None, final_hp: int | None = None, final_mp: int | None = None, stamina_cost: int = 0, challenge_succeeded: bool = True) -> Any: ...


class LegacyTowerRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_tower.transaction_service import TowerPurchaseService, TowerSettlementService

        return TowerPurchaseService(self.game_database, self.player_database), TowerSettlementService(self.game_database, self.player_database)

    def purchase(self, *args, **kwargs):
        return self._services()[0].purchase(*args, **kwargs)

    def settle(self, *args, **kwargs):
        return self._services()[1].settle(*args, **kwargs)


__all__ = ["LegacyTowerRepository", "TowerRepository"]
