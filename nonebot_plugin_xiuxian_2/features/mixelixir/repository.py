from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class MixelixirRepository(Protocol):
    def harvest(self, operation_id: str, user_id: str, expected_last_time: str, harvested_at: str, rewards: Sequence[Mapping[str, Any]], *, max_goods_num: int) -> Any: ...
    def settle(self, operation_id: str, user_id: str, materials: Mapping[int, int], reward_id: int, reward_name: str, reward_quantity: int, *, max_goods_num: int) -> Any: ...


class LegacyMixelixirRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_mixelixir.transaction_service import MixelixirHarvestService, MixelixirSettlementService

        return MixelixirHarvestService(self.game_database, self.player_database), MixelixirSettlementService(self.game_database)

    def harvest(self, operation_id, user_id, expected_last_time, harvested_at, rewards, *, max_goods_num):
        return self._services()[0].harvest(
            operation_id, user_id, expected_last_time, harvested_at,
            [(item["item_id"], item["name"], item["quantity"]) for item in rewards],
            max_goods_num=max_goods_num,
        )

    def settle(self, operation_id, user_id, materials, reward_id, reward_name, reward_quantity, *, max_goods_num):
        return self._services()[1].settle(
            operation_id, user_id, materials, reward_id, reward_name, reward_quantity,
            max_goods_num=max_goods_num,
        )


__all__ = ["LegacyMixelixirRepository", "MixelixirRepository"]
