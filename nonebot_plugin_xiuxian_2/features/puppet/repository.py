from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class PuppetRepository(Protocol):
    def purchase(self, operation_id: str, user_id: str, stone_cost: int) -> Any: ...
    def upgrade(self, operation_id: str, user_id: str, upgrade_costs: dict[int, int], *, max_level: int) -> Any: ...
    def harvest(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyPuppetRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _service(self):
        from ...xiuxian.xiuxian_puppet.transaction_service import PuppetOperationService

        return PuppetOperationService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._service().purchase(*args, **kwargs)

    def upgrade(self, *args: Any, **kwargs: Any) -> Any:
        return self._service().upgrade(*args, **kwargs)

    def harvest(self, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_puppet.transaction_service import PuppetHarvestService

        service = PuppetHarvestService(
            self.game_database,
            self.player_database,
            max_goods_num=int(kwargs.pop("max_goods_num")),
        )
        return service.harvest(operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["PuppetRepository", "LegacyPuppetRepository"]
