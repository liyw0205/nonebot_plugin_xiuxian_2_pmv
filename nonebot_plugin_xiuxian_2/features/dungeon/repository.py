from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class DungeonRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def operation_result(self, *args: Any, **kwargs: Any) -> Any: ...
    def replay(self, *args: Any, **kwargs: Any) -> Any: ...
    def prepare(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...
    def resolve_rejection(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyDungeonRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _purchase_service(self):
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonPurchaseService

        return DungeonPurchaseService(self.game_database)

    def _explore_service(self):
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonExploreOperationService

        return DungeonExploreOperationService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._purchase_service().purchase(*args, **kwargs)

    def operation_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._purchase_service().operation_result(*args, **kwargs)

    def replay(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().replay(*args, **kwargs)

    def prepare(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().prepare(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().settle(*args, **kwargs)

    def resolve_rejection(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().resolve_rejection(*args, **kwargs)


__all__ = ["DungeonRepository", "LegacyDungeonRepository"]
