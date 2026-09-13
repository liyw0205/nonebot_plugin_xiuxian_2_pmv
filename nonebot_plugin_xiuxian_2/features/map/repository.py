from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class MapRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyMapRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_map.transaction_service import (
            MapCombatLifecycleService, MapCombatSettlementService, MapDongfuBuildService,
            MapExploreSettlementService, MapExploreStartService, MapHomeReturnService,
            MapInteractiveActionService, MapMissionClaimService, MapMovementSettlementService,
            MapResourceRewardService, SeedPurchaseService,
        )
        mapping = {
            "move": (MapMovementSettlementService, "move", (self.game_database, self.player_database)),
            "return_home": (MapHomeReturnService, "return_home", (self.player_database,)),
            "interactive_start": (MapInteractiveActionService, "start", (self.game_database, self.player_database)),
            "interactive_finish": (MapInteractiveActionService, "save_settlement", (self.game_database, self.player_database)),
            "combat_start": (MapCombatLifecycleService, "start", (self.game_database, self.player_database)),
            "combat_settle": (MapCombatSettlementService, "settle", (self.game_database, self.player_database)),
            "explore_start": (MapExploreStartService, "start", (self.game_database, self.player_database)),
            "explore_settle": (MapExploreSettlementService, "settle", (self.game_database, self.player_database)),
            "resource_reward": (MapResourceRewardService, "settle", (self.game_database, self.player_database)),
            "mission_claim": (MapMissionClaimService, "claim", (self.game_database, self.player_database)),
            "purchase_seed": (SeedPurchaseService, "purchase", (self.game_database,)),
            "build_dongfu": (MapDongfuBuildService, "build", (self.game_database, self.player_database)),
        }
        cls, method, databases = mapping[action]
        return getattr(cls(*databases), method)(operation_id, user_id, **kwargs)


__all__ = ["LegacyMapRepository", "MapRepository"]
