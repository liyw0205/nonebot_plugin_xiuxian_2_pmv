from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class BuffRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyBuffRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_buff.transaction_service import (
            BlessedSpotService, ClosingSettlementService, NormalPvpSettlementService,
            NormalTrainingLifecycleService, StoneTrainingSettlementService,
        )
        mapping = {
            "open": (BlessedSpotService, "open", (self.game_database, self.player_database)),
            "upgrade_field": (BlessedSpotService, "upgrade_field", (self.game_database, self.player_database)),
            "rename": (BlessedSpotService, "rename", (self.game_database, self.player_database)),
            "training_start": (NormalTrainingLifecycleService, "start", (self.game_database, self.player_database)),
            "training_complete": (NormalTrainingLifecycleService, "complete", (self.game_database,)),
            "stone_training": (StoneTrainingSettlementService, "settle", (self.game_database, self.player_database)),
            "pvp_settle": (NormalPvpSettlementService, "settle", (self.game_database, self.player_database)),
        }
        cls, method, databases = mapping[action]
        return getattr(cls(*databases), method)(operation_id, user_id, **kwargs)


__all__ = ["BuffRepository", "LegacyBuffRepository"]
