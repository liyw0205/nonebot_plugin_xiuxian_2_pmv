from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class BaseRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyBaseRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_base.transaction_service import (
            BreakthroughService, DestinyTribulationService, OrdinaryTribulationService,
            PlayerRenameService, StoneContestService, StoneRobberySettlementService,
            SignInService,
        )
        mapping = {
            "breakthrough": (BreakthroughService, "apply_success", (self.game_database,)),
            "tribulation": (OrdinaryTribulationService, "settle", (self.game_database, self.player_database)),
            "rename": (PlayerRenameService, "rename_user", (self.game_database,)),
            "stone_contest": (StoneContestService, "transfer", (self.game_database,)),
            "stone_robbery": (StoneRobberySettlementService, "settle", (self.game_database, self.player_database)),
            "sign": (SignInService, "sign", (self.game_database,)),
        }
        cls, method, databases = mapping[action]
        return getattr(cls(*databases), method)(operation_id, user_id, **kwargs)


__all__ = ["BaseRepository", "LegacyBaseRepository"]
