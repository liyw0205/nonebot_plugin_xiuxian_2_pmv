from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class ArenaRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyArenaRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_arena.transaction_service import (
            ArenaPurchaseService,
            ArenaChallengePurchaseService,
            ArenaChallengeSettlementService,
        )

        return ArenaPurchaseService(self.game_database, self.player_database), ArenaChallengePurchaseService(self.game_database, self.player_database), ArenaChallengeSettlementService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].purchase(*args, **kwargs)

    def purchase_challenges(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].purchase(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[2].settle(*args, **kwargs)


__all__ = ["ArenaRepository", "LegacyArenaRepository"]
