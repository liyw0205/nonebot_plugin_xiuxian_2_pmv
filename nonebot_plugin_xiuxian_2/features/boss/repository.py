from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class BossRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyBossRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, activity_database: str | Path | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.activity_database = str(activity_database) if activity_database else None

    def _services(self):
        from ...xiuxian.xiuxian_boss.transaction_service import BossPurchaseService, WorldBossBattleSettlementService

        return (
            BossPurchaseService(self.game_database, self.player_database),
            WorldBossBattleSettlementService(self.game_database, self.player_database, self.activity_database),
        )

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].purchase(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].settle(*args, **kwargs)


__all__ = ["BossRepository", "LegacyBossRepository"]
