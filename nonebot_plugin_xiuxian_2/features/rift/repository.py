from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class RiftRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyRiftRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_rift.transaction_service import (
            RiftEntryService, RiftKeyEventSettlementService,
            RiftSettlementService, RiftSpeedupService, RiftTerminationService,
        )
        if action == "generate":
            return RiftEntryService(self.game_database).generate(operation_id, kwargs.pop("rift_key"), kwargs.pop("rift_plan"))
        if action == "enter":
            return RiftEntryService(self.game_database).enter(operation_id, user_id, **kwargs)
        if action == "terminate":
            return RiftTerminationService(self.game_database).terminate(operation_id, user_id, kwargs.pop("rift_data"))
        if action == "event_settle":
            return RiftKeyEventSettlementService(self.game_database, self.player_database).settle(operation_id, user_id, **kwargs)
        if action == "speedup":
            return RiftSpeedupService(self.game_database).apply(operation_id, user_id, **kwargs)
        return RiftSettlementService(self.game_database, self.player_database).settle(operation_id, user_id, **kwargs)


__all__ = ["LegacyRiftRepository", "RiftRepository"]
