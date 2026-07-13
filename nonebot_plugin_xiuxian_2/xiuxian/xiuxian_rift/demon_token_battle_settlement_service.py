from __future__ import annotations

from .key_event_settlement_service import (
    RiftKeyEventSettlementResult,
    RiftKeyEventSettlementService,
)


class RiftDemonTokenBattleSettlementService(RiftKeyEventSettlementService):
    """Commit a pre-rolled demon-token Boss battle as one rift transaction."""

    def __init__(self, game_database, player_database, lock=None) -> None:
        super().__init__(
            game_database,
            player_database,
            lock=lock,
            operation_table="rift_demon_token_battle_operations",
        )


RiftDemonTokenBattleSettlementResult = RiftKeyEventSettlementResult

__all__ = ["RiftDemonTokenBattleSettlementResult", "RiftDemonTokenBattleSettlementService"]
