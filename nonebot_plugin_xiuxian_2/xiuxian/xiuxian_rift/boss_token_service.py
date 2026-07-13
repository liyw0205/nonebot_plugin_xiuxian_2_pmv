from __future__ import annotations

from .key_settlement_service import RiftKeySettlementResult, RiftKeySettlementService


class RiftBossTokenService(RiftKeySettlementService):
    """Claim an active rift with a boss token using a distinct idempotency table."""

    def __init__(self, database, lock=None) -> None:
        super().__init__(database, lock=lock, operation_table="rift_boss_token_operations")


RiftBossTokenResult = RiftKeySettlementResult

__all__ = ["RiftBossTokenResult", "RiftBossTokenService"]
