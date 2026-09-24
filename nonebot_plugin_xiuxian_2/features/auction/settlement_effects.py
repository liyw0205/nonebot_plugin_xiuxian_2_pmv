from __future__ import annotations

from typing import Any, Mapping, Protocol


class AuctionSettlementEffects(Protocol):
    def on_settlement(
        self,
        *,
        event_id: str,
        event_key: str,
        operation_id: str,
        settlement: Mapping[str, Any],
        occurred_at: str,
        replayed: bool,
    ) -> None: ...


class NullAuctionSettlementEffects:
    def on_settlement(
        self,
        *,
        event_id: str,
        event_key: str,
        operation_id: str,
        settlement: Mapping[str, Any],
        occurred_at: str,
        replayed: bool,
    ) -> None:
        return None


__all__ = ["AuctionSettlementEffects", "NullAuctionSettlementEffects"]
