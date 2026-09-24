from __future__ import annotations

from typing import Any, Protocol


class AuctionBidEffects(Protocol):
    """Post-commit effects for a successful auction bid."""

    def on_bid(
        self,
        *,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        item_name: str,
        bid_price: int,
        replayed: bool,
        occurred_at: str,
    ) -> str | None: ...


class NullAuctionBidEffects:
    """No-op default for isolated application and Web use."""

    def on_bid(
        self,
        *,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        item_name: str,
        bid_price: int,
        replayed: bool,
        occurred_at: str,
    ) -> str | None:
        return None


__all__ = ["AuctionBidEffects", "NullAuctionBidEffects"]
