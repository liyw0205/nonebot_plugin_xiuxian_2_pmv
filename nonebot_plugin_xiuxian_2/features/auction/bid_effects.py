from __future__ import annotations

from typing import Any, Callable, Protocol


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
    ) -> str | None:
        return None


class LegacyAuctionBidEffects:
    """Compatibility adapter for the historical log/statistics projection.

    The asset transaction remains feature-owned.  This adapter is deliberately
    injected by the legacy trade composition root and is not imported by the
    auction application itself.
    """

    def __init__(self, record_trade_event: Callable[..., Any]) -> None:
        self._record_trade_event = record_trade_event

    def on_bid(
        self,
        *,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        item_name: str,
        bid_price: int,
        replayed: bool,
    ) -> str | None:
        if replayed:
            return None
        self._record_trade_event(
            str(bidder_id),
            "拍卖竞拍",
            f"竞拍{item_name}，出价{int(bid_price)}灵石，拍卖ID:{auction_id}",
            {"拍卖出价次数": 1, "拍卖出价灵石": int(bid_price)},
        )
        return None


__all__ = ["AuctionBidEffects", "LegacyAuctionBidEffects", "NullAuctionBidEffects"]
