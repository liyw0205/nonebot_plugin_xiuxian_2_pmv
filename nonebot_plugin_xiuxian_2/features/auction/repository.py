from __future__ import annotations

from typing import Any, Protocol, Mapping


class AuctionBidRepository(Protocol):
    def place_auction_bid(
        self,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        bid_price: int,
        expected_price: int,
        expected_bids: Mapping[str, int],
        bid_time: float,
    ) -> Any: ...


class LegacyTradeRepository:
    """Temporary adapter around the historical auction repository."""

    def __init__(self, database: str) -> None:
        from ...xiuxian.xiuxian_trade.repository import TradeRepository

        self._repository = TradeRepository(database)

    def place_auction_bid(self, *args: Any, **kwargs: Any) -> Any:
        return self._repository.place_auction_bid(*args, **kwargs)


__all__ = ["AuctionBidRepository", "LegacyTradeRepository"]
