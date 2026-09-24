from __future__ import annotations

from typing import Any, Protocol, Mapping

from .bid_repository import AuctionBidSqlRepository


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
    """Rollback adapter backed by the feature-owned bid repository.

    The historical name is retained for callers that explicitly inject the
    compatibility repository.  Even that path must not re-enter the legacy
    trade repository, otherwise a missing application binding silently brings
    the old auction transaction graph back into production.
    """

    def __init__(self, database: str) -> None:
        self._repository = AuctionBidSqlRepository(database)

    def place_auction_bid(self, *args: Any, **kwargs: Any) -> Any:
        return self._repository.place_auction_bid(*args, **kwargs)


__all__ = ["AuctionBidRepository", "LegacyTradeRepository"]
