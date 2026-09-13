from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class AuctionBidRequest:
    operation_id: str
    auction_id: str
    bidder_id: str
    bid_price: int
    expected_price: int
    expected_bids: Mapping[str, int]
    bid_time: float

    def validate(self) -> None:
        if not self.operation_id or not self.auction_id or not self.bidder_id:
            raise ValueError("operation_id, auction_id and bidder_id are required")
        if self.bid_price <= 0 or self.expected_price < 0:
            raise ValueError("bid prices must be non-negative")
        if any(int(value) < 0 for value in self.expected_bids.values()):
            raise ValueError("expected bids must be non-negative")

    def payload(self) -> dict[str, Any]:
        return {
            "auction_id": self.auction_id,
            "bidder_id": self.bidder_id,
            "bid_price": self.bid_price,
            "expected_price": self.expected_price,
            "expected_bids": dict(self.expected_bids),
            "bid_time": self.bid_time,
        }


__all__ = ["AuctionBidRequest"]
