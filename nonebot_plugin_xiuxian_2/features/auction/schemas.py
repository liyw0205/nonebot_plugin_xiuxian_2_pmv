from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class AuctionBidResult:
    status: str
    operation_id: str
    auction_id: str
    bidder_id: str
    bid_price: int = 0
    debit: int = 0
    refunded_bidder: str = ""
    refunded_amount: int = 0

    @property
    def ok(self) -> bool:
        return self.status in {"bid", "duplicate", "replayed"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "auction_id": self.auction_id,
            "bidder_id": self.bidder_id,
            "bid_price": self.bid_price,
            "debit": self.debit,
            "refunded_bidder": self.refunded_bidder,
            "refunded_amount": self.refunded_amount,
        }


__all__ = ["AuctionBidResult"]
