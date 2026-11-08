from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


def session_end_datetime(end_time: Any, now: datetime) -> datetime:
    """Session epoch seconds → datetime expressed in ``now``'s timezone frame.

    ``features.auction`` stores ``end_time`` as a UTC epoch float
    (``clock.now().timestamp()``).  Comparing it against an aware
    ``runtime_clock.now()`` requires the *same* frame: a naive
    ``datetime.fromtimestamp(...)`` (process-local) raises
    ``TypeError: can't compare offset-naive and offset-aware datetimes`` in
    every end-check job.  Naive ``now`` values keep local-time semantics so
    display-only callers are unaffected.
    """
    moment = datetime.fromtimestamp(float(end_time), tz=now.tzinfo)
    return moment if now.tzinfo is not None else moment.replace(tzinfo=None)


def session_window(session: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    """Compare a session's end time with ``now`` on one clock frame.

    Returns ``end_time`` (same frame as ``now``), ``expired`` and
    ``remaining_seconds`` (never negative).
    """
    end_time = session_end_datetime(session["end_time"], now)
    remaining = int((end_time - now).total_seconds())
    return {
        "end_time": end_time,
        "expired": remaining <= 0,
        "remaining_seconds": max(remaining, 0),
    }


__all__ = ["AuctionBidRequest", "session_end_datetime", "session_window"]


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
        }
