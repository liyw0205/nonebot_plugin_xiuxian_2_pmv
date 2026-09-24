from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AuctionBidWriteResult:
    status: str
    operation_id: str
    auction_id: str
    bidder_id: str
    bid_price: int = 0
    debit: int = 0
    refunded_bidder: str = ""
    refunded_amount: int = 0


class AuctionBidSqlRepository:
    """Atomically lock bidder funds and replace the active auction bid."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _payload(
        auction_id: str,
        bidder_id: str,
        bid_price: int,
        expected_price: int,
        expected_bids: Mapping[str, int],
    ) -> str:
        return json.dumps(
            [auction_id, bidder_id, bid_price, expected_price, dict(expected_bids)],
            sort_keys=True,
        )

    def place_auction_bid(
        self,
        operation_id: str,
        auction_id: str,
        bidder_id: str,
        bid_price: int,
        expected_price: int,
        expected_bids: Mapping[str, int],
        bid_time: float,
    ) -> AuctionBidWriteResult:
        operation_id, auction_id, bidder_id = map(
            str, (operation_id, auction_id, bidder_id)
        )
        bid_price, expected_price = int(bid_price), int(expected_price)
        expected_bids = {str(key): int(value) for key, value in expected_bids.items()}
        payload = self._payload(
            auction_id, bidder_id, bid_price, expected_price, expected_bids
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one(
                "SELECT payload,bid_price,debit,refunded_bidder,refunded_amount "
                "FROM auction_bid_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                return AuctionBidWriteResult(
                    "duplicate" if str(old["payload"]) == payload else "state_changed",
                    operation_id,
                    auction_id,
                    bidder_id,
                    int(old["bid_price"] or 0),
                    int(old["debit"] or 0),
                    str(old["refunded_bidder"] or ""),
                    int(old["refunded_amount"] or 0),
                )

            row = uow.query_one(
                "SELECT current_price,bids,seller_id FROM auction_current WHERE id=?",
                (auction_id,),
            )
            if row is None:
                return AuctionBidWriteResult("auction_missing", operation_id, auction_id, bidder_id)
            current_bids = {
                str(key): int(value)
                for key, value in json.loads(str(row["bids"] or "{}")).items()
            }
            if int(row["current_price"]) != expected_price or current_bids != expected_bids:
                return AuctionBidWriteResult("state_changed", operation_id, auction_id, bidder_id)
            if str(row["seller_id"]) == bidder_id:
                return AuctionBidWriteResult("self_bid", operation_id, auction_id, bidder_id)

            previous_bidder, previous_amount = ("", 0)
            if current_bids:
                previous_bidder, previous_amount = max(
                    current_bids.items(), key=lambda item: item[1]
                )
            debit = bid_price - current_bids.get(bidder_id, 0)
            if debit <= 0:
                return AuctionBidWriteResult("bid_too_low", operation_id, auction_id, bidder_id)
            if uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL) "
                "WHERE user_id=? AND stone>=?",
                (debit, bidder_id, debit),
            ).rowcount != 1:
                return AuctionBidWriteResult("stone_insufficient", operation_id, auction_id, bidder_id)

            refunded_bidder, refunded_amount = ("", 0)
            if previous_bidder and previous_bidder != bidder_id:
                if uow.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) "
                    "WHERE user_id=?",
                    (previous_amount, previous_bidder),
                ).rowcount != 1:
                    return AuctionBidWriteResult("participant_missing", operation_id, auction_id, bidder_id)
                refunded_bidder, refunded_amount = previous_bidder, previous_amount

            new_bids = {bidder_id: bid_price}
            uow.execute(
                "UPDATE auction_current SET current_price=?,bids=?,bid_times=?,last_bid_time=? "
                "WHERE id=?",
                (
                    bid_price,
                    json.dumps(new_bids),
                    json.dumps({bidder_id: float(bid_time)}),
                    float(bid_time),
                    auction_id,
                ),
            )
            uow.execute(
                "INSERT INTO auction_bid_operations "
                "(operation_id,payload,auction_id,bidder_id,bid_price,debit,refunded_bidder,refunded_amount) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    payload,
                    auction_id,
                    bidder_id,
                    bid_price,
                    debit,
                    refunded_bidder,
                    refunded_amount,
                ),
            )
            return AuctionBidWriteResult(
                "bid", operation_id, auction_id, bidder_id, bid_price, debit,
                refunded_bidder, refunded_amount,
            )


__all__ = ["AuctionBidSqlRepository", "AuctionBidWriteResult"]
