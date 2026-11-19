from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import DomainError
from ...core.numeric import exact_crystal_integer, require_exact_integer
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
    reason: str = ""
    subject: str = ""


BID_WRITE_REJECTIONS = frozenset(
    f"auction_bid_{target}_write_rejected" for target in ("debit", "refund", "item", "receipt")
)


class _BidRejected(Exception):
    def __init__(self, reason: str, subject: str, status: str = "state_changed") -> None:
        self.reason, self.subject, self.status = reason, subject, status


class _BidWrites:
    def __init__(self, uow: DatabaseUnitOfWork) -> None:
        self.uow = uow
        self.expected: list[tuple[str, tuple, dict, str, str]] = []

    @staticmethod
    def affected(cursor: Any, target: str, subject: str) -> None:
        if cursor.rowcount != 1:
            raise _BidRejected(f"auction_bid_{target}_write_rejected", subject)

    def expect(self, sql: str, params: tuple, row: dict, target: str, subject: str) -> None:
        self.expected.append((sql, params, row, target, subject))
        self._check(sql, params, row, target, subject)

    def _check(self, sql: str, params: tuple, row: dict, target: str, subject: str) -> None:
        actual = self.uow.query_one(sql, params)
        if actual is None or any(
            (exact_crystal_integer(actual[key]) != value if isinstance(value, int) else actual[key] != value)
            for key, value in row.items()
        ):
            raise _BidRejected(f"auction_bid_{target}_write_rejected", subject)

    def verify(self) -> None:
        for expected in self.expected:
            self._check(*expected)


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
        self, operation_id: str, auction_id: str, bidder_id: str, bid_price: int,
        expected_price: int, expected_bids: Mapping[str, int], bid_time: float,
    ) -> AuctionBidWriteResult:
        try:
            return self._place_auction_bid(operation_id, auction_id, bidder_id, bid_price,
                                           expected_price, expected_bids, bid_time)
        except _BidRejected as exc:
            return AuctionBidWriteResult(exc.status, str(operation_id), str(auction_id), str(bidder_id),
                                         reason=exc.reason, subject=exc.subject)
        except DomainError as exc:
            if not exc.details.get("reason"):
                raise
            return AuctionBidWriteResult("state_changed", str(operation_id), str(auction_id), str(bidder_id),
                                         reason=exc.details["reason"], subject=str(exc.details.get("subject") or bidder_id))

    @staticmethod
    def _balance(uow: DatabaseUnitOfWork, user_id: str, status: str) -> int:
        row = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
        if row is None:
            raise _BidRejected(status, user_id, status)
        amount = require_exact_integer(row["stone"] if row["stone"] is not None else 0,
                                       field="stone", subject=user_id)
        if amount < 0:
            raise _BidRejected("auction_bid_balance_invalid", user_id)
        return amount

    def _place_auction_bid(
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
                same_request = str(old["payload"]) == payload
                return AuctionBidWriteResult(
                    "duplicate" if same_request else "state_changed",
                    operation_id,
                    auction_id,
                    bidder_id,
                    int(old["bid_price"] or 0),
                    int(old["debit"] or 0),
                    str(old["refunded_bidder"] or ""),
                    int(old["refunded_amount"] or 0),
                    reason="" if same_request else "operation_conflict",
                    subject="" if same_request else operation_id,
                )

            row = uow.query_one(
                "SELECT * FROM auction_current WHERE id=?",
                (auction_id,),
            )
            if row is None:
                raise _BidRejected("auction_missing", auction_id, "auction_missing")
            session = uow.query_one(
                "SELECT session_id,start_time,end_time FROM auction_sessions WHERE status='active'"
            )
            reason = ""
            if session is None:
                reason = "auction_session_missing"
            else:
                try:
                    start, end, moment = float(session["start_time"]), float(session["end_time"]), float(bid_time)
                except (TypeError, ValueError, OverflowError):
                    reason = "auction_session_time_invalid"
                else:
                    if not all(math.isfinite(value) for value in (start, end, moment)) or end <= start:
                        reason = "auction_session_time_invalid"
                    elif moment < start:
                        reason = "auction_not_started"
                    elif moment >= end:
                        reason = "auction_expired"
            if reason:
                return AuctionBidWriteResult(
                    reason, operation_id, auction_id, bidder_id, reason=reason,
                    subject=str(session["session_id"]) if session else auction_id,
                )
            current_bids = {
                str(key): int(value)
                for key, value in json.loads(str(row["bids"] or "{}")).items()
            }
            if int(row["current_price"]) != expected_price or current_bids != expected_bids:
                raise _BidRejected("auction_bid_snapshot_changed", auction_id)
            if str(row["seller_id"]) == bidder_id:
                raise _BidRejected("self_bid", bidder_id, "self_bid")

            previous_bidder, previous_amount = ("", 0)
            if current_bids:
                previous_bidder, previous_amount = max(
                    current_bids.items(), key=lambda item: item[1]
                )
            debit = bid_price - current_bids.get(bidder_id, 0)
            if debit <= 0:
                raise _BidRejected("bid_too_low", bidder_id, "bid_too_low")
            balance = self._balance(uow, bidder_id, "participant_missing")
            if balance < debit:
                raise _BidRejected("stone_insufficient", bidder_id, "stone_insufficient")
            previous_balance = None
            if previous_bidder and previous_bidder != bidder_id:
                previous_balance = self._balance(uow, previous_bidder, "participant_missing")
            writes = _BidWrites(uow)
            cursor = uow.execute(
                "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_sub(COALESCE(stone,0),?,'stone','actor')) "
                "WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')>=0",
                (debit, bidder_id, debit),
            )
            writes.affected(cursor, "debit", bidder_id)
            writes.expect("SELECT stone FROM user_xiuxian WHERE user_id=?", (bidder_id,),
                          {"stone": balance - debit}, "debit", bidder_id)

            refunded_bidder, refunded_amount = ("", 0)
            if previous_bidder and previous_bidder != bidder_id:
                cursor = uow.execute(
                    "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')) "
                    "WHERE user_id=?",
                    (previous_amount, previous_bidder),
                )
                writes.affected(cursor, "refund", previous_bidder)
                writes.expect("SELECT stone FROM user_xiuxian WHERE user_id=?", (previous_bidder,),
                              {"stone": previous_balance + previous_amount}, "refund", previous_bidder)
                refunded_bidder, refunded_amount = previous_bidder, previous_amount

            new_bids = {bidder_id: bid_price}
            cursor = uow.execute(
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
            writes.affected(cursor, "item", auction_id)
            writes.expect("SELECT * FROM auction_current WHERE id=?", (auction_id,),
                          dict(row, current_price=bid_price, bids=json.dumps(new_bids),
                               bid_times=json.dumps({bidder_id: float(bid_time)}), last_bid_time=float(bid_time)),
                          "item", auction_id)
            cursor = uow.execute(
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
            writes.affected(cursor, "receipt", operation_id)
            writes.expect("SELECT operation_id,payload,auction_id,bidder_id,bid_price,debit,refunded_bidder,refunded_amount "
                          "FROM auction_bid_operations WHERE operation_id=?", (operation_id,),
                          dict(operation_id=operation_id, payload=payload, auction_id=auction_id, bidder_id=bidder_id,
                               bid_price=bid_price, debit=debit, refunded_bidder=refunded_bidder, refunded_amount=refunded_amount),
                          "receipt", operation_id)
            writes.verify()
            return AuctionBidWriteResult(
                "bid", operation_id, auction_id, bidder_id, bid_price, debit,
                refunded_bidder, refunded_amount,
            )


__all__ = ["AuctionBidSqlRepository", "AuctionBidWriteResult"]
