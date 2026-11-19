"""Claim auction items that could not enter a backpack at settlement.

Settlement parks an undeliverable item in ``auction_pending_items`` instead of
destroying it (seller's unsold item) or blocking the whole session (winner's
purchase). This module is the player side of that mailbox: list what is waiting and
move one or more rows into ``back`` once the stack has room.

The claim is a single immediate transaction: capacity is re-checked per row, granted
rows are marked claimed in the same write, and a repeated operation id replays the
first result without handing out a second copy.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import require_exact_integer
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .settlement_repository import DEFAULT_MAX_GOODS_NUM, configured_max_goods_num

_ROLE_LABELS = {"winner": "中标", "seller": "流拍"}


@dataclass(frozen=True)
class AuctionPendingClaimResult:
    status: str
    claimed: tuple[dict[str, Any], ...] = ()
    blocked: tuple[dict[str, Any], ...] = ()
    remaining: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"claimed", "duplicate"} and bool(self.claimed)


@dataclass(frozen=True)
class _PendingRow:
    auction_id: str
    item_id: int
    item_name: str
    item_type: str
    role: str
    created_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "auction_id": self.auction_id,
            "item_id": self.item_id,
            "item_name": self.item_name,
            "item_type": self.item_type,
            "role": self.role,
            "role_label": _ROLE_LABELS.get(self.role, self.role),
            "created_at": self.created_at,
        }


class _ClaimRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.result = AuctionPendingClaimResult("state_changed", reason=reason, subject=subject)


class _ClaimWrites:
    """Recheck cumulative inventory and claim marks after the receipt write."""

    def __init__(self, uow: DatabaseUnitOfWork) -> None:
        self.uow = uow
        self.rows: dict[tuple, tuple[dict, str, str]] = {}

    @staticmethod
    def affected(cursor: Any, reason: str, subject: str) -> None:
        if cursor.rowcount != 1:
            raise _ClaimRejected(reason, subject)

    def before(self, sql: str, params: tuple) -> dict | None:
        previous = self.rows.get((sql, params))
        return dict(previous[0]) if previous else self.uow.query_one(sql, params)

    def expect(self, sql: str, params: tuple, expected: dict, reason: str, subject: str) -> None:
        self.rows[(sql, params)] = (expected, reason, subject)
        self._check(sql, params, expected, reason, subject)

    def _check(self, sql: str, params: tuple, expected: dict, reason: str, subject: str) -> None:
        if self.uow.query_one(sql, params) != expected:
            raise _ClaimRejected(reason, subject)

    def verify(self) -> None:
        for (sql, params), (expected, reason, subject) in self.rows.items():
            self._check(sql, params, expected, reason, subject)


class AuctionPendingApplication:
    """Read and claim the player's undelivered auction items."""

    action = "auction.pending.claim"

    def __init__(
        self,
        database: str | Path,
        *,
        max_goods_num: int | None = None,
        clock: Any | None = None,
    ) -> None:
        self.database = str(database)
        self.max_goods_num = max(
            int(max_goods_num)
            if max_goods_num is not None
            else configured_max_goods_num(),
            1,
        )
        self.clock = clock or SystemClock()

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    def _pending_rows(
        self, uow: DatabaseUnitOfWork, user_id: str, auction_id: str | None
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT * "
            "FROM auction_pending_items WHERE owner_id=? AND claimed_at IS NULL"
        )
        params: list[Any] = [str(user_id)]
        if auction_id:
            sql += " AND auction_id=?"
            params.append(str(auction_id))
        sql += " ORDER BY id ASC"
        return [dict(row) for row in uow.query_all(sql, params) or []]

    def list_for(self, user_id: str) -> list[dict[str, Any]]:
        """Return the player's unclaimed auction items (read-only, no DDL)."""

        database = Path(self.database)
        if not database.is_file():
            return []
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            if not self._table_exists(uow, "auction_pending_items"):
                return []
            rows = self._pending_rows(uow, str(user_id), None)
        return [_PendingRow(
            auction_id=str(row["auction_id"]),
            item_id=int(row["item_id"]),
            item_name=str(row["item_name"]),
            item_type=str(row["item_type"]),
            role=str(row["role"]),
            created_at=str(row.get("created_at") or ""),
        ).as_dict() for row in rows]

    def _claim(
        self,
        uow: DatabaseUnitOfWork,
        user_id: str,
        auction_id: str | None,
        operation_id: str,
        writes: _ClaimWrites,
    ) -> AuctionPendingClaimResult:
        rows = self._pending_rows(uow, user_id, auction_id)
        claimed: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for row in rows:
            item_id = int(row["item_id"])
            item_name = str(row["item_name"])
            item_type = str(row["item_type"])
            inventory_sql = "SELECT goods_num,bind_num,goods_name,goods_type FROM back WHERE user_id=? AND goods_id=?"
            held = writes.before(inventory_sql, (user_id, item_id)) or {}
            subject = f"{user_id}:{item_id}"
            current = require_exact_integer(held.get("goods_num") or 0, field="goods_num", subject=subject)
            bound = require_exact_integer(held.get("bind_num") or 0, field="bind_num", subject=subject)
            if current < 0 or bound < 0 or bound > current:
                raise _ClaimRejected("auction_claim_inventory_invalid", subject)
            record = _PendingRow(
                auction_id=str(row["auction_id"]),
                item_id=item_id,
                item_name=item_name,
                item_type=item_type,
                role=str(row["role"]),
                created_at=str(row.get("created_at") or ""),
            ).as_dict()
            if current + 1 > self.max_goods_num:
                writes.expect(inventory_sql, (user_id, item_id), held,
                              "auction_claim_inventory_write_rejected", subject)
                writes.expect("SELECT * FROM auction_pending_items WHERE id=?", (row["id"],), row,
                              "auction_claim_pending_write_rejected", str(row["auction_id"]))
                blocked.append(record)
                continue
            cursor = uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,1,CURRENT_TIMESTAMP,"
                "CURRENT_TIMESTAMP,1) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                "goods_num=COALESCE(back.goods_num,0)+1,"
                "bind_num=COALESCE(back.bind_num,0)+1,update_time=CURRENT_TIMESTAMP",
                (user_id, item_id, item_name, item_type),
            )
            reason = "auction_claim_inventory_write_rejected"
            writes.affected(cursor, reason, subject)
            writes.expect(inventory_sql, (user_id, item_id),
                          {"goods_num": current + 1, "bind_num": bound + 1,
                           "goods_name": item_name, "goods_type": item_type}, reason, subject)
            claimed_at = self.clock.now().isoformat()
            cursor = uow.execute(
                "UPDATE auction_pending_items SET claimed_at=?,claim_operation_id=? "
                "WHERE id=? AND owner_id=? AND claimed_at IS NULL",
                (claimed_at, operation_id, row["id"], user_id),
            )
            reason = "auction_claim_pending_write_rejected"
            writes.affected(cursor, reason, str(row["auction_id"]))
            writes.expect("SELECT * FROM auction_pending_items WHERE id=?", (row["id"],),
                          dict(row, claimed_at=claimed_at, claim_operation_id=operation_id),
                          reason, str(row["auction_id"]))
            claimed.append(record)
        remaining = len(self._pending_rows(uow, user_id, None))
        if claimed:
            status = "claimed"
        elif blocked:
            status = "blocked"
        else:
            status = "empty"
        return AuctionPendingClaimResult(
            status, tuple(claimed), tuple(blocked), remaining
        )

    def claim(
        self,
        *,
        operation_id: str,
        user_id: str,
        auction_id: str | None = None,
    ) -> AuctionPendingClaimResult:
        """Return known refusals only after the complete claim has rolled back."""
        try:
            return self._claim_operation(operation_id=operation_id, user_id=user_id, auction_id=auction_id)
        except _ClaimRejected as rejected:
            return rejected.result
        except DomainError as rejected:
            if not rejected.details.get("reason"):
                raise
            return AuctionPendingClaimResult("state_changed", reason=rejected.details["reason"],
                                             subject=str(rejected.details.get("subject") or user_id))

    def _claim_operation(
        self,
        *,
        operation_id: str,
        user_id: str,
        auction_id: str | None = None,
    ) -> AuctionPendingClaimResult:
        """Move claimable pending items into the player's backpack."""

        operation_id = str(operation_id or "").strip()
        user_id = str(user_id or "").strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not user_id:
            raise ValueError("user_id must not be empty")
        scoped_auction = str(auction_id).strip() if auction_id else None
        payload = json.dumps([user_id, scoped_auction or ""], ensure_ascii=False)

        database = Path(self.database)
        if not database.is_file():
            return AuctionPendingClaimResult("schema_missing", reason="schema_missing", subject="auction_pending_items")
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            for table in ("auction_pending_items", "auction_pending_claim_operations"):
                if not self._table_exists(uow, table):
                    return AuctionPendingClaimResult("schema_missing", reason="schema_missing", subject=table)
            previous = uow.query_one(
                "SELECT payload,result FROM auction_pending_claim_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AuctionPendingClaimResult("operation_conflict", reason="operation_conflict", subject=operation_id)
                stored = json.loads(str(previous["result"]))
                return AuctionPendingClaimResult(
                    "duplicate",
                    tuple(dict(item) for item in stored.get("claimed", ())),
                    tuple(dict(item) for item in stored.get("blocked", ())),
                    int(stored.get("remaining", 0)),
                )
            if not self._table_exists(uow, "back"):
                return AuctionPendingClaimResult("schema_missing", reason="schema_missing", subject="back")
            writes = _ClaimWrites(uow)
            result = self._claim(uow, user_id, scoped_auction, operation_id, writes)
            if result.status == "empty":
                return result
            result_json = json.dumps(
                {"claimed": list(result.claimed), "blocked": list(result.blocked), "remaining": result.remaining},
                ensure_ascii=False,
            )
            cursor = uow.execute(
                "INSERT INTO auction_pending_claim_operations(operation_id,payload,user_id,"
                "claimed_count,blocked_count,result) VALUES(?,?,?,?,?,?)",
                (
                    operation_id,
                    payload,
                    user_id,
                    len(result.claimed),
                    len(result.blocked),
                    result_json,
                ),
            )
            reason = "auction_claim_receipt_write_rejected"
            writes.affected(cursor, reason, operation_id)
            writes.expect("SELECT operation_id,payload,user_id,claimed_count,blocked_count,result "
                          "FROM auction_pending_claim_operations WHERE operation_id=?", (operation_id,),
                          {"operation_id": operation_id, "payload": payload, "user_id": user_id,
                           "claimed_count": len(result.claimed), "blocked_count": len(result.blocked),
                           "result": result_json}, reason, operation_id)
            writes.verify()
            return result


__all__ = [
    "AuctionPendingApplication",
    "AuctionPendingClaimResult",
    "DEFAULT_MAX_GOODS_NUM",
]
