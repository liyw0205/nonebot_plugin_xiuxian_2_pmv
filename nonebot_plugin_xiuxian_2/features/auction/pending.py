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

    @property
    def succeeded(self) -> bool:
        return self.status in {"claimed", "duplicate"}


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
            "SELECT auction_id,item_id,item_name,item_type,role,created_at "
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
    ) -> AuctionPendingClaimResult:
        rows = self._pending_rows(uow, user_id, auction_id)
        claimed: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for row in rows:
            item_id = int(row["item_id"])
            item_name = str(row["item_name"])
            item_type = str(row["item_type"])
            held = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                "WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            current = int(held["goods_num"]) if held else 0
            record = _PendingRow(
                auction_id=str(row["auction_id"]),
                item_id=item_id,
                item_name=item_name,
                item_type=item_type,
                role=str(row["role"]),
                created_at=str(row.get("created_at") or ""),
            ).as_dict()
            if current + 1 > self.max_goods_num:
                blocked.append(record)
                continue
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,1,CURRENT_TIMESTAMP,"
                "CURRENT_TIMESTAMP,1) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                "goods_num=COALESCE(back.goods_num,0)+1,"
                "bind_num=COALESCE(back.bind_num,0)+1,update_time=CURRENT_TIMESTAMP",
                (user_id, item_id, item_name, item_type),
            )
            uow.execute(
                "UPDATE auction_pending_items SET claimed_at=CURRENT_TIMESTAMP,"
                "claim_operation_id=? WHERE auction_id=? AND role=? AND claimed_at IS NULL",
                (operation_id, str(row["auction_id"]), str(row["role"])),
            )
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
            return AuctionPendingClaimResult("schema_missing")
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            if not self._table_exists(uow, "auction_pending_items"):
                return AuctionPendingClaimResult("schema_missing")
            previous = uow.query_one(
                "SELECT payload,result FROM auction_pending_claim_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AuctionPendingClaimResult("operation_conflict")
                stored = json.loads(str(previous["result"]))
                return AuctionPendingClaimResult(
                    "duplicate",
                    tuple(dict(item) for item in stored.get("claimed", ())),
                    tuple(dict(item) for item in stored.get("blocked", ())),
                    int(stored.get("remaining", 0)),
                )
            result = self._claim(uow, user_id, scoped_auction, operation_id)
            if result.status == "empty":
                return result
            uow.execute(
                "INSERT INTO auction_pending_claim_operations(operation_id,payload,user_id,"
                "claimed_count,blocked_count,result) VALUES(?,?,?,?,?,?)",
                (
                    operation_id,
                    payload,
                    user_id,
                    len(result.claimed),
                    len(result.blocked),
                    json.dumps(
                        {
                            "claimed": list(result.claimed),
                            "blocked": list(result.blocked),
                            "remaining": result.remaining,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            return result


__all__ = [
    "AuctionPendingApplication",
    "AuctionPendingClaimResult",
    "DEFAULT_MAX_GOODS_NUM",
]
