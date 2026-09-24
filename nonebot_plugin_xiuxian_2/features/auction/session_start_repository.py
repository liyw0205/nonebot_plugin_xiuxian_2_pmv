from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AuctionSessionStartResult:
    status: str
    operation_id: str
    session_id: str = ""
    start_time: float = 0.0
    end_time: float = 0.0
    items_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"started", "duplicate"}


class AuctionSessionStartSqlRepository:
    """Create a session and move every queued player item in one UoW."""

    def __init__(self, game_database: str | Path, trade_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)

    @staticmethod
    def _payload(value: Mapping[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _auction_id(session_id: str, index: int) -> str:
        return hashlib.sha256(f"{session_id}:{index}".encode("utf-8")).hexdigest()[:8]

    @staticmethod
    def _result(status: str, operation_id: str, value: Mapping[str, Any] | None = None):
        value = value or {}
        return AuctionSessionStartResult(
            status,
            operation_id,
            str(value.get("session_id", "")),
            float(value.get("start_time", 0.0)),
            float(value.get("end_time", 0.0)),
            int(value.get("items_count", 0)),
        )

    def get_active_session(self) -> dict[str, Any] | None:
        if not Path(self.game_database).is_file():
            return None
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            exists = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                ("auction_sessions",),
            )
            if exists is None:
                return None
            row = uow.query_one(
                "SELECT session_id,start_time,end_time,items_count "
                "FROM auction_sessions WHERE status='active'"
            )
        if row is None:
            return None
        return {
            "session_id": str(row["session_id"]),
            "start_time": float(row["start_time"]),
            "end_time": float(row["end_time"]),
            "items_count": int(row["items_count"]),
        }

    def get_start_operation(self, operation_id: str) -> AuctionSessionStartResult | None:
        if not Path(self.game_database).is_file():
            return None
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            exists = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                ("auction_session_operations",),
            )
            if exists is None:
                return None
            row = uow.query_one(
                "SELECT result FROM auction_session_operations "
                "WHERE operation_id=? AND action='start'",
                (str(operation_id),),
            )
        return None if row is None else self._result(
            "duplicate", str(operation_id), json.loads(str(row["result"]))
        )

    def start(
        self,
        operation_id: str,
        session_id: str,
        *,
        start_time: float,
        end_time: float,
        system_items: list[dict[str, Any]],
    ) -> AuctionSessionStartResult:
        operation_id, session_id = str(operation_id).strip(), str(session_id).strip()
        if not operation_id or not session_id:
            raise ValueError("operation_id and session_id must not be empty")
        normalized_system = [
            {
                "item_id": int(item["item_id"]),
                "name": str(item["name"]),
                "start_price": int(item["start_price"]),
            }
            for item in system_items
        ]
        start_time, end_time = float(start_time), float(end_time)
        payload = self._payload(
            {
                "session_id": session_id,
                "start_time": start_time,
                "end_time": end_time,
                "system_items": normalized_system,
            }
        )

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "auction_trade")
            previous = uow.query_one(
                "SELECT action,payload,result FROM auction_session_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["action"]) != "start" or str(previous["payload"]) != payload:
                    return self._result("state_changed", operation_id)
                return self._result("duplicate", operation_id, json.loads(previous["result"]))
            if uow.query_one(
                "SELECT 1 AS present FROM auction_sessions WHERE status='active'"
            ) or uow.query_one("SELECT 1 AS present FROM auction_current LIMIT 1"):
                return self._result("already_active", operation_id)

            queue = uow.query_all(
                "SELECT user_id,item_id,item_name,start_price,user_name "
                "FROM auction_trade.auction_player_upload ORDER BY user_id,item_id"
            )
            all_items = list(normalized_system)
            all_items.extend(
                {
                    "item_id": int(row["item_id"]),
                    "name": str(row["item_name"]),
                    "start_price": int(row["start_price"]),
                    "seller_id": str(row["user_id"]),
                    "seller_name": str(row["user_name"]),
                }
                for row in queue
            )
            if not all_items:
                return self._result("empty", operation_id)

            for index, item in enumerate(all_items):
                uow.execute(
                    "INSERT INTO auction_current(id,item_id,name,start_price,current_price,"
                    "seller_id,seller_name,bids,bid_times,is_system,last_bid_time) "
                    "VALUES(?,?,?,?,?,?,?,'{}','{}',?,?)",
                    (
                        self._auction_id(session_id, index),
                        item["item_id"],
                        item["name"],
                        item["start_price"],
                        item["start_price"],
                        item.get("seller_id", "0"),
                        item.get("seller_name", "系统"),
                        0 if "seller_id" in item else 1,
                        start_time,
                    ),
                )
            uow.execute("DELETE FROM auction_trade.auction_player_upload")
            result = {
                "session_id": session_id,
                "start_time": start_time,
                "end_time": end_time,
                "items_count": len(all_items),
            }
            uow.execute(
                "INSERT INTO auction_sessions(session_id,status,start_time,end_time,items_count,"
                "start_operation_id,created_at) VALUES(?,'active',?,?,?,?,CURRENT_TIMESTAMP)",
                (session_id, start_time, end_time, len(all_items), operation_id),
            )
            uow.execute(
                "INSERT INTO auction_session_operations(operation_id,action,payload,result) "
                "VALUES(?,'start',?,?)",
                (operation_id, payload, self._payload(result)),
            )
            return self._result("started", operation_id, result)


__all__ = ["AuctionSessionStartResult", "AuctionSessionStartSqlRepository"]
