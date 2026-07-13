from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from threading import RLock
from typing import Any

from ..xiuxian_utils import db_backend


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


class AuctionSessionService:
    """Use the game database as the durable truth for an auction session."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._lock = lock or RLock()

    def _connect(self):
        conn = db_backend.connect(self._game_database)
        conn.execute("ATTACH DATABASE %s AS auction_trade", (str(self._trade_database),))
        return conn

    @staticmethod
    def _ensure_schema(conn, *, include_trade: bool = False) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_sessions (
                session_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                start_time REAL NOT NULL,
                end_time REAL NOT NULL,
                items_count INTEGER NOT NULL,
                start_operation_id TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS auction_one_active_session
            ON auction_sessions(status) WHERE status='active'
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_session_operations (
                operation_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                payload TEXT NOT NULL,
                result TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_current (
                id TEXT PRIMARY KEY, item_id INTEGER NOT NULL, name TEXT NOT NULL,
                start_price INTEGER NOT NULL, current_price INTEGER NOT NULL,
                seller_id TEXT NOT NULL, seller_name TEXT NOT NULL,
                bids TEXT DEFAULT '{}', bid_times TEXT DEFAULT '{}',
                is_system INTEGER DEFAULT 0, last_bid_time REAL DEFAULT NULL
            )
            """
        )
        if include_trade:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS auction_trade.auction_player_upload (
                    user_id TEXT NOT NULL, item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL, start_price INTEGER NOT NULL,
                    user_name TEXT NOT NULL, PRIMARY KEY (user_id, item_id)
                )
                """
            )

    @staticmethod
    def _payload(value: dict[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _read_operation(conn, operation_id: str, action: str, payload: str):
        row = conn.execute(
            "SELECT action, payload, result FROM auction_session_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        if str(row[0]) != action or str(row[1]) != payload:
            return "state_changed", None
        return "duplicate", json.loads(row[2])

    @staticmethod
    def _auction_id(session_id: str, index: int) -> str:
        digest = hashlib.sha256(f"{session_id}:{index}".encode("utf-8")).hexdigest()
        return digest[:8]

    def get_active_session(self) -> dict[str, Any] | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT session_id, start_time, end_time, items_count "
                "FROM auction_sessions WHERE status='active'"
            ).fetchone()
            if row is None:
                return None
            return {
                "session_id": str(row[0]),
                "start_time": float(row[1]),
                "end_time": float(row[2]),
                "items_count": int(row[3]),
            }

    def get_start_operation(self, operation_id: str) -> AuctionSessionStartResult | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT result FROM auction_session_operations "
                "WHERE operation_id=%s AND action='start'", (str(operation_id),)
            ).fetchone()
            if row is None:
                return None
            value = json.loads(row[0])
            return AuctionSessionStartResult(
                "duplicate", str(operation_id), str(value["session_id"]),
                float(value["start_time"]), float(value["end_time"]),
                int(value["items_count"]),
            )

    def close_active_session(self) -> None:
        """Keep start-session state compatible with the legacy item closer."""
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_schema(conn)
            conn.execute(
                "UPDATE auction_sessions SET status='closed' WHERE status='active'"
            )
            conn.commit()

    def start(
        self,
        operation_id: str,
        session_id: str,
        *,
        start_time: float,
        end_time: float,
        system_items: list[dict[str, Any]],
    ) -> AuctionSessionStartResult:
        operation_id = str(operation_id).strip()
        session_id = str(session_id).strip()
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
        payload = self._payload(
            {
                "session_id": session_id,
                "start_time": float(start_time),
                "end_time": float(end_time),
                "system_items": normalized_system,
            }
        )
        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, include_trade=True)
                previous = self._read_operation(conn, operation_id, "start", payload)
                if previous is not None:
                    conn.rollback()
                    if previous[0] == "state_changed":
                        return AuctionSessionStartResult("state_changed", operation_id)
                    value = previous[1]
                    return AuctionSessionStartResult(
                        "duplicate", operation_id, str(value["session_id"]),
                        float(value["start_time"]), float(value["end_time"]),
                        int(value["items_count"]),
                    )
                if conn.execute(
                    "SELECT 1 FROM auction_sessions WHERE status='active'"
                ).fetchone() or conn.execute("SELECT 1 FROM auction_current LIMIT 1").fetchone():
                    conn.rollback()
                    return AuctionSessionStartResult("already_active", operation_id)

                queue = conn.execute(
                    "SELECT user_id, item_id, item_name, start_price, user_name "
                    "FROM auction_trade.auction_player_upload ORDER BY user_id, item_id"
                ).fetchall()
                all_items = list(normalized_system)
                all_items.extend(
                    {
                        "item_id": int(row[1]), "name": str(row[2]),
                        "start_price": int(row[3]), "seller_id": str(row[0]),
                        "seller_name": str(row[4]),
                    }
                    for row in queue
                )
                if not all_items:
                    conn.rollback()
                    return AuctionSessionStartResult("empty", operation_id)

                for index, item in enumerate(all_items):
                    conn.execute(
                        """
                        INSERT INTO auction_current (
                            id, item_id, name, start_price, current_price,
                            seller_id, seller_name, bids, bid_times,
                            is_system, last_bid_time
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,'{}','{}',%s,%s)
                        """,
                        (
                            self._auction_id(session_id, index), item["item_id"], item["name"],
                            item["start_price"], item["start_price"],
                            item.get("seller_id", "0"), item.get("seller_name", "系统"),
                            0 if "seller_id" in item else 1, float(start_time),
                        ),
                    )
                conn.execute("DELETE FROM auction_trade.auction_player_upload")
                count = len(all_items)
                conn.execute(
                    "INSERT INTO auction_sessions VALUES (%s,'active',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (session_id, float(start_time), float(end_time), count, operation_id),
                )
                result = {
                    "session_id": session_id, "start_time": float(start_time),
                    "end_time": float(end_time), "items_count": count,
                }
                conn.execute(
                    "INSERT INTO auction_session_operations (operation_id,action,payload,result) VALUES (%s,'start',%s,%s)",
                    (operation_id, payload, self._payload(result)),
                )
                conn.commit()
                return AuctionSessionStartResult(
                    "started", operation_id, session_id, float(start_time), float(end_time), count
                )
            except Exception:
                conn.rollback()
                raise
