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


@dataclass(frozen=True)
class AuctionSessionFinishResult:
    status: str
    operation_id: str
    session_id: str = ""
    results: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


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
        self._max_goods_num = max(int(max_goods_num or 1), 1)
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
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(auction_sessions)").fetchall()
        }
        if "finish_operation_id" not in columns:
            conn.execute("ALTER TABLE auction_sessions ADD COLUMN finish_operation_id TEXT")
        if "settled_at" not in columns:
            conn.execute("ALTER TABLE auction_sessions ADD COLUMN settled_at REAL")
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, auction_id TEXT NOT NULL,
                item_id INTEGER NOT NULL, item_name TEXT NOT NULL,
                start_price INTEGER NOT NULL, final_price INTEGER,
                seller_id TEXT NOT NULL, seller_name TEXT NOT NULL,
                winner_id TEXT, winner_name TEXT, status TEXT NOT NULL,
                fee INTEGER, seller_earnings INTEGER,
                start_time REAL NOT NULL, end_time REAL NOT NULL
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
                    "INSERT INTO auction_sessions (session_id,status,start_time,end_time,items_count,"
                    "start_operation_id,created_at) VALUES (%s,'active',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
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

    def finish(
        self,
        operation_id: str,
        session_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: dict[int, str],
    ) -> AuctionSessionFinishResult:
        operation_id = str(operation_id).strip()
        session_id = str(session_id).strip()
        normalized_types = {str(int(key)): str(value) for key, value in item_types.items()}
        payload = self._payload(
            {"session_id": session_id, "fee_rate": float(fee_rate),
             "item_types": normalized_types}
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._read_operation(conn, operation_id, "finish", payload)
                if previous is not None:
                    conn.rollback()
                    if previous[0] == "state_changed":
                        return AuctionSessionFinishResult("state_changed", operation_id, session_id)
                    return AuctionSessionFinishResult(
                        "duplicate", operation_id, session_id, tuple(previous[1]["results"])
                    )
                session = conn.execute(
                    "SELECT start_time FROM auction_sessions WHERE session_id=%s AND status='active'",
                    (session_id,),
                ).fetchone()
                if session is None:
                    conn.rollback()
                    return AuctionSessionFinishResult("not_active", operation_id, session_id)
                rows = conn.execute(
                    "SELECT id,item_id,name,start_price,seller_id,seller_name,bids,is_system,last_bid_time "
                    "FROM auction_current ORDER BY id"
                ).fetchall()
                results: list[dict[str, Any]] = []
                for row in rows:
                    auction_id, item_id, name = str(row[0]), int(row[1]), str(row[2])
                    seller_id, seller_name = str(row[4]), str(row[5])
                    is_system = bool(row[7])
                    bids_value = json.loads(row[6] or "{}")
                    bids = {str(key): int(value) for key, value in bids_value.items()}
                    winner_id = final_price = winner_name = None
                    fee = earnings = 0
                    status = "流拍"
                    item_type = normalized_types.get(str(item_id))
                    if (bids or not is_system) and not item_type:
                        conn.rollback()
                        return AuctionSessionFinishResult("item_missing", operation_id, session_id)
                    if bids:
                        winner_id, final_price = max(bids.items(), key=lambda entry: entry[1])
                        winner = conn.execute(
                            "SELECT user_name FROM user_xiuxian WHERE user_id=%s", (winner_id,)
                        ).fetchone()
                        seller = True if is_system else conn.execute(
                            "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (seller_id,)
                        ).fetchone()
                        if winner is None or not seller:
                            conn.rollback()
                            return AuctionSessionFinishResult("participant_missing", operation_id, session_id)
                        winner_name = str(winner[0] or winner_id)
                        if self._inventory_full(conn, winner_id, item_id):
                            conn.rollback()
                            return AuctionSessionFinishResult("inventory_full", operation_id, session_id)
                        self._grant_item(conn, winner_id, item_id, name, item_type)
                        fee = 0 if is_system else int(final_price * float(fee_rate))
                        earnings = 0 if is_system else final_price - fee
                        if not is_system:
                            conn.execute(
                                "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                                (earnings, seller_id),
                            )
                        for bidder_id, locked in bids.items():
                            if bidder_id != winner_id and conn.execute(
                                "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                                (locked, bidder_id),
                            ).rowcount != 1:
                                conn.rollback()
                                return AuctionSessionFinishResult("participant_missing", operation_id, session_id)
                        status = "成交"
                    elif not is_system:
                        if not conn.execute(
                            "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (seller_id,)
                        ).fetchone():
                            conn.rollback()
                            return AuctionSessionFinishResult("participant_missing", operation_id, session_id)
                        if self._inventory_full(conn, seller_id, item_id):
                            conn.rollback()
                            return AuctionSessionFinishResult("inventory_full", operation_id, session_id)
                        self._grant_item(conn, seller_id, item_id, name, item_type)

                    record = {
                        "auction_id": auction_id, "item_id": item_id, "item_name": name,
                        "start_price": int(row[3]), "final_price": final_price,
                        "seller_id": seller_id, "seller_name": seller_name,
                        "winner_id": winner_id, "winner_name": winner_name,
                        "status": status, "fee": fee, "seller_earnings": earnings,
                        "start_time": float(row[8] or session[0]), "end_time": float(end_time),
                    }
                    conn.execute(
                        """
                        INSERT INTO auction_history (
                            auction_id,item_id,item_name,start_price,final_price,
                            seller_id,seller_name,winner_id,winner_name,status,fee,
                            seller_earnings,start_time,end_time
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        tuple(record[key] for key in (
                            "auction_id", "item_id", "item_name", "start_price", "final_price",
                            "seller_id", "seller_name", "winner_id", "winner_name", "status", "fee",
                            "seller_earnings", "start_time", "end_time",
                        )),
                    )
                    results.append(record)
                conn.execute("DELETE FROM auction_current")
                conn.execute(
                    "UPDATE auction_sessions SET status='settled',finish_operation_id=%s,settled_at=%s "
                    "WHERE session_id=%s AND status='active'",
                    (operation_id, float(end_time), session_id),
                )
                result_value = {"session_id": session_id, "results": results}
                conn.execute(
                    "INSERT INTO auction_session_operations (operation_id,action,payload,result) "
                    "VALUES (%s,'finish',%s,%s)",
                    (operation_id, payload, self._payload(result_value)),
                )
                conn.commit()
                return AuctionSessionFinishResult(
                    "settled", operation_id, session_id, tuple(results)
                )
            except Exception:
                conn.rollback()
                raise

    def _inventory_full(self, conn, user_id: str, item_id: int) -> bool:
        row = conn.execute(
            "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
            (user_id, item_id),
        ).fetchone()
        return bool(row and int(row[0] or 0) >= self._max_goods_num)

    @staticmethod
    def _grant_item(conn, user_id: str, item_id: int, name: str, item_type: str) -> None:
        conn.execute(
            """
            INSERT INTO back (
                user_id,goods_id,goods_name,goods_type,goods_num,
                create_time,update_time,bind_num
            ) VALUES (%s,%s,%s,%s,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)
            ON CONFLICT (user_id,goods_id) DO UPDATE SET
                goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,
                goods_num=COALESCE(back.goods_num,0)+1,
                bind_num=COALESCE(back.bind_num,0)+1,
                update_time=CURRENT_TIMESTAMP
            """,
            (user_id, item_id, name, item_type),
        )
