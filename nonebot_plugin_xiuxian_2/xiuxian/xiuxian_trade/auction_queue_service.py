from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AuctionQueueResult:
    status: str
    action: str
    user_id: str
    item_id: int
    item_name: str = ""
    start_price: int = 0
    user_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


class AuctionQueueService:
    """Atomically move items between inventory and the auction waiting queue."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._trade_database = Path(trade_database)
        self._max_goods_num = max(int(max_goods_num), 1)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_queue_operations (
                operation_id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                start_price INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auction_trade.auction_player_upload (
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                start_price INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                PRIMARY KEY (user_id, item_id)
            )
            """
        )

    @staticmethod
    def _result(status: str, action: str, row) -> AuctionQueueResult:
        user_id, item_id, item_name, start_price, user_name = row
        return AuctionQueueResult(
            status,
            action,
            str(user_id),
            int(item_id),
            str(item_name),
            int(start_price),
            str(user_name),
        )

    def _connect(self):
        conn = db_backend.connect(self._game_database)
        conn.execute("ATTACH DATABASE %s AS auction_trade", (str(self._trade_database),))
        return conn

    def _previous(self, conn, operation_id, action, user_id, item_id):
        row = conn.execute(
            "SELECT action, user_id, item_id, item_name, start_price, user_name "
            "FROM auction_queue_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return None
        previous_action, previous_user, previous_item, name, price, user_name = row
        if (
            str(previous_action) != action
            or str(previous_user) != user_id
            or int(previous_item) != item_id
        ):
            return AuctionQueueResult("state_changed", action, user_id, item_id)
        return AuctionQueueResult(
            "duplicate", action, user_id, item_id, str(name), int(price), str(user_name)
        )

    def get_operation(self, operation_id, action, user_id, item_id):
        operation_id = str(operation_id).strip()
        action = str(action)
        user_id = str(user_id)
        item_id = int(item_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("auction_queue_operations",),
            ).fetchone()
            if exists is None:
                return None
            return self._previous(conn, operation_id, action, user_id, item_id)

    def enqueue(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        start_price,
        user_name,
        *,
        max_user_items: int,
    ) -> AuctionQueueResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        start_price = int(start_price)
        user_name = str(user_name)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if start_price <= 0:
            raise ValueError("start_price must be positive")

        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._previous(
                    conn, operation_id, "enqueue", user_id, item_id
                )
                if previous is not None:
                    conn.rollback()
                    return previous
                queued = conn.execute(
                    "SELECT COUNT(*) FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()[0]
                if int(queued) >= max(int(max_user_items), 1):
                    conn.rollback()
                    return AuctionQueueResult("limit_reached", "enqueue", user_id, item_id)
                if conn.execute(
                    "SELECT 1 FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s AND item_id=%s",
                    (user_id, item_id),
                ).fetchone():
                    conn.rollback()
                    return AuctionQueueResult("already_queued", "enqueue", user_id, item_id)

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                consumed = conn.execute(
                    """
                    UPDATE back
                    SET goods_num=goods_num-1,
                        bind_num=LEAST(COALESCE(bind_num, 0), goods_num-1),
                        update_time=%s
                    WHERE user_id=%s AND goods_id=%s
                      AND COALESCE(goods_num, 0)-COALESCE(bind_num, 0)
                          -COALESCE(state, 0) >= 1
                    """,
                    (now, user_id, item_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return AuctionQueueResult("stock_insufficient", "enqueue", user_id, item_id)
                conn.execute(
                    "INSERT INTO auction_trade.auction_player_upload "
                    "(user_id, item_id, item_name, start_price, user_name) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (user_id, item_id, item_name, start_price, user_name),
                )
                conn.execute(
                    "INSERT INTO auction_queue_operations "
                    "(operation_id, action, user_id, item_id, item_name, "
                    "start_price, user_name) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id,
                        "enqueue",
                        user_id,
                        item_id,
                        item_name,
                        start_price,
                        user_name,
                    ),
                )
                conn.commit()
                return AuctionQueueResult(
                    "completed", "enqueue", user_id, item_id,
                    item_name, start_price, user_name
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE auction_trade")

    def dequeue(self, operation_id, user_id, item_id, item_type) -> AuctionQueueResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_type = str(item_type)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = self._previous(
                    conn, operation_id, "dequeue", user_id, item_id
                )
                if previous is not None:
                    conn.rollback()
                    return previous
                row = conn.execute(
                    "SELECT user_id, item_id, item_name, start_price, user_name "
                    "FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s AND item_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AuctionQueueResult("queue_missing", "dequeue", user_id, item_id)
                inventory = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if inventory is not None and int(inventory[0]) >= self._max_goods_num:
                    conn.rollback()
                    return AuctionQueueResult("inventory_full", "dequeue", user_id, item_id)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                conn.execute(
                    """
                    INSERT INTO back (
                        user_id, goods_id, goods_name, goods_type,
                        goods_num, create_time, update_time, bind_num
                    ) VALUES (%s, %s, %s, %s, 1, %s, %s, 1)
                    ON CONFLICT (user_id, goods_id) DO UPDATE
                    SET goods_num=COALESCE(back.goods_num, 0)+1,
                        bind_num=COALESCE(back.bind_num, 0)+1,
                        update_time=EXCLUDED.update_time
                    """,
                    (user_id, item_id, str(row[2]), item_type, now, now),
                )
                conn.execute(
                    "DELETE FROM auction_trade.auction_player_upload "
                    "WHERE user_id=%s AND item_id=%s",
                    (user_id, item_id),
                )
                conn.execute(
                    "INSERT INTO auction_queue_operations "
                    "(operation_id, action, user_id, item_id, item_name, "
                    "start_price, user_name) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, "dequeue", *row),
                )
                conn.commit()
                return self._result("completed", "dequeue", row)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE auction_trade")


__all__ = ["AuctionQueueResult", "AuctionQueueService"]
