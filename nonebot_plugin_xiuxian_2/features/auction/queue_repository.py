from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import require_exact_integer
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AuctionQueueResult:
    status: str
    action: str
    user_id: str
    item_id: int
    item_name: str = ""
    start_price: int = 0
    user_name: str = ""
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


class _QueueRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason, self.subject = reason, subject


class _QueueWrites:
    """Verify the handoff again after later triggers have run."""

    def __init__(self, uow: DatabaseUnitOfWork) -> None:
        self.uow = uow
        self.expected: list[tuple[str, tuple, dict | None, str, str]] = []

    @staticmethod
    def affected(cursor: Any, target: str, subject: str) -> None:
        if cursor.rowcount != 1:
            raise _QueueRejected(f"auction_queue_{target}_write_rejected", subject)

    def expect(self, sql: str, params: tuple, row: dict | None, target: str, subject: str) -> None:
        self.expected.append((sql, params, row, target, subject))
        if self.uow.query_one(sql, params) != row:
            raise _QueueRejected(f"auction_queue_{target}_write_rejected", subject)

    def verify(self) -> None:
        for sql, params, row, target, subject in self.expected:
            if self.uow.query_one(sql, params) != row:
                raise _QueueRejected(f"auction_queue_{target}_write_rejected", subject)


class AuctionQueueSqlRepository:
    """Atomically move tradeable inventory into and out of the auction queue."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        max_goods_num: int,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.max_goods_num = max(int(max_goods_num), 1)
        self.clock = clock or SystemClock()

    def get_player_items(self, user_id: str | None = None) -> list[dict[str, Any]]:
        database = Path(self.trade_database)
        if not database.is_file():
            return []
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' "
                "AND name='auction_player_upload'"
            ) is None:
                return []
            if user_id is None:
                return uow.query_all(
                    "SELECT user_id,item_id,item_name,start_price,user_name "
                    "FROM auction_player_upload"
                )
            return uow.query_all(
                "SELECT user_id,item_id,item_name,start_price,user_name "
                "FROM auction_player_upload WHERE user_id=?",
                (str(user_id),),
            )

    def count_player_items(self, user_id: str | None = None) -> int:
        database = Path(self.trade_database)
        if not database.is_file():
            return 0
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' "
                "AND name='auction_player_upload'"
            ) is None:
                return 0
            if user_id is None:
                row = uow.query_one(
                    "SELECT COUNT(*) AS total FROM auction_player_upload"
                )
            else:
                row = uow.query_one(
                    "SELECT COUNT(*) AS total FROM auction_player_upload WHERE user_id=?",
                    (str(user_id),),
                )
            return int(row["total"]) if row else 0

    @staticmethod
    def _from_row(status: str, action: str, row) -> AuctionQueueResult:
        return AuctionQueueResult(
            status,
            action,
            str(row["user_id"]),
            int(row["item_id"]),
            str(row["item_name"]),
            int(row["start_price"]),
            str(row["user_name"]),
        )

    @staticmethod
    def _previous(uow: DatabaseUnitOfWork, operation_id: str, action: str, user_id: str, item_id: int,
                  start_price: int | None = None):
        row = uow.query_one(
            "SELECT action,user_id,item_id,item_name,start_price,user_name "
            "FROM auction_queue_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return None
        if (
            str(row["action"]) != action
            or str(row["user_id"]) != user_id
            or int(row["item_id"]) != item_id
            or (start_price is not None and int(row["start_price"]) != start_price)
        ):
            return AuctionQueueResult("state_changed", action, user_id, item_id,
                                      reason="operation_conflict", subject=operation_id)
        return AuctionQueueResult(
            "duplicate",
            action,
            user_id,
            item_id,
            str(row["item_name"]),
            int(row["start_price"]),
            str(row["user_name"]),
        )

    def get_operation(self, operation_id: str, action: str, user_id: str, item_id: int):
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database) as uow:
            return self._previous(
                uow, operation_id, str(action), str(user_id), int(item_id)
            )

    def enqueue(
        self, operation_id: str, user_id: str, item_id: int, item_name: str,
        start_price: int, user_name: str, *, max_user_items: int,
    ) -> AuctionQueueResult:
        try:
            return self._enqueue(operation_id, user_id, item_id, item_name, start_price,
                                 user_name, max_user_items=max_user_items)
        except _QueueRejected as exc:
            return AuctionQueueResult("state_changed", "enqueue", str(user_id), int(item_id),
                                      reason=exc.reason, subject=exc.subject)
        except DomainError as exc:
            if not exc.details.get("reason"):
                raise
            return AuctionQueueResult("state_changed", "enqueue", str(user_id), int(item_id),
                                      reason=exc.details["reason"], subject=str(exc.details.get("subject") or user_id))

    @staticmethod
    def _inventory(uow: DatabaseUnitOfWork, user_id: str, item_id: int):
        row = uow.query_one("SELECT * FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
        subject = f"{user_id}:{item_id}"
        values = tuple(require_exact_integer((row or {}).get(key) if (row or {}).get(key) is not None else 0,
                                            field=key, subject=subject)
                       for key in ("goods_num", "bind_num", "state"))
        count, bound, equipped = values
        if count < 0 or bound < 0 or bound > count or equipped < 0 or equipped > count:
            raise _QueueRejected("auction_queue_inventory_invalid", subject)
        return row, count, bound, equipped

    @staticmethod
    def _receipt(uow: DatabaseUnitOfWork, writes: _QueueWrites, operation_id: str,
                 action: str, row: dict) -> None:
        cursor = uow.execute(
            "INSERT INTO auction_queue_operations "
            "(operation_id,action,user_id,item_id,item_name,start_price,user_name) VALUES(?,?,?,?,?,?,?)",
            (operation_id, action, row["user_id"], row["item_id"], row["item_name"], row["start_price"], row["user_name"]),
        )
        writes.affected(cursor, "receipt", operation_id)
        writes.expect("SELECT operation_id,action,user_id,item_id,item_name,start_price,user_name "
                      "FROM auction_queue_operations WHERE operation_id=?", (operation_id,),
                      dict(row, operation_id=operation_id, action=action), "receipt", operation_id)
        writes.verify()

    def _enqueue(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        item_name: str,
        start_price: int,
        user_name: str,
        *,
        max_user_items: int,
    ) -> AuctionQueueResult:
        operation_id = str(operation_id).strip()
        user_id, item_id = str(user_id), int(item_id)
        item_name, start_price, user_name = str(item_name), int(start_price), str(user_name)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if start_price <= 0:
            raise ValueError("start_price must be positive")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "auction_trade")
            previous = self._previous(uow, operation_id, "enqueue", user_id, item_id, start_price)
            if previous is not None:
                return previous
            queued = uow.query_one(
                "SELECT COUNT(*) AS count FROM auction_trade.auction_player_upload "
                "WHERE user_id=?",
                (user_id,),
            )
            if int(queued["count"]) >= max(int(max_user_items), 1):
                return AuctionQueueResult("limit_reached", "enqueue", user_id, item_id,
                                          reason="limit_reached", subject=user_id)
            if uow.query_one(
                "SELECT 1 AS present FROM auction_trade.auction_player_upload "
                "WHERE user_id=? AND item_id=?",
                (user_id, item_id),
            ):
                return AuctionQueueResult("already_queued", "enqueue", user_id, item_id,
                                          reason="already_queued", subject=f"{user_id}:{item_id}")

            inventory, count, bound, equipped = self._inventory(uow, user_id, item_id)
            subject = f"{user_id}:{item_id}"
            if inventory is None or count - bound - equipped < 1:
                return AuctionQueueResult("stock_insufficient", "enqueue", user_id, item_id,
                                          reason="stock_insufficient", subject=subject)
            writes = _QueueWrites(uow)
            now = self.clock.now().isoformat(sep=" ")
            consumed = uow.execute(
                "UPDATE back SET goods_num=?,bind_num=?,update_time=? WHERE user_id=? AND goods_id=?",
                (count - 1, bound, now, user_id, item_id),
            )
            writes.affected(consumed, "inventory", subject)
            writes.expect("SELECT * FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id),
                          dict(inventory, goods_num=count - 1, bind_num=bound, update_time=now), "inventory", subject)
            row = dict(user_id=user_id, item_id=item_id, item_name=item_name,
                       start_price=start_price, user_name=user_name)
            cursor = uow.execute(
                "INSERT INTO auction_trade.auction_player_upload "
                "(user_id,item_id,item_name,start_price,user_name) VALUES(?,?,?,?,?)",
                (user_id, item_id, item_name, start_price, user_name),
            )
            writes.affected(cursor, "queue", subject)
            writes.expect("SELECT * FROM auction_trade.auction_player_upload WHERE user_id=? AND item_id=?",
                          (user_id, item_id), row, "queue", subject)
            self._receipt(uow, writes, operation_id, "enqueue", row)
            return AuctionQueueResult(
                "completed", "enqueue", user_id, item_id, item_name, start_price, user_name
            )

    def dequeue(
        self, operation_id: str, user_id: str, item_id: int, item_type: str
    ) -> AuctionQueueResult:
        try:
            return self._dequeue(operation_id, user_id, item_id, item_type)
        except _QueueRejected as exc:
            return AuctionQueueResult("state_changed", "dequeue", str(user_id), int(item_id),
                                      reason=exc.reason, subject=exc.subject)
        except DomainError as exc:
            if not exc.details.get("reason"):
                raise
            return AuctionQueueResult("state_changed", "dequeue", str(user_id), int(item_id),
                                      reason=exc.details["reason"], subject=str(exc.details.get("subject") or user_id))

    def _dequeue(
        self, operation_id: str, user_id: str, item_id: int, item_type: str
    ) -> AuctionQueueResult:
        operation_id = str(operation_id).strip()
        user_id, item_id, item_type = str(user_id), int(item_id), str(item_type)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "auction_trade")
            previous = self._previous(uow, operation_id, "dequeue", user_id, item_id)
            if previous is not None:
                return previous
            row = uow.query_one(
                "SELECT user_id,item_id,item_name,start_price,user_name "
                "FROM auction_trade.auction_player_upload WHERE user_id=? AND item_id=?",
                (user_id, item_id),
            )
            if row is None:
                return AuctionQueueResult("queue_missing", "dequeue", user_id, item_id,
                                          reason="queue_missing", subject=f"{user_id}:{item_id}")
            inventory, count, bound, _ = self._inventory(uow, user_id, item_id)
            subject = f"{user_id}:{item_id}"
            if count >= self.max_goods_num:
                return AuctionQueueResult("inventory_full", "dequeue", user_id, item_id,
                                          reason="inventory_full", subject=subject)

            writes = _QueueWrites(uow)
            now = self.clock.now().isoformat(sep=" ")
            cursor = uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,1,?,?,1) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_num=COALESCE(back.goods_num,0)+1,"
                "bind_num=COALESCE(back.bind_num,0)+1,update_time=excluded.update_time",
                (user_id, item_id, str(row["item_name"]), item_type, now, now),
            )
            writes.affected(cursor, "inventory", subject)
            if inventory is not None:
                inventory_sql = "SELECT * FROM back WHERE user_id=? AND goods_id=?"
                expected = dict(inventory, goods_num=count + 1, bind_num=bound + 1, update_time=now)
            else:
                inventory_sql = ("SELECT user_id,goods_id,goods_name,goods_type,goods_num,bind_num,"
                                 "COALESCE(state,0) AS state,create_time,update_time "
                                 "FROM back WHERE user_id=? AND goods_id=?")
                expected = dict(user_id=user_id, goods_id=item_id, goods_name=str(row["item_name"]),
                                goods_type=item_type, goods_num=1, bind_num=1, state=0, create_time=now, update_time=now)
            writes.expect(inventory_sql, (user_id, item_id), expected, "inventory", subject)
            cursor = uow.execute(
                "DELETE FROM auction_trade.auction_player_upload WHERE user_id=? AND item_id=?",
                (user_id, item_id),
            )
            writes.affected(cursor, "queue", subject)
            writes.expect("SELECT * FROM auction_trade.auction_player_upload WHERE user_id=? AND item_id=?",
                          (user_id, item_id), None, "queue", subject)
            self._receipt(uow, writes, operation_id, "dequeue", row)
            return self._from_row("completed", "dequeue", row)


__all__ = ["AuctionQueueResult", "AuctionQueueSqlRepository"]
