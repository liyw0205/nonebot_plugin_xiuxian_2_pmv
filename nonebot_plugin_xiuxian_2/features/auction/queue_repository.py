from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

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

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "completed"


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
    def _previous(uow: DatabaseUnitOfWork, operation_id: str, action: str, user_id: str, item_id: int):
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
        ):
            return AuctionQueueResult("state_changed", action, user_id, item_id)
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
            previous = self._previous(uow, operation_id, "enqueue", user_id, item_id)
            if previous is not None:
                return previous
            queued = uow.query_one(
                "SELECT COUNT(*) AS count FROM auction_trade.auction_player_upload "
                "WHERE user_id=?",
                (user_id,),
            )
            if int(queued["count"]) >= max(int(max_user_items), 1):
                return AuctionQueueResult("limit_reached", "enqueue", user_id, item_id)
            if uow.query_one(
                "SELECT 1 AS present FROM auction_trade.auction_player_upload "
                "WHERE user_id=? AND item_id=?",
                (user_id, item_id),
            ):
                return AuctionQueueResult("already_queued", "enqueue", user_id, item_id)

            now = self.clock.now().isoformat(sep=" ")
            consumed = uow.execute(
                "UPDATE back SET goods_num=goods_num-1, "
                "bind_num=MIN(COALESCE(bind_num,0),COALESCE(goods_num,0)-1),update_time=? "
                "WHERE user_id=? AND goods_id=? "
                "AND COALESCE(goods_num,0)-COALESCE(bind_num,0)-COALESCE(state,0)>=1",
                (now, user_id, item_id),
            )
            if consumed.rowcount != 1:
                return AuctionQueueResult("stock_insufficient", "enqueue", user_id, item_id)
            uow.execute(
                "INSERT INTO auction_trade.auction_player_upload "
                "(user_id,item_id,item_name,start_price,user_name) VALUES(?,?,?,?,?)",
                (user_id, item_id, item_name, start_price, user_name),
            )
            uow.execute(
                "INSERT INTO auction_queue_operations "
                "(operation_id,action,user_id,item_id,item_name,start_price,user_name) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, "enqueue", user_id, item_id, item_name, start_price, user_name),
            )
            return AuctionQueueResult(
                "completed", "enqueue", user_id, item_id, item_name, start_price, user_name
            )

    def dequeue(
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
                return AuctionQueueResult("queue_missing", "dequeue", user_id, item_id)
            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                "WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if inventory is not None and int(inventory["goods_num"]) >= self.max_goods_num:
                return AuctionQueueResult("inventory_full", "dequeue", user_id, item_id)

            now = self.clock.now().isoformat(sep=" ")
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,1,?,?,1) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_num=COALESCE(back.goods_num,0)+1,"
                "bind_num=COALESCE(back.bind_num,0)+1,update_time=excluded.update_time",
                (user_id, item_id, str(row["item_name"]), item_type, now, now),
            )
            uow.execute(
                "DELETE FROM auction_trade.auction_player_upload WHERE user_id=? AND item_id=?",
                (user_id, item_id),
            )
            uow.execute(
                "INSERT INTO auction_queue_operations "
                "(operation_id,action,user_id,item_id,item_name,start_price,user_name) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    operation_id,
                    "dequeue",
                    row["user_id"],
                    row["item_id"],
                    row["item_name"],
                    row["start_price"],
                    row["user_name"],
                ),
            )
            return self._from_row("completed", "dequeue", row)


__all__ = ["AuctionQueueResult", "AuctionQueueSqlRepository"]
