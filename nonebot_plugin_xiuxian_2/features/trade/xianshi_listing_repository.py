from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.ids import UUIDGenerator


@dataclass(frozen=True)
class XianshiListingResult:
    status: str
    operation_id: str
    seller_id: str
    goods_id: int
    name: str
    goods_type: str
    price: int
    requested_quantity: int
    listed_quantity: int = 0
    fee_charged: int = 0
    stamina_charged: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"listed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "listed"


class XianshiListingSqlRepository:
    def __init__(self, database: str | Path, *, clock: Any = None, ids: Any = None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()
        self.ids = ids or UUIDGenerator()

    @staticmethod
    def _fee(total_price: int) -> int:
        if total_price <= 5_000_000:
            rate = 0.1
        elif total_price <= 10_000_000:
            rate = 0.15
        elif total_price <= 20_000_000:
            rate = 0.2
        else:
            rate = 0.3
        return int(total_price * rate)

    @staticmethod
    def _listing_id(raw_id: str) -> str:
        try:
            numeric_id = int(str(raw_id), 16)
        except ValueError as exc:
            raise ValueError("ID generator must return a hexadecimal identifier") from exc
        return str(numeric_id % 9_000_000_000_000 + 1_000_000_000_000)

    @staticmethod
    def _result(
        status: str,
        operation_id: str,
        seller_id: str,
        goods_id: int,
        name: str,
        goods_type: str,
        price: int,
        quantity: int,
        listed_quantity: int = 0,
        fee_charged: int = 0,
        stamina_charged: int = 0,
    ) -> XianshiListingResult:
        return XianshiListingResult(
            status, operation_id, seller_id, goods_id, name, goods_type, price,
            quantity, listed_quantity, fee_charged, stamina_charged,
        )

    def list_items(
        self,
        operation_id: str,
        seller_id: str,
        goods_id: int,
        name: str,
        goods_type: str,
        price: int,
        quantity: int,
        *,
        stamina_cost: int = 0,
    ) -> XianshiListingResult:
        operation_id, seller_id = str(operation_id).strip(), str(seller_id)
        goods_id, price, quantity = int(goods_id), int(price), int(quantity)
        stamina_cost = int(stamina_cost)
        name, goods_type = str(name), str(goods_type)
        if not operation_id or not seller_id:
            raise ValueError("operation_id and seller_id are required")
        if goods_id <= 0 or price <= 0 or quantity <= 0:
            raise ValueError("goods_id, price and quantity must be positive")
        if stamina_cost < 0:
            raise ValueError("stamina_cost must not be negative")
        fee = self._fee(price * quantity)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost FROM xianshi_listing_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                same_request = (
                    str(previous["seller_id"]) == seller_id
                    and int(previous["goods_id"]) == goods_id
                    and str(previous["name"]) == name
                    and str(previous["goods_type"]) == goods_type
                    and int(previous["price"]) == price
                    and int(previous["requested_quantity"]) == quantity
                    and int(previous["stamina_cost"] or 0) == stamina_cost
                )
                if not same_request:
                    return self._result(
                        "state_changed", operation_id, seller_id, goods_id, name,
                        goods_type, price, quantity,
                    )
                return self._result(
                    "duplicate", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity, int(previous["listed_quantity"]),
                    int(previous["fee_charged"]), int(previous["stamina_cost"] or 0),
                )

            player_columns = "COALESCE(stone,0) AS stone"
            if stamina_cost:
                player_columns += ",COALESCE(user_stamina,0) AS stamina"
            player = uow.query_one(
                f"SELECT {player_columns} FROM user_xiuxian WHERE user_id=?",
                (seller_id,),
            )
            if player is None:
                return self._result(
                    "player_missing", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity,
                )
            if stamina_cost and int(player["stamina"] or 0) < stamina_cost:
                return self._result(
                    "stamina_insufficient", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity,
                )
            if float(player["stone"] or 0) < fee:
                return self._result(
                    "stone_insufficient", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity,
                )
            stock_row = uow.query_one(
                "SELECT COALESCE(goods_num,0)-COALESCE(bind_num,0)-COALESCE(state,0) "
                "AS tradeable FROM back WHERE user_id=? AND goods_id=?",
                (seller_id, goods_id),
            )
            if stock_row is None or int(stock_row["tradeable"] or 0) < quantity:
                return self._result(
                    "stock_insufficient", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity,
                )
            now = self.clock.now()
            if stamina_cost:
                stamina = uow.execute(
                    "UPDATE user_xiuxian SET user_stamina=COALESCE(user_stamina,0)-? "
                    "WHERE user_id=? AND COALESCE(user_stamina,0)>=?",
                    (stamina_cost, seller_id, stamina_cost),
                )
                if stamina.rowcount != 1:
                    raise RuntimeError("xianshi listing stamina snapshot changed")
            changed_stone = uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL) "
                "WHERE user_id=? AND COALESCE(stone,0)>=?",
                (fee, seller_id, fee),
            )
            if changed_stone.rowcount != 1:
                raise RuntimeError("xianshi listing stone snapshot changed")
            stock = uow.execute(
                "UPDATE back SET goods_num=COALESCE(goods_num,0)-?,update_time=?,action_time=? "
                "WHERE user_id=? AND goods_id=? AND "
                "COALESCE(goods_num,0)-COALESCE(bind_num,0)-COALESCE(state,0)>=?",
                (
                    quantity,
                    now,
                    now,
                    seller_id,
                    goods_id,
                    quantity,
                ),
            )
            if stock.rowcount != 1:
                raise RuntimeError("xianshi listing stock snapshot changed")

            for _ in range(quantity):
                for _attempt in range(20):
                    listing_id = self._listing_id(self.ids.new_id())
                    exists = uow.query_one(
                        "SELECT 1 AS present FROM xianshi_item WHERE id=?", (listing_id,)
                    )
                    if exists is not None:
                        continue
                    uow.execute(
                        "INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity) "
                        "VALUES(?,?,?,?,?,?,1)",
                        (listing_id, seller_id, goods_id, name, goods_type, price),
                    )
                    break
                else:
                    raise RuntimeError("failed to allocate xianshi listing id")

            uow.execute(
                "INSERT INTO xianshi_listing_operations "
                "(operation_id,seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    operation_id, seller_id, goods_id, name, goods_type, price,
                    quantity, quantity, fee, stamina_cost,
                ),
            )
            return self._result(
                "listed", operation_id, seller_id, goods_id, name, goods_type,
                price, quantity, quantity, fee, stamina_cost,
            )


__all__ = ["XianshiListingResult", "XianshiListingSqlRepository"]
