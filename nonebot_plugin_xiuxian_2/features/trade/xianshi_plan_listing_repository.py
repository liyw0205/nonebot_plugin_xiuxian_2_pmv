from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.ids import UUIDGenerator


@dataclass(frozen=True)
class XianshiPlanListingResult:
    status: str
    seller_id: str
    item_count: int
    listed_quantity: int = 0
    fee_charged: int = 0
    fee_refund: int = 0
    stamina_charged: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"listed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "listed"


class XianshiPlanListingSqlRepository:
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
        seller_id: str,
        item_count: int,
        listed_quantity: int = 0,
        fee_charged: int = 0,
        stamina_charged: int = 0,
    ) -> XianshiPlanListingResult:
        return XianshiPlanListingResult(
            status,
            seller_id,
            item_count,
            listed_quantity,
            fee_charged,
            0,
            stamina_charged,
        )

    def list_plan(
        self,
        operation_id: str,
        seller_id: str,
        listing_plan: Iterable[Mapping[str, Any]],
        *,
        stamina_cost: int = 30,
    ) -> XianshiPlanListingResult:
        operation_id, seller_id = str(operation_id).strip(), str(seller_id)
        stamina_cost = int(stamina_cost)
        plan = []
        for entry in listing_plan:
            quantity = int(entry["quantity"])
            if quantity <= 0:
                continue
            normalized = {
                "goods_id": int(entry["goods_id"]),
                "name": str(entry["name"]),
                "goods_type": str(entry["goods_type"]),
                "price": int(entry["price"]),
                "quantity": quantity,
            }
            if normalized["goods_id"] <= 0 or normalized["price"] <= 0:
                raise ValueError("goods_id and price must be positive")
            plan.append(normalized)
        if not operation_id or not seller_id:
            raise ValueError("operation_id and seller_id are required")
        if not plan:
            raise ValueError("listing_plan must not be empty")
        if stamina_cost < 0:
            raise ValueError("stamina_cost must not be negative")

        plan_text = json.dumps(plan, ensure_ascii=False, sort_keys=True)
        item_count = len(plan)
        listed_quantity = sum(entry["quantity"] for entry in plan)
        fee = sum(
            self._fee(entry["price"] * entry["quantity"]) for entry in plan
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT seller_id,listing_plan,listed_quantity,fee_charged,stamina_cost "
                "FROM xianshi_plan_listing_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if (
                    str(previous["seller_id"]) != seller_id
                    or str(previous["listing_plan"]) != plan_text
                    or int(previous["stamina_cost"] or 0) != stamina_cost
                ):
                    return self._result("state_changed", seller_id, item_count)
                return self._result(
                    "duplicate",
                    seller_id,
                    item_count,
                    int(previous["listed_quantity"]),
                    int(previous["fee_charged"]),
                    int(previous["stamina_cost"]),
                )

            player = uow.query_one(
                "SELECT COALESCE(stone,0) AS stone,"
                "COALESCE(user_stamina,0) AS stamina "
                "FROM user_xiuxian WHERE user_id=?",
                (seller_id,),
            )
            if player is None or int(player["stamina"] or 0) < stamina_cost:
                return self._result("stamina_insufficient", seller_id, item_count)
            if float(player["stone"] or 0) < fee:
                return self._result("stone_insufficient", seller_id, item_count)

            required_stock: dict[int, int] = {}
            for entry in plan:
                goods_id = entry["goods_id"]
                required_stock[goods_id] = (
                    required_stock.get(goods_id, 0) + entry["quantity"]
                )
            for goods_id, quantity in required_stock.items():
                stock = uow.query_one(
                    "SELECT COALESCE(goods_num,0)-COALESCE(bind_num,0)-"
                    "COALESCE(state,0) AS tradeable FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (seller_id, goods_id),
                )
                if stock is None or int(stock["tradeable"] or 0) < quantity:
                    return self._result("stock_insufficient", seller_id, item_count)

            now = self.clock.now()
            stamina_update = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=COALESCE(user_stamina,0)-? "
                "WHERE user_id=? AND COALESCE(user_stamina,0)>=?",
                (stamina_cost, seller_id, stamina_cost),
            )
            if stamina_update.rowcount != 1:
                raise RuntimeError("xianshi plan stamina snapshot changed")
            stone_update = uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL) "
                "WHERE user_id=? AND COALESCE(stone,0)>=?",
                (fee, seller_id, fee),
            )
            if stone_update.rowcount != 1:
                raise RuntimeError("xianshi plan stone snapshot changed")

            for entry in plan:
                stock_update = uow.execute(
                    "UPDATE back SET goods_num=COALESCE(goods_num,0)-?,"
                    "update_time=?,action_time=? WHERE user_id=? AND goods_id=? "
                    "AND COALESCE(goods_num,0)-COALESCE(bind_num,0)-"
                    "COALESCE(state,0)>=?",
                    (
                        entry["quantity"],
                        now,
                        now,
                        seller_id,
                        entry["goods_id"],
                        entry["quantity"],
                    ),
                )
                if stock_update.rowcount != 1:
                    raise RuntimeError("xianshi plan stock snapshot changed")

            listed = 0
            for entry in plan:
                for _ in range(entry["quantity"]):
                    for _attempt in range(20):
                        listing_id = self._listing_id(self.ids.new_id())
                        exists = uow.query_one(
                            "SELECT 1 AS present FROM xianshi_item WHERE id=?",
                            (listing_id,),
                        )
                        if exists is not None:
                            continue
                        uow.execute(
                            "INSERT INTO xianshi_item"
                            "(id,user_id,goods_id,name,type,price,quantity) "
                            "VALUES(?,?,?,?,?,?,1)",
                            (
                                listing_id,
                                seller_id,
                                entry["goods_id"],
                                entry["name"],
                                entry["goods_type"],
                                entry["price"],
                            ),
                        )
                        listed += 1
                        break
                    else:
                        raise RuntimeError("failed to allocate xianshi listing id")

            uow.execute(
                "INSERT INTO xianshi_plan_listing_operations "
                "(operation_id,seller_id,listing_plan,listed_quantity,fee_charged,"
                "stamina_cost) VALUES(?,?,?,?,?,?)",
                (operation_id, seller_id, plan_text, listed, fee, stamina_cost),
            )
            return self._result(
                "listed", seller_id, item_count, listed, fee, stamina_cost
            )


__all__ = ["XianshiPlanListingResult", "XianshiPlanListingSqlRepository"]
