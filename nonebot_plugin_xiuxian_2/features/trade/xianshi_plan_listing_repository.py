from __future__ import annotations

from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer

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
    reason: str = ""
    subject: str = ""

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
            rate = 10
        elif total_price <= 10_000_000:
            rate = 15
        elif total_price <= 20_000_000:
            rate = 20
        else:
            rate = 30
        return total_price * rate // 100

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
        reason: str = "",
        subject: str = "",
    ) -> XianshiPlanListingResult:
        return XianshiPlanListingResult(
            status,
            seller_id,
            item_count,
            listed_quantity,
            fee_charged,
            0,
            stamina_charged,
            reason,
            subject,
        )

    @staticmethod
    def _release_savepoint(uow: DatabaseUnitOfWork) -> None:
        uow.execute('RELEASE SAVEPOINT "xianshi_plan_listing"')

    @classmethod
    def _reject_write(
        cls,
        uow: DatabaseUnitOfWork,
        result: XianshiPlanListingResult,
        reason: str,
        subject: str,
    ) -> XianshiPlanListingResult:
        uow.execute('ROLLBACK TO SAVEPOINT "xianshi_plan_listing"')
        cls._release_savepoint(uow)
        return cls._result(
            "state_changed",
            result.seller_id,
            result.item_count,
            reason=reason,
            subject=subject,
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
                    return self._result(
                        "state_changed", seller_id, item_count,
                        reason="operation_conflict", subject=operation_id,
                    )
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
            if as_int_like(player["stone"]) < fee:
                return self._result("stone_insufficient", seller_id, item_count)

            required_stock: dict[int, int] = {}
            for entry in plan:
                goods_id = entry["goods_id"]
                required_stock[goods_id] = (
                    required_stock.get(goods_id, 0) + entry["quantity"]
                )
            stock_expected: dict[int, int] = {}
            for goods_id, quantity in required_stock.items():
                stock = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num,"
                    "COALESCE(goods_num,0)-COALESCE(bind_num,0)-"
                    "COALESCE(state,0) AS tradeable FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (seller_id, goods_id),
                )
                if stock is None or int(stock["tradeable"] or 0) < quantity:
                    return self._result("stock_insufficient", seller_id, item_count)
                stock_expected[goods_id] = int(stock["goods_num"] or 0)

            now = self.clock.now()
            uow.execute('SAVEPOINT "xianshi_plan_listing"')
            expected_stamina = int(player["stamina"] or 0) - stamina_cost
            stamina_update = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=COALESCE(user_stamina,0)-? "
                "WHERE user_id=? AND COALESCE(user_stamina,0)>=?",
                (stamina_cost, seller_id, stamina_cost),
            )
            saved_stamina = uow.query_one(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (seller_id,)
            )
            if (
                stamina_update.rowcount != 1
                or saved_stamina is None
                or int(saved_stamina["user_stamina"] or 0) != expected_stamina
            ):
                return self._reject_write(
                    uow,
                    self._result("listed", seller_id, item_count),
                    "xianshi_plan_listing_stamina_write_rejected",
                    seller_id,
                )
            stone_update = uow.execute(
                "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_sub(COALESCE(stone,0),?,'stone','actor')) "
                "WHERE user_id=? AND xiuxian_int_cmp(COALESCE(stone,0),?,1,'stone','actor')>=0",
                (bind_sqlite_param(fee, exact=True), seller_id, bind_sqlite_param(fee, exact=True)),
            )
            saved_stone = uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id=?", (seller_id,)
            )
            expected_stone = require_exact_integer(player["stone"], field="stone", subject="seller") - fee
            if (
                stone_update.rowcount != 1
                or saved_stone is None
                or require_exact_integer(saved_stone["stone"], field="stone", subject="seller") != expected_stone
            ):
                return self._reject_write(
                    uow,
                    self._result("listed", seller_id, item_count),
                    "xianshi_plan_listing_stone_write_rejected",
                    seller_id,
                )

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
                goods_id = entry["goods_id"]
                stock_expected[goods_id] -= entry["quantity"]
                saved_stock = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (seller_id, goods_id),
                )
                if (
                    stock_update.rowcount != 1
                    or saved_stock is None
                    or int(saved_stock["goods_num"] or 0) != stock_expected[goods_id]
                ):
                    return self._reject_write(
                        uow,
                        self._result("listed", seller_id, item_count),
                        "xianshi_plan_listing_stock_write_rejected",
                        f"{seller_id}:{goods_id}",
                    )

            listed = 0
            listing_ids: list[str] = []
            listing_expected: dict[str, dict[str, object]] = {}
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
                        listing_write = uow.execute(
                            "INSERT INTO xianshi_item"
                            "(id,user_id,goods_id,name,type,price,quantity) "
                            "VALUES(?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),1)",
                            (
                                listing_id,
                                seller_id,
                                entry["goods_id"],
                                entry["name"],
                                entry["goods_type"],
                                entry["price"],
                            ),
                        )
                        saved_listing = uow.query_one(
                            "SELECT id,user_id,goods_id,name,type,price,quantity FROM xianshi_item WHERE id=?",
                            (listing_id,),
                        )
                        expected_listing = {
                            "id": listing_id,
                            "user_id": seller_id,
                            "goods_id": entry["goods_id"],
                            "name": entry["name"],
                            "type": entry["goods_type"],
                            "price": entry["price"],
                            "quantity": 1,
                        }
                        if listing_write.rowcount != 1 or saved_listing != expected_listing:
                            return self._reject_write(
                                uow,
                                self._result("listed", seller_id, item_count),
                                "xianshi_plan_listing_item_write_rejected",
                                listing_id,
                            )
                        listing_ids.append(listing_id)
                        listing_expected[listing_id] = expected_listing
                        listed += 1
                        break
                    else:
                        raise RuntimeError("failed to allocate xianshi listing id")

            receipt_write = uow.execute(
                "INSERT INTO xianshi_plan_listing_operations "
                "(operation_id,seller_id,listing_plan,listed_quantity,fee_charged,"
                "stamina_cost) VALUES(?,?,?,?,xiuxian_int_store(?,'stone','actor'),?)",
                (operation_id, seller_id, plan_text, listed, bind_sqlite_param(fee, exact=True), stamina_cost),
            )
            saved_receipt = uow.query_one(
                "SELECT operation_id,seller_id,listing_plan,listed_quantity,fee_charged,stamina_cost "
                "FROM xianshi_plan_listing_operations WHERE operation_id=?",
                (operation_id,),
            )
            expected_receipt = {
                "operation_id": operation_id,
                "seller_id": seller_id,
                "listing_plan": plan_text,
                "listed_quantity": listed,
                "fee_charged": fee,
                "stamina_cost": stamina_cost,
            }
            if receipt_write.rowcount != 1 or saved_receipt != expected_receipt:
                return self._reject_write(
                    uow,
                    self._result("listed", seller_id, item_count),
                    "xianshi_plan_listing_receipt_write_rejected",
                    operation_id,
                )
            for goods_id, expected_goods_num in stock_expected.items():
                saved_stock = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (seller_id, goods_id),
                )
                if saved_stock is None or int(saved_stock["goods_num"] or 0) != expected_goods_num:
                    return self._reject_write(
                        uow,
                        self._result("listed", seller_id, item_count),
                        "xianshi_plan_listing_stock_write_rejected",
                        f"{seller_id}:{goods_id}",
                    )
            for listing_id in listing_ids:
                saved_listing = uow.query_one(
                    "SELECT id,user_id,goods_id,name,type,price,quantity FROM xianshi_item WHERE id=?",
                    (listing_id,),
                )
                if saved_listing != listing_expected[listing_id]:
                    return self._reject_write(
                        uow,
                        self._result("listed", seller_id, item_count),
                        "xianshi_plan_listing_item_write_rejected",
                        listing_id,
                    )
            saved_stone = uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id=?", (seller_id,)
            )
            if saved_stone is None or require_exact_integer(saved_stone["stone"], field="stone", subject="seller") != expected_stone:
                return self._reject_write(
                    uow,
                    self._result("listed", seller_id, item_count),
                    "xianshi_plan_listing_stone_write_rejected",
                    seller_id,
                )
            saved_stamina = uow.query_one(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (seller_id,)
            )
            if saved_stamina is None or int(saved_stamina["user_stamina"] or 0) != expected_stamina:
                return self._reject_write(
                    uow,
                    self._result("listed", seller_id, item_count),
                    "xianshi_plan_listing_stamina_write_rejected",
                    seller_id,
                )
            self._release_savepoint(uow)
            return self._result(
                "listed", seller_id, item_count, listed, fee, stamina_cost
            )


__all__ = ["XianshiPlanListingResult", "XianshiPlanListingSqlRepository"]
