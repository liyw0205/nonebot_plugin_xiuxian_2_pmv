from __future__ import annotations

from ...core.numeric import bind_sqlite_param, require_exact_integer

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
    reason: str = ""
    subject: str = ""

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
        reason: str = "",
        subject: str = "",
    ) -> XianshiListingResult:
        return XianshiListingResult(
            status, operation_id, seller_id, goods_id, name, goods_type, price,
            quantity, listed_quantity, fee_charged, stamina_charged, reason, subject,
        )

    @staticmethod
    def _release_savepoint(uow: DatabaseUnitOfWork) -> None:
        uow.execute('RELEASE SAVEPOINT "xianshi_listing"')

    @classmethod
    def _reject_write(
        cls,
        uow: DatabaseUnitOfWork,
        result: XianshiListingResult,
        reason: str,
        subject: str,
    ) -> XianshiListingResult:
        uow.execute('ROLLBACK TO SAVEPOINT "xianshi_listing"')
        cls._release_savepoint(uow)
        return cls._result(
            "state_changed", result.operation_id, result.seller_id, result.goods_id,
            result.name, result.goods_type, result.price, result.requested_quantity,
            reason=reason, subject=subject,
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
                        reason="operation_conflict", subject=operation_id,
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
            wallet = require_exact_integer(player["stone"], field="stone", subject="seller")
            if wallet < fee:
                return self._result(
                    "stone_insufficient", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity,
                )
            stock_row = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num,"
                "COALESCE(state,0) AS state,"
                "COALESCE(goods_num,0)-COALESCE(bind_num,0)-COALESCE(state,0) AS tradeable "
                "FROM back WHERE user_id=? AND goods_id=?",
                (seller_id, goods_id),
            )
            if stock_row is None or int(stock_row["tradeable"] or 0) < quantity:
                return self._result(
                    "stock_insufficient", operation_id, seller_id, goods_id, name,
                    goods_type, price, quantity,
                )
            now = self.clock.now()
            uow.execute('SAVEPOINT "xianshi_listing"')
            expected_stamina = int(player["stamina"] or 0) - stamina_cost if stamina_cost else None
            if stamina_cost:
                stamina = uow.execute(
                    "UPDATE user_xiuxian SET user_stamina=COALESCE(user_stamina,0)-? "
                    "WHERE user_id=? AND COALESCE(user_stamina,0)>=?",
                    (stamina_cost, seller_id, stamina_cost),
                )
                saved_stamina = uow.query_one(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (seller_id,)
                )
                if (
                    stamina.rowcount != 1
                    or saved_stamina is None
                    or int(saved_stamina["user_stamina"] or 0) != expected_stamina
                ):
                    return self._reject_write(
                        uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                          goods_type, price, quantity),
                        "xianshi_listing_stamina_write_rejected", seller_id,
                    )
            changed_stone = uow.execute(
                "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') "
                "WHERE user_id=? AND xiuxian_int_cmp(COALESCE(stone,0),?,1,'stone','actor')=0",
                (bind_sqlite_param(wallet - fee, exact=True), seller_id, bind_sqlite_param(wallet, exact=True)),
            )
            if changed_stone.rowcount != 1:
                return self._reject_write(
                    uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                      goods_type, price, quantity),
                    "xianshi_listing_stone_write_rejected", seller_id,
                )
            saved = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (seller_id,))
            if saved is None or require_exact_integer(saved["stone"], field="stone", subject="seller") != wallet - fee:
                return self._reject_write(
                    uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                      goods_type, price, quantity),
                    "xianshi_listing_stone_write_rejected", seller_id,
                )
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
            saved_stock = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                "WHERE user_id=? AND goods_id=?",
                (seller_id, goods_id),
            )
            if (
                stock.rowcount != 1
                or saved_stock is None
                or int(saved_stock["goods_num"] or 0) != int(stock_row["goods_num"]) - quantity
            ):
                return self._reject_write(
                    uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                      goods_type, price, quantity),
                    "xianshi_listing_stock_write_rejected", f"{seller_id}:{goods_id}",
                )

            listing_ids = []
            for _ in range(quantity):
                for _attempt in range(20):
                    listing_id = self._listing_id(self.ids.new_id())
                    exists = uow.query_one(
                        "SELECT 1 AS present FROM xianshi_item WHERE id=?", (listing_id,)
                    )
                    if exists is not None:
                        continue
                    listing_write = uow.execute(
                        "INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity) "
                        "VALUES(?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),1)",
                        (listing_id, seller_id, goods_id, name, goods_type, bind_sqlite_param(price, exact=True)),
                    )
                    saved_listing = uow.query_one(
                        "SELECT id,user_id,goods_id,name,type,price,quantity FROM xianshi_item WHERE id=?",
                        (listing_id,),
                    )
                    if listing_write.rowcount != 1 or saved_listing != {
                        "id": listing_id,
                        "user_id": seller_id,
                        "goods_id": goods_id,
                        "name": name,
                        "type": goods_type,
                        "price": price,
                        "quantity": 1,
                    }:
                        return self._reject_write(
                            uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                              goods_type, price, quantity),
                            "xianshi_listing_item_write_rejected", listing_id,
                        )
                    listing_ids.append(listing_id)
                    break
                else:
                    raise RuntimeError("failed to allocate xianshi listing id")

            receipt_write = uow.execute(
                "INSERT INTO xianshi_listing_operations "
                "(operation_id,seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost) VALUES(?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),?,?,xiuxian_int_store(?,'stone','actor'),?)",
                (
                    operation_id, seller_id, goods_id, name, goods_type, bind_sqlite_param(price, exact=True),
                    quantity, quantity, bind_sqlite_param(fee, exact=True), stamina_cost,
                ),
            )
            saved_receipt = uow.query_one(
                "SELECT operation_id,seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost FROM xianshi_listing_operations WHERE operation_id=?",
                (operation_id,),
            )
            expected_receipt = {
                "operation_id": operation_id,
                "seller_id": seller_id,
                "goods_id": goods_id,
                "name": name,
                "goods_type": goods_type,
                "price": price,
                "requested_quantity": quantity,
                "listed_quantity": quantity,
                "fee_charged": fee,
                "stamina_cost": stamina_cost,
            }
            if receipt_write.rowcount != 1 or saved_receipt != expected_receipt:
                return self._reject_write(
                    uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                      goods_type, price, quantity),
                    "xianshi_listing_receipt_write_rejected", operation_id,
                )

            for listing_id in listing_ids:
                saved_listing = uow.query_one(
                    "SELECT id,user_id,goods_id,name,type,price,quantity FROM xianshi_item WHERE id=?",
                    (listing_id,),
                )
                if saved_listing != {
                    "id": listing_id,
                    "user_id": seller_id,
                    "goods_id": goods_id,
                    "name": name,
                    "type": goods_type,
                    "price": price,
                    "quantity": 1,
                }:
                    return self._reject_write(
                        uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                          goods_type, price, quantity),
                        "xianshi_listing_item_write_rejected", listing_id,
                    )
            saved = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (seller_id,))
            if saved is None or require_exact_integer(saved["stone"], field="stone", subject="seller") != wallet - fee:
                return self._reject_write(
                    uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                      goods_type, price, quantity),
                    "xianshi_listing_stone_write_rejected", seller_id,
                )
            if stamina_cost:
                saved_stamina = uow.query_one(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (seller_id,)
                )
                if saved_stamina is None or int(saved_stamina["user_stamina"] or 0) != expected_stamina:
                    return self._reject_write(
                        uow, self._result("listed", operation_id, seller_id, goods_id, name,
                                          goods_type, price, quantity),
                        "xianshi_listing_stamina_write_rejected", seller_id,
                    )
            self._release_savepoint(uow)
            return self._result(
                "listed", operation_id, seller_id, goods_id, name, goods_type,
                price, quantity, quantity, fee, stamina_cost,
            )

    def list_system_item(
        self,
        operation_id: str,
        goods_id: int,
        name: str,
        goods_type: str,
        price: int,
        quantity: int,
    ) -> XianshiListingResult:
        operation_id = str(operation_id).strip()
        goods_id, price, quantity = int(goods_id), int(price), int(quantity)
        name, goods_type = str(name), str(goods_type)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost FROM xianshi_listing_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                same_request = (
                    str(previous["seller_id"]) == "0"
                    and int(previous["goods_id"]) == goods_id
                    and str(previous["name"]) == name
                    and str(previous["goods_type"]) == goods_type
                    and int(previous["price"]) == price
                    and int(previous["requested_quantity"]) == quantity
                    and int(previous["stamina_cost"] or 0) == 0
                )
                if not same_request:
                    return self._result(
                        "state_changed", operation_id, "0", goods_id, name,
                        goods_type, price, quantity,
                        reason="operation_conflict", subject=operation_id,
                    )
                return self._result(
                    "duplicate", operation_id, "0", goods_id, name, goods_type,
                    price, quantity, int(previous["listed_quantity"]),
                    int(previous["fee_charged"]),
                )

            uow.execute('SAVEPOINT "xianshi_listing"')
            for _attempt in range(20):
                listing_id = self._listing_id(self.ids.new_id())
                exists = uow.query_one(
                    "SELECT 1 AS present FROM xianshi_item WHERE id=?", (listing_id,)
                )
                if exists is not None:
                    continue
                listing_write = uow.execute(
                    "INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity) "
                    "VALUES(?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),?)",
                    (listing_id, "0", goods_id, name, goods_type, bind_sqlite_param(price, exact=True), quantity),
                )
                saved_listing = uow.query_one(
                    "SELECT id,user_id,goods_id,name,type,price,quantity FROM xianshi_item WHERE id=?",
                    (listing_id,),
                )
                if listing_write.rowcount != 1 or saved_listing != {
                    "id": listing_id,
                    "user_id": "0",
                    "goods_id": goods_id,
                    "name": name,
                    "type": goods_type,
                    "price": price,
                    "quantity": quantity,
                }:
                    return self._reject_write(
                        uow, self._result("listed", operation_id, "0", goods_id, name,
                                          goods_type, price, quantity),
                        "xianshi_listing_item_write_rejected", listing_id,
                    )
                break
            else:
                raise RuntimeError("failed to allocate system xianshi listing id")

            receipt_write = uow.execute(
                "INSERT INTO xianshi_listing_operations "
                "(operation_id,seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost) VALUES(?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),?,?,xiuxian_int_store(?,'stone','actor'),0)",
                (
                    operation_id,
                    "0",
                    goods_id,
                    name,
                    goods_type,
                    price,
                    quantity,
                    quantity,
                    0,
                ),
            )
            saved_receipt = uow.query_one(
                "SELECT operation_id,seller_id,goods_id,name,goods_type,price,requested_quantity,"
                "listed_quantity,fee_charged,stamina_cost FROM xianshi_listing_operations WHERE operation_id=?",
                (operation_id,),
            )
            expected_receipt = {
                "operation_id": operation_id,
                "seller_id": "0",
                "goods_id": goods_id,
                "name": name,
                "goods_type": goods_type,
                "price": price,
                "requested_quantity": quantity,
                "listed_quantity": quantity,
                "fee_charged": 0,
                "stamina_cost": 0,
            }
            if receipt_write.rowcount != 1 or saved_receipt != expected_receipt:
                return self._reject_write(
                    uow, self._result("listed", operation_id, "0", goods_id, name,
                                      goods_type, price, quantity),
                    "xianshi_listing_receipt_write_rejected", operation_id,
                )
            self._release_savepoint(uow)
            return self._result(
                "listed", operation_id, "0", goods_id, name, goods_type, price,
                quantity, quantity, 0,
            )


__all__ = ["XianshiListingResult", "XianshiListingSqlRepository"]
