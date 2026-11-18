from __future__ import annotations

from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer
from ...core.errors import DomainError
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, Mapping

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...core.result import OperationOutcome


PurchaseStatus = Literal[
    "purchased",
    "duplicate",
    "listing_missing",
    "self_purchase",
    "stock_insufficient",
    "buyer_missing",
    "seller_missing",
    "stone_insufficient",
    "stamina_insufficient",
    "inventory_full",
    "state_changed",
]


@dataclass(frozen=True)
class XianshiPurchaseResult:
    status: PurchaseStatus
    listing_id: str
    buyer_id: str
    seller_id: str = ""
    goods_id: int = 0
    name: str = ""
    goods_type: str = ""
    quantity: int = 0
    total_cost: int = 0
    stamina_charged: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"purchased", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "purchased"


class XianshiPurchaseSqlRepository:
    """Feature-owned atomic purchase transaction for Xianshi listings."""

    def __init__(self, database: str | Path, *, clock: object | None = None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()

    def fast_plan(self, operation_id, buyer_id, names, quantities, listings):
        """Freeze candidate identities before buying so a retry cannot buy later stock."""
        buyer_id = str(buyer_id)
        payload = {"buyer_id": buyer_id, "names": names, "quantities": quantities}
        ledger, action = OperationLedger(clock=self.clock), "trade.fast_purchase.plan"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = ledger.begin(uow, operation_id, action, payload)
            if previous is not None and previous.outcome() is not None:
                return previous.outcome().data["listings"]
            # Receipts made before this fix remain authoritative on their first replay.
            completed = uow.query_all(
                "SELECT listing_id AS id,seller_id AS user_id,goods_id,name,goods_type AS type,"
                "total_cost,quantity FROM xianshi_operations "
                "WHERE buyer_id=? AND stamina_operation_id=? ORDER BY operation_id",
                (buyer_id, operation_id),
            )
            for row in completed:
                row["price"] = as_int_like(row.pop("total_cost")) // int(row["quantity"])
            frozen = {str(row["id"]): row for row in completed}
            frozen.update({str(row["id"]): {**row, "price": require_exact_integer(row["price"], field="listing_price", subject=str(row["id"]))} for row in listings if str(row["id"]) not in frozen})
            candidates = sorted(
                (dict(row) for row in frozen.values() if str(row["user_id"]) not in {buyer_id, "0"} and row["name"] in names),
                key=lambda row: (as_int_like(row["price"]), str(row["id"])),
            )
            ledger.finish(uow, OperationOutcome.applied(operation_id, action, data={"listings": candidates}))
            saved = ledger.get(uow, operation_id, action)
            if saved is None or saved.outcome() is None:
                raise RuntimeError("fast purchase candidate receipt was not saved")
            return candidates

    @staticmethod
    def _consume_stamina(uow: DatabaseUnitOfWork, user_id: str, cost: int) -> bool:
        if cost <= 0:
            return True
        return (
            uow.execute(
                "UPDATE user_xiuxian SET user_stamina=COALESCE(user_stamina,0)-? "
                "WHERE user_id=? AND COALESCE(user_stamina,0)>=?",
                (cost, user_id, cost),
            ).rowcount
            == 1
        )

    @staticmethod
    def _release_purchase_savepoint(uow: DatabaseUnitOfWork) -> None:
        uow.execute('RELEASE SAVEPOINT "xianshi_purchase"')

    @classmethod
    def _reject_purchase_write(
        cls,
        uow: DatabaseUnitOfWork,
        result: XianshiPurchaseResult,
        reason: str,
        subject: str,
    ) -> XianshiPurchaseResult:
        uow.execute('ROLLBACK TO SAVEPOINT "xianshi_purchase"')
        cls._release_purchase_savepoint(uow)
        return replace(result, status="state_changed", reason=reason, subject=subject)

    @staticmethod
    def _from_row(row: Mapping[str, object], status: PurchaseStatus) -> XianshiPurchaseResult:
        return XianshiPurchaseResult(
            status=status,
            listing_id=str(row["listing_id"]),
            buyer_id=str(row["buyer_id"]),
            seller_id=str(row["seller_id"]),
            goods_id=int(row["goods_id"]),
            name=str(row["name"]),
            goods_type=str(row["goods_type"]),
            quantity=int(row["quantity"]),
            total_cost=require_exact_integer(row["total_cost"], field="purchase_receipt_cost", subject=str(row["buyer_id"])),
            stamina_charged=int(row["stamina_charged"]),
        )

    def purchase(
        self,
        operation_id: str,
        buyer_id: str,
        listing_id: str,
        quantity: int,
        *,
        max_goods_num: int,
        stamina_operation_id: str | None = None,
        stamina_cost: int = 0,
    ) -> XianshiPurchaseResult:
        operation_id = str(operation_id).strip()
        buyer_id = str(buyer_id)
        listing_id = str(listing_id)
        quantity = max(1, int(quantity))
        stamina_operation_id = str(stamina_operation_id or "").strip()
        stamina_cost = int(stamina_cost)
        max_goods_num = max(1, int(max_goods_num))
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if stamina_cost < 0 or bool(stamina_operation_id) != (stamina_cost > 0):
            raise ValueError("stamina operation and positive cost must be provided together")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            existing = uow.query_one(
                "SELECT listing_id,buyer_id,seller_id,goods_id,name,goods_type,quantity,total_cost,"
                "stamina_operation_id,stamina_cost,stamina_charged "
                "FROM xianshi_operations WHERE operation_id=?",
                (operation_id,),
            )
            if existing is not None:
                if (
                    str(existing["listing_id"]) != listing_id
                    or str(existing["buyer_id"]) != buyer_id
                    or int(existing["quantity"]) != quantity
                    or str(existing["stamina_operation_id"]) != stamina_operation_id
                    or int(existing["stamina_cost"]) != stamina_cost
                ):
                    return XianshiPurchaseResult("state_changed", listing_id, buyer_id, reason="operation_conflict")
                return self._from_row(existing, "duplicate")

            listing = uow.query_one(
                "SELECT user_id,goods_id,name,type,price,quantity "
                "FROM xianshi_item WHERE id=?",
                (listing_id,),
            )
            if listing is None:
                return XianshiPurchaseResult("listing_missing", listing_id, buyer_id)

            seller_id = str(listing["user_id"])
            goods_id = int(listing["goods_id"])
            name = str(listing["name"])
            goods_type = str(listing["type"])
            price = require_exact_integer(listing["price"], field="listing_price", subject=listing_id)
            if price < 0:
                raise DomainError("currency_invalid_price", "listing price is negative", {"reason": "listing_price_negative", "subject": listing_id})
            stock = int(listing["quantity"])
            total_cost = price * quantity
            result = XianshiPurchaseResult(
                "purchased", listing_id, buyer_id, seller_id, goods_id,
                name, goods_type, quantity, total_cost,
            )

            if seller_id == buyer_id:
                return replace(result, status="self_purchase")
            if stock != -1 and stock < quantity:
                return replace(result, status="stock_insufficient")

            buyers = uow.query_all(
                "SELECT stone FROM user_xiuxian WHERE user_id=?", (buyer_id,)
            )
            if not buyers:
                return replace(result, status="buyer_missing")
            if len(buyers) != 1:
                return replace(result, status="state_changed", reason="account_rows_inconsistent", subject="buyer")
            buyer_stone = require_exact_integer(buyers[0]["stone"], field="stone", subject="buyer")
            if buyer_stone < total_cost:
                return replace(result, status="stone_insufficient")

            seller_stone = 0
            if seller_id != "0":
                sellers = uow.query_all("SELECT stone FROM user_xiuxian WHERE user_id=?", (seller_id,))
                if not sellers:
                    return replace(result, status="seller_missing")
                if len(sellers) != 1:
                    return replace(result, status="state_changed", reason="account_rows_inconsistent", subject="seller")
                seller_stone = require_exact_integer(sellers[0]["stone"], field="stone", subject="seller")

            inventory = uow.query_one(
                "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                (buyer_id, goods_id),
            )
            current_quantity = int(inventory["goods_num"] or 0) if inventory else 0
            if current_quantity + quantity > max_goods_num:
                return replace(result, status="inventory_full")

            uow.execute('SAVEPOINT "xianshi_purchase"')
            if stamina_operation_id:
                stamina_operation = uow.query_one(
                    "SELECT buyer_id,stamina_cost FROM xianshi_stamina_operations "
                    "WHERE operation_id=?",
                    (stamina_operation_id,),
                )
                if stamina_operation is not None:
                    if (
                        str(stamina_operation["buyer_id"]) != buyer_id
                        or int(stamina_operation["stamina_cost"]) != stamina_cost
                    ):
                        self._release_purchase_savepoint(uow)
                        return replace(result, status="state_changed", reason="operation_conflict")
                else:
                    stamina_before = uow.query_one(
                        "SELECT user_stamina FROM user_xiuxian WHERE user_id=?",
                        (buyer_id,),
                    )
                    expected_stamina = (
                        int(stamina_before["user_stamina"] or 0) - stamina_cost
                        if stamina_before is not None else None
                    )
                    if not self._consume_stamina(uow, buyer_id, stamina_cost):
                        self._release_purchase_savepoint(uow)
                        return replace(result, status="stamina_insufficient")
                    saved_stamina = uow.query_one(
                        "SELECT user_stamina FROM user_xiuxian WHERE user_id=?",
                        (buyer_id,),
                    )
                    if (
                        saved_stamina is None
                        or expected_stamina is None
                        or int(saved_stamina["user_stamina"] or 0) != expected_stamina
                    ):
                        return self._reject_purchase_write(
                            uow, result,
                            "xianshi_purchase_stamina_write_rejected", buyer_id,
                        )
                    uow.execute(
                        "INSERT INTO xianshi_stamina_operations "
                        "(operation_id,buyer_id,stamina_cost) VALUES(?,?,?)",
                        (stamina_operation_id, buyer_id, stamina_cost),
                    )
                    saved_stamina_receipt = uow.query_one(
                        "SELECT buyer_id,stamina_cost FROM xianshi_stamina_operations "
                        "WHERE operation_id=?",
                        (stamina_operation_id,),
                    )
                    if saved_stamina_receipt != {
                        "buyer_id": buyer_id,
                        "stamina_cost": stamina_cost,
                    }:
                        return self._reject_purchase_write(
                            uow, result,
                            "xianshi_purchase_stamina_receipt_write_rejected",
                            stamina_operation_id,
                        )
                    result = replace(result, stamina_charged=stamina_cost)

            expected_wallets = {buyer_id: buyer_stone - total_cost}
            if seller_id != "0":
                expected_wallets[seller_id] = seller_stone + total_cost
            for uid, after in expected_wallets.items():
                before = buyer_stone if uid == buyer_id else seller_stone
                subject = "buyer" if uid == buyer_id else "seller"
                changed = uow.execute(
                    "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? "
                    "AND xiuxian_int_cmp(COALESCE(stone,0),?,1,'stone','actor')=0",
                    (bind_sqlite_param(after, exact=True), uid, bind_sqlite_param(before, exact=True)),
                )
                saved = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (uid,))
                if changed.rowcount != 1 or saved is None or require_exact_integer(saved["stone"], field="stone", subject=subject) != after:
                    return self._reject_purchase_write(uow, result, "stone_write_rejected", subject)

            expected_listing_quantity = None if stock == quantity else (stock - quantity if stock != -1 else stock)
            if stock != -1:
                if stock == quantity:
                    changed = uow.execute("DELETE FROM xianshi_item WHERE id=?", (listing_id,))
                    saved_listing = uow.query_one(
                        "SELECT id FROM xianshi_item WHERE id=?", (listing_id,)
                    )
                else:
                    changed = uow.execute(
                        "UPDATE xianshi_item SET quantity=quantity-? WHERE id=?",
                        (quantity, listing_id),
                    )
                    saved_listing = uow.query_one(
                        "SELECT quantity FROM xianshi_item WHERE id=?", (listing_id,)
                    )
                if (
                    changed.rowcount != 1
                    or (expected_listing_quantity is None and saved_listing is not None)
                    or (expected_listing_quantity is not None and (
                        saved_listing is None
                        or int(saved_listing["quantity"]) != expected_listing_quantity
                    ))
                ):
                    return self._reject_purchase_write(
                        uow, result, "xianshi_purchase_listing_write_rejected", listing_id
                    )

            now = self.clock.now()
            inventory_write = uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num,"
                "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                "update_time=excluded.update_time",
                (buyer_id, goods_id, name, goods_type, quantity, now, now, quantity),
            )
            expected_inventory = {
                "goods_name": name,
                "goods_type": goods_type,
                "goods_num": current_quantity + quantity,
                "bind_num": int((inventory or {}).get("bind_num") or 0) + quantity,
            }
            saved_inventory = uow.query_one(
                "SELECT goods_name,goods_type,goods_num,bind_num FROM back "
                "WHERE user_id=? AND goods_id=?",
                (buyer_id, goods_id),
            )
            if saved_inventory is not None:
                saved_inventory = {
                    "goods_name": str(saved_inventory["goods_name"]),
                    "goods_type": str(saved_inventory["goods_type"]),
                    "goods_num": int(saved_inventory["goods_num"] or 0),
                    "bind_num": int(saved_inventory["bind_num"] or 0),
                }
            if inventory_write.rowcount != 1 or saved_inventory != expected_inventory:
                return self._reject_purchase_write(
                    uow, result,
                    "xianshi_purchase_inventory_write_rejected",
                    f"{buyer_id}:{goods_id}",
                )

            receipt_write = uow.execute(
                "INSERT INTO xianshi_operations(operation_id,listing_id,buyer_id,seller_id,goods_id,"
                "name,goods_type,quantity,total_cost,stamina_operation_id,stamina_cost,stamina_charged) "
                "VALUES(?,?,?,?,?,?,?,?,xiuxian_int_store(?,'stone','actor'),?,?,?)",
                (
                    operation_id, listing_id, buyer_id, seller_id, goods_id, name,
                    goods_type, quantity, bind_sqlite_param(total_cost, exact=True), stamina_operation_id,
                    stamina_cost, result.stamina_charged,
                ),
            )
            saved_receipt = uow.query_one(
                "SELECT operation_id,listing_id,buyer_id,seller_id,goods_id,name,goods_type,"
                "quantity,total_cost,stamina_operation_id,stamina_cost,stamina_charged "
                "FROM xianshi_operations WHERE operation_id=?",
                (operation_id,),
            )
            expected_receipt = {
                "operation_id": operation_id,
                "listing_id": listing_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "goods_id": goods_id,
                "name": name,
                "goods_type": goods_type,
                "quantity": quantity,
                "total_cost": total_cost,
                "stamina_operation_id": stamina_operation_id,
                "stamina_cost": stamina_cost,
                "stamina_charged": result.stamina_charged,
            }
            if receipt_write.rowcount != 1 or saved_receipt != expected_receipt:
                return self._reject_purchase_write(
                    uow, result, "xianshi_purchase_receipt_write_rejected", operation_id
                )

            # Validate the complete projection after the final write as later triggers
            # may alter an earlier wallet, listing, or inventory row.
            for uid, expected in expected_wallets.items():
                saved = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (uid,))
                if saved is None or require_exact_integer(saved["stone"], field="stone", subject=uid) != expected:
                    return self._reject_purchase_write(
                        uow, result, "stone_write_rejected",
                        "buyer" if uid == buyer_id else "seller",
                    )
            if stamina_operation_id and result.stamina_charged:
                saved = uow.query_one("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (buyer_id,))
                if saved is None or int(saved["user_stamina"] or 0) != expected_stamina:
                    return self._reject_purchase_write(
                        uow, result, "xianshi_purchase_stamina_write_rejected", buyer_id
                    )
            self._release_purchase_savepoint(uow)
            return result


__all__ = ["PurchaseStatus", "XianshiPurchaseResult", "XianshiPurchaseSqlRepository"]
