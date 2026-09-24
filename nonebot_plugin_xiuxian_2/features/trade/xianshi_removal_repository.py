from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class XianshiRemovalResult:
    status: str
    listing_id: str
    seller_id: str = ""
    goods_id: int = 0
    name: str = ""
    goods_type: str = ""
    refunded_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"removed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "removed"


@dataclass(frozen=True)
class XianshiNameRemovalResult:
    status: str
    seller_id: str
    item_name: str
    requested_quantity: int
    removed_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"removed", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "removed"


@dataclass(frozen=True)
class XianshiClearResult:
    status: str
    listing_count: int = 0
    refunded_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"cleared", "duplicate", "empty"}

    @property
    def applied(self) -> bool:
        return self.status == "cleared"


class XianshiRemovalSqlRepository:
    """Feature-owned atomic removal/refund transactions for xianshi listings."""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()

    def _refund(
        self,
        uow: DatabaseUnitOfWork,
        *,
        seller_id: str,
        goods_id: int,
        name: str,
        goods_type: str,
        quantity: int,
        max_goods_num: int,
    ) -> bool:
        if quantity <= 0:
            return True
        current = uow.query_one(
            "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
            "WHERE user_id=? AND goods_id=?",
            (seller_id, goods_id),
        )
        current_quantity = int(current["goods_num"]) if current else 0
        if current_quantity + quantity > max_goods_num:
            return False
        now = self.clock.now()
        uow.execute(
            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
            "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,0) "
            "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
            "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
            "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num,"
            "update_time=excluded.update_time",
            (
                seller_id,
                goods_id,
                name,
                goods_type,
                quantity,
                now,
                now,
            ),
        )
        return True

    def remove_listing(
        self,
        operation_id: str,
        listing_id: str,
        *,
        max_goods_num: int,
    ) -> XianshiRemovalResult:
        operation_id = str(operation_id).strip()
        listing_id = str(listing_id).strip()
        max_goods_num = max(int(max_goods_num), 1)
        if not operation_id or not listing_id:
            raise ValueError("operation_id and listing_id are required")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT listing_id,seller_id,goods_id,name,goods_type,refunded_quantity "
                "FROM xianshi_removal_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["listing_id"]) != listing_id:
                    return XianshiRemovalResult("state_changed", listing_id)
                return XianshiRemovalResult(
                    "duplicate",
                    str(previous["listing_id"]),
                    str(previous["seller_id"]),
                    int(previous["goods_id"]),
                    str(previous["name"]),
                    str(previous["goods_type"]),
                    int(previous["refunded_quantity"]),
                )

            listing = uow.query_one(
                "SELECT user_id,goods_id,name,type,quantity FROM xianshi_item WHERE id=?",
                (listing_id,),
            )
            if listing is None:
                return XianshiRemovalResult("listing_missing", listing_id)

            seller_id = str(listing["user_id"])
            goods_id = int(listing["goods_id"])
            name = str(listing["name"])
            goods_type = str(listing["type"])
            quantity = int(listing["quantity"])
            refunded_quantity = 0 if seller_id == "0" else max(quantity, 0)
            if not self._refund(
                uow,
                seller_id=seller_id,
                goods_id=goods_id,
                name=name,
                goods_type=goods_type,
                quantity=refunded_quantity,
                max_goods_num=max_goods_num,
            ):
                return XianshiRemovalResult(
                    "inventory_full", listing_id, seller_id, goods_id, name,
                    goods_type, refunded_quantity,
                )

            uow.execute("DELETE FROM xianshi_item WHERE id=?", (listing_id,))
            uow.execute(
                "INSERT INTO xianshi_removal_operations "
                "(operation_id,listing_id,seller_id,goods_id,name,goods_type,refunded_quantity) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    operation_id,
                    listing_id,
                    seller_id,
                    goods_id,
                    name,
                    goods_type,
                    refunded_quantity,
                ),
            )
            return XianshiRemovalResult(
                "removed", listing_id, seller_id, goods_id, name, goods_type,
                refunded_quantity,
            )

    def clear_all(
        self,
        operation_id: str,
        *,
        max_goods_num: int,
    ) -> XianshiClearResult:
        operation_id = str(operation_id).strip()
        max_goods_num = max(int(max_goods_num), 1)
        if not operation_id:
            raise ValueError("operation_id must not be empty")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT listing_count,refunded_quantity FROM xianshi_clear_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return XianshiClearResult(
                    "duplicate", int(previous["listing_count"]),
                    int(previous["refunded_quantity"]),
                )

            listings = uow.query_all(
                "SELECT id,user_id,goods_id,name,type,quantity FROM xianshi_item"
            )
            if not listings:
                return XianshiClearResult("empty")

            refunds: dict[tuple[str, int, str, str], int] = {}
            for listing in listings:
                seller_id = str(listing["user_id"])
                quantity = int(listing["quantity"])
                if seller_id == "0" or quantity <= 0:
                    continue
                key = (
                    seller_id,
                    int(listing["goods_id"]),
                    str(listing["name"]),
                    str(listing["type"]),
                )
                refunds[key] = refunds.get(key, 0) + quantity

            capacity_totals: dict[tuple[str, int], int] = {}
            for (seller_id, goods_id, _name, _goods_type), quantity in refunds.items():
                key = (seller_id, goods_id)
                capacity_totals[key] = capacity_totals.get(key, 0) + quantity

            for (seller_id, goods_id), quantity in capacity_totals.items():
                current = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (seller_id, goods_id),
                )
                current_quantity = int(current["goods_num"]) if current else 0
                if current_quantity + quantity > max_goods_num:
                    return XianshiClearResult(
                        "inventory_full", len(listings), sum(refunds.values())
                    )

            for (seller_id, goods_id, name, goods_type), quantity in refunds.items():
                if not self._refund(
                    uow,
                    seller_id=seller_id,
                    goods_id=goods_id,
                    name=name,
                    goods_type=goods_type,
                    quantity=quantity,
                    max_goods_num=max_goods_num,
                ):
                    raise RuntimeError("xianshi clear inventory snapshot changed")

            listing_count = len(listings)
            refunded_quantity = sum(refunds.values())
            uow.execute("DELETE FROM xianshi_item")
            uow.execute(
                "INSERT INTO xianshi_clear_operations "
                "(operation_id,listing_count,refunded_quantity) VALUES(?,?,?)",
                (operation_id, listing_count, refunded_quantity),
            )
            return XianshiClearResult("cleared", listing_count, refunded_quantity)

    def remove_by_name(
        self,
        operation_id: str,
        seller_id: str,
        item_name: str,
        quantity: int,
        *,
        max_goods_num: int,
    ) -> XianshiNameRemovalResult:
        operation_id = str(operation_id).strip()
        seller_id = str(seller_id)
        item_name = str(item_name).strip()
        quantity = int(quantity)
        max_goods_num = max(int(max_goods_num), 1)
        if not operation_id or not item_name:
            raise ValueError("operation_id and item_name are required")
        if quantity <= 0:
            raise ValueError("quantity must be positive")

        def result(status: str, removed_quantity: int = 0) -> XianshiNameRemovalResult:
            return XianshiNameRemovalResult(
                status, seller_id, item_name, quantity, int(removed_quantity)
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT seller_id,item_name,requested_quantity,removed_quantity "
                "FROM xianshi_name_removal_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                same_request = (
                    str(previous["seller_id"]) == seller_id
                    and str(previous["item_name"]) == item_name
                    and int(previous["requested_quantity"]) == quantity
                )
                if not same_request:
                    return result("state_changed")
                return result("duplicate", int(previous["removed_quantity"]))

            listings = uow.query_all(
                "SELECT id,goods_id,name,type,price,quantity FROM xianshi_item "
                "WHERE user_id=? AND name=? AND quantity<>-1 ORDER BY price ASC,id ASC",
                (seller_id, item_name),
            )
            if not listings:
                return result("listing_missing")
            total_available = sum(max(int(row["quantity"]), 0) for row in listings)
            if total_available <= 0:
                return result("listing_missing")
            remove_quantity = min(quantity, total_available)
            goods_ids = {int(row["goods_id"]) for row in listings}
            goods_types = {str(row["type"]) for row in listings}
            if len(goods_ids) != 1 or len(goods_types) != 1:
                return result("listing_conflict")
            goods_id = goods_ids.pop()
            goods_type = goods_types.pop()
            if not self._refund(
                uow,
                seller_id=seller_id,
                goods_id=goods_id,
                name=item_name,
                goods_type=goods_type,
                quantity=remove_quantity,
                max_goods_num=max_goods_num,
            ):
                return result("inventory_full", remove_quantity)

            left = remove_quantity
            for listing in listings:
                if left <= 0:
                    break
                listing_quantity = int(listing["quantity"])
                take = min(left, listing_quantity)
                if take == listing_quantity:
                    uow.execute("DELETE FROM xianshi_item WHERE id=?", (str(listing["id"]),))
                else:
                    uow.execute(
                        "UPDATE xianshi_item SET quantity=quantity-? WHERE id=?",
                        (take, str(listing["id"])),
                    )
                left -= take

            uow.execute(
                "INSERT INTO xianshi_name_removal_operations "
                "(operation_id,seller_id,item_name,requested_quantity,removed_quantity) "
                "VALUES(?,?,?,?,?)",
                (operation_id, seller_id, item_name, quantity, remove_quantity),
            )
            return result("removed", remove_quantity)


__all__ = [
    "XianshiClearResult",
    "XianshiNameRemovalResult",
    "XianshiRemovalResult",
    "XianshiRemovalSqlRepository",
]
