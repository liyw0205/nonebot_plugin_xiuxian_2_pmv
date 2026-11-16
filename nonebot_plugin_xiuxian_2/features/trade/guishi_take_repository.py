from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import bind_sqlite_param, exact_crystal_integer
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_operation_replay import order_receipts


@dataclass(frozen=True)
class GuishiStoredItemTakeResult:
    status: str
    user_id: str
    goods_id: int
    item_name: str = ""
    goods_type: str = ""
    quantity: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"taken", "duplicate"}

    @property
    def applied(self) -> bool:
        return self.status == "taken"


class GuishiStoredItemTakeSqlRepository:
    """Move Guishi storage into bound inventory under one cross-db transaction."""

    def __init__(
        self,
        game_database: str | Path,
        trade_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.trade_database = str(trade_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _stored_items(value: Any, user_id: str) -> dict[str, Any]:
        def unique_pairs(pairs):
            result = {}
            for key, quantity in pairs:
                if key in result:
                    raise DomainError("guishi_storage_duplicate", "Duplicate stored item key", {"subject": user_id})
                result[key] = quantity
            return result

        try:
            items = json.loads(value if value is not None and value != "" else "{}", object_pairs_hook=unique_pairs)
        except (TypeError, ValueError) as exc:
            raise DomainError("guishi_storage_invalid", "Invalid stored items JSON", {"subject": user_id}) from exc
        if not isinstance(items, dict):
            raise DomainError("guishi_storage_invalid", "Stored items must be an object", {"subject": user_id})
        for key, quantity in items.items():
            count = exact_crystal_integer(quantity)
            if quantity is None or isinstance(quantity, bool) or count is None or count < 0:
                raise DomainError("guishi_storage_quantity_invalid", "Invalid stored item quantity", {"subject": f"item:{key}"})
        # Leave unrelated legacy values in their original representation.
        return items

    def take(
        self,
        *,
        operation_id: str,
        user_id: str,
        goods_id: int,
        item_name: str,
        goods_type: str,
        max_goods_num: int,
        take_open: bool = True,
    ) -> GuishiStoredItemTakeResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id).strip()
        goods_id = int(goods_id)
        item_name = str(item_name)
        goods_type = str(goods_type)
        max_goods_num = max(int(max_goods_num), 1)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not user_id or not item_name or not goods_type:
            raise ValueError("user_id, item_name, and goods_type are required")

        def reject(status, *, reason="", subject=user_id, quantity=0):
            return GuishiStoredItemTakeResult(status, user_id, goods_id, item_name, goods_type,
                                             quantity, reason or status, subject)

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.trade_database, "guishi_trade")
            receipts = order_receipts(uow, "guishi_take_item_operations", operation_id, legacy_parts=1)
            if len(receipts) > 1:
                return reject("operation_conflict")
            previous = receipts[0] if receipts else None
            if previous is not None:
                previous_goods_id = exact_crystal_integer(previous["goods_id"])
                previous_quantity = exact_crystal_integer(previous["quantity"])
                if previous_goods_id is None or previous_quantity is None or previous_quantity <= 0:
                    return reject("state_changed", reason="guishi_take_receipt_invalid")
                if (
                    str(previous["user_id"]) != user_id
                    or previous_goods_id != goods_id
                    or str(previous["item_name"]) != item_name
                    or str(previous["goods_type"]) != goods_type
                ):
                    return reject("operation_conflict")
                return GuishiStoredItemTakeResult(
                    "duplicate",
                    user_id,
                    goods_id,
                    str(previous["item_name"]),
                    str(previous["goods_type"]),
                    previous_quantity,
                )

            if not take_open:
                return reject("weekend_closed")
            account = uow.query_one(
                "SELECT stored_stone,items FROM guishi_trade.guishi_info WHERE user_id=?",
                (user_id,),
            )
            if account is None:
                return reject("user_missing")
            try:
                stored_items = self._stored_items(account["items"], user_id)
            except DomainError as exc:
                return reject("state_changed", reason=exc.code, subject=exc.details.get("subject", user_id))
            quantity = exact_crystal_integer(stored_items.get(str(goods_id), 0))
            if quantity <= 0:
                return reject("item_missing", subject=f"item:{goods_id}")

            inventory = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,"
                "COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, goods_id),
            )
            current_quantity = exact_crystal_integer(inventory["goods_num"]) if inventory else 0
            current_bound = exact_crystal_integer(inventory["bind_num"]) if inventory else 0
            if (current_quantity is None or current_bound is None or current_quantity < 0
                    or not 0 <= current_bound <= current_quantity):
                return reject("state_changed", reason="inventory_invalid", subject=f"item:{goods_id}")
            if current_quantity + quantity > max_goods_num:
                return reject("inventory_full", subject=f"item:{goods_id}", quantity=quantity)

            del stored_items[str(goods_id)]
            stored_json = json.dumps(stored_items, ensure_ascii=False, sort_keys=True)
            expected_inventory = dict(goods_name=item_name, goods_type=goods_type,
                                      goods_num=current_quantity + quantity, bind_num=current_bound + quantity)
            expected_receipt = dict(user_id=user_id, goods_id=goods_id, item_name=item_name,
                                    goods_type=goods_type, quantity=quantity)
            try:
                with uow.savepoint("guishi_take"):
                    removed = uow.execute(
                        "UPDATE guishi_trade.guishi_info SET items=? WHERE user_id=? AND items IS ?",
                        (stored_json, user_id, account["items"]),
                    )
                    if removed.rowcount != 1:
                        raise DomainError("guishi_take_storage_write_rejected", "Stored item removal rejected")
                    now = self.clock.now().isoformat()
                    granted = uow.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) "
                        "VALUES(?,?,?,?,xiuxian_int_store(?,'goods_num','actor'),?,?,xiuxian_int_store(?,'bind_num','actor')) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                        "goods_num=excluded.goods_num,bind_num=excluded.bind_num,update_time=excluded.update_time",
                        (user_id, goods_id, item_name, goods_type,
                         bind_sqlite_param(expected_inventory["goods_num"], exact=True), now, now,
                         bind_sqlite_param(expected_inventory["bind_num"], exact=True)),
                    )
                    if granted.rowcount != 1:
                        raise DomainError("guishi_take_inventory_write_rejected", "Inventory grant rejected")
                    receipt = uow.execute(
                        "INSERT INTO guishi_take_item_operations(operation_id,user_id,goods_id,"
                        "item_name,goods_type,quantity) VALUES(?,?,?,?,?,?)",
                        (operation_id, user_id, goods_id, item_name, goods_type, quantity),
                    )
                    saved_receipt = uow.query_one(
                        "SELECT user_id,goods_id,item_name,goods_type,quantity FROM guishi_take_item_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if receipt.rowcount != 1 or saved_receipt != expected_receipt:
                        raise DomainError("guishi_take_receipt_write_rejected", "Take receipt did not persist exactly")
                    # Read after all writes so later triggers cannot corrupt earlier projections.
                    saved_account = uow.query_one(
                        "SELECT stored_stone,items FROM guishi_trade.guishi_info WHERE user_id=?", (user_id,),
                    )
                    if saved_account != dict(stored_stone=account["stored_stone"], items=stored_json):
                        raise DomainError("guishi_take_storage_write_rejected", "Stored item removal did not persist exactly")
                    saved_inventory = uow.query_one(
                        "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, goods_id),
                    )
                    if saved_inventory is not None:
                        for column in ("goods_num", "bind_num"):
                            saved_inventory[column] = exact_crystal_integer(saved_inventory[column])
                    if saved_inventory != expected_inventory:
                        raise DomainError("guishi_take_inventory_write_rejected", "Inventory grant did not persist exactly")
            except DomainError as exc:
                return reject("state_changed", reason=exc.details.get("reason") or exc.code,
                              subject=exc.details.get("subject") or user_id, quantity=quantity)
            return GuishiStoredItemTakeResult(
                "taken", user_id, goods_id, item_name, goods_type, quantity
            )


__all__ = ["GuishiStoredItemTakeResult", "GuishiStoredItemTakeSqlRepository"]
