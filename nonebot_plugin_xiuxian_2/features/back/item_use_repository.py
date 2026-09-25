from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ItemUseResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    @property
    def ok(self) -> bool:
        return self.succeeded

    @property
    def code(self) -> str | None:
        return None if self.succeeded else self.status

    @property
    def message(self) -> str:
        return "" if self.succeeded else f"物品使用未完成：{self.status}。"

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "user_id": self.user_id,
            "item_id": self.item_id,
            "quantity": self.quantity,
            "item_remaining": self.item_remaining,
        }


class ItemUseSqlRepository:
    """Consume one ordinary backpack item batch without owning its effect."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _payload(user_id: str, item_id: int, quantity: int, expected_item_count: int | None) -> str:
        return json.dumps(
            [str(user_id), int(item_id), int(quantity), expected_item_count],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(status: str, user_id: str, item_id: int, quantity: int, remaining: int = 0) -> ItemUseResult:
        return ItemUseResult(status, str(user_id), int(item_id), int(quantity), int(remaining))

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        expected_item_count: int | None = None,
    ) -> ItemUseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        quantity = int(quantity)
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        if item_id <= 0 or quantity <= 0:
            raise ValueError("item_id and quantity must be positive")
        if expected_item_count is not None:
            expected_item_count = int(expected_item_count)
            if expected_item_count < quantity:
                raise ValueError("expected_item_count must cover quantity")
        payload = self._payload(user_id, item_id, quantity, expected_item_count)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT payload, user_id, item_id, quantity, item_remaining "
                "FROM back_item_use_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return self._result("operation_conflict", user_id, item_id, quantity)
                return self._result(
                    "duplicate",
                    str(previous["user_id"]),
                    int(previous["item_id"]),
                    int(previous["quantity"]),
                    int(previous["item_remaining"]),
                )

            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            if not columns:
                return self._result("item_missing", user_id, item_id, quantity)
            state_expression = "COALESCE(state, 0)" if "state" in columns else "0"
            row = uow.query_one(
                f"SELECT COALESCE(goods_num,0) AS goods_num, {state_expression} AS state "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if row is None:
                return self._result("item_missing", user_id, item_id, quantity)
            available = max(0, int(row["goods_num"] or 0) - int(row["state"] or 0))
            if available < quantity:
                return self._result("item_insufficient", user_id, item_id, quantity, available)
            if expected_item_count is not None and available != expected_item_count:
                return self._result("state_changed", user_id, item_id, quantity, available)

            updates = ["goods_num=goods_num-?"]
            params: list[object] = [quantity]
            if "bind_num" in columns:
                updates.append("bind_num=MIN(COALESCE(bind_num,0), goods_num-?)")
                params.append(quantity)
            for column in ("update_time", "action_time"):
                if column in columns:
                    updates.append(f"{column}=CURRENT_TIMESTAMP")
            updated = uow.execute(
                f"UPDATE back SET {', '.join(updates)} WHERE user_id=? AND goods_id=? "
                f"AND COALESCE(goods_num,0)-{state_expression}>=?",
                (*params, user_id, item_id, quantity),
            )
            if updated.rowcount != 1:
                return self._result("state_changed", user_id, item_id, quantity, available)
            remaining = available - quantity
            uow.execute(
                "INSERT INTO back_item_use_operations "
                "(operation_id,payload,user_id,item_id,quantity,item_remaining) "
                "VALUES(?,?,?,?,?,?)",
                (operation_id, payload, user_id, item_id, quantity, remaining),
            )
            return self._result("applied", user_id, item_id, quantity, remaining)


__all__ = ["ItemUseResult", "ItemUseSqlRepository"]
