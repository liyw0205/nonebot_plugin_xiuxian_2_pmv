from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class BreakthroughRateItemResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    rate_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BreakthroughRateItemSqlRepository:
    """Atomically consume elixirs and increase breakthrough rate."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        rate_gain: int,
    ) -> BreakthroughRateItemResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        rate_gain = int(rate_gain)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0 or rate_gain < 0:
            raise ValueError("quantity must be positive and rate gain non-negative")

        def result(status: str, values=None) -> BreakthroughRateItemResult:
            values = values or (quantity, rate_gain)
            return BreakthroughRateItemResult(
                status, user_id, item_id, int(values[0]), int(values[1])
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity,rate_gain FROM breakthrough_rate_item_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result("duplicate", (previous["quantity"], previous["rate_gain"]))

            if uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return result("user_missing")
            item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None or int(item["goods_num"] or 0) < quantity:
                return result("item_insufficient")

            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            updates = ["goods_num=goods_num-?"]
            params: list[object] = [quantity]
            if "day_num" in columns:
                updates.append("day_num=COALESCE(day_num, 0)+?")
                params.append(quantity)
            if "all_num" in columns:
                updates.append("all_num=COALESCE(all_num, 0)+?")
                params.append(quantity)
            if "bind_num" in columns:
                updates.append(
                    "bind_num=CASE WHEN goods_num-?=0 THEN 0 "
                    "WHEN COALESCE(bind_num, 0)>=? THEN COALESCE(bind_num, 0)-? "
                    "ELSE MIN(COALESCE(bind_num, 0), goods_num-?) END"
                )
                params.extend((quantity, quantity, quantity, quantity))
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(updates)} "
                "WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (*params, user_id, item_id, quantity),
            )
            updated = uow.execute(
                "UPDATE user_xiuxian SET level_up_rate=COALESCE(level_up_rate, 0)+? "
                "WHERE user_id=?",
                (rate_gain, user_id),
            )
            if consumed.rowcount != 1 or updated.rowcount != 1:
                return result("state_changed")
            uow.execute(
                "INSERT INTO breakthrough_rate_item_operations "
                "(operation_id,user_id,item_id,quantity,rate_gain) VALUES(?,?,?,?,?)",
                (operation_id, user_id, item_id, quantity, rate_gain),
            )
            return result("applied")


__all__ = ["BreakthroughRateItemResult", "BreakthroughRateItemSqlRepository"]
