from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class PermanentAtkItemResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    atk_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PermanentAtkItemSqlRepository:
    """Atomically consume attack elixirs and increase permanent attack."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        atk_gain: int,
    ) -> PermanentAtkItemResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        atk_gain = int(atk_gain)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0 or atk_gain < 0:
            raise ValueError("quantity must be positive and attack gain non-negative")

        def result(status: str, values=None) -> PermanentAtkItemResult:
            values = values or (quantity, atk_gain)
            return PermanentAtkItemResult(
                status, user_id, item_id, int(values[0]), int(values[1])
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity,atk_gain FROM permanent_atk_item_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result("duplicate", (previous["quantity"], previous["atk_gain"]))

            if uow.query_one(
                "SELECT 1 AS present FROM BuffInfo WHERE user_id=?", (user_id,)
            ) is None:
                return result("buff_missing")
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
                "UPDATE BuffInfo SET atk_buff=COALESCE(atk_buff, 0)+? "
                "WHERE user_id=?",
                (atk_gain, user_id),
            )
            if consumed.rowcount != 1 or updated.rowcount != 1:
                return result("state_changed")
            uow.execute(
                "INSERT INTO permanent_atk_item_operations "
                "(operation_id,user_id,item_id,quantity,atk_gain) VALUES(?,?,?,?,?)",
                (operation_id, user_id, item_id, quantity, atk_gain),
            )
            return result("applied")


__all__ = ["PermanentAtkItemResult", "PermanentAtkItemSqlRepository"]
