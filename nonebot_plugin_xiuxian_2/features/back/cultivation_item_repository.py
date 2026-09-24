from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class CultivationItemResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    exp_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class CultivationItemSqlRepository:
    """Atomically consume an item and apply cultivation gains."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        exp_gain: int,
        *,
        hp_gain: int,
        mp_gain: int,
        atk_gain: int,
        power_multiplier: float,
        track_usage: bool = False,
    ) -> CultivationItemResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        exp_gain = int(exp_gain)
        hp_gain = int(hp_gain)
        mp_gain = int(mp_gain)
        atk_gain = int(atk_gain)
        power_multiplier = float(power_multiplier)
        track_usage = bool(track_usage)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0 or min(exp_gain, hp_gain, mp_gain, atk_gain) < 0:
            raise ValueError("quantity and gains must be non-negative")
        if power_multiplier < 0:
            raise ValueError("power_multiplier must be non-negative")

        def result(status: str, result_quantity: int = quantity, result_exp: int = exp_gain) -> CultivationItemResult:
            return CultivationItemResult(status, user_id, item_id, int(result_quantity), int(result_exp))

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity, exp_gain FROM cultivation_item_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result("duplicate", int(previous["quantity"]), int(previous["exp_gain"]))

            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
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
            if track_usage and "day_num" in columns:
                updates.append("day_num=COALESCE(day_num, 0)+?")
                params.append(quantity)
            if track_usage and "all_num" in columns:
                updates.append("all_num=COALESCE(all_num, 0)+?")
                params.append(quantity)
            if "bind_num" in columns:
                updates.append(
                    "bind_num=CASE WHEN goods_num-?=0 THEN 0 "
                    "WHEN COALESCE(bind_num, 0)>=? THEN COALESCE(bind_num, 0)-? "
                    "ELSE MIN(COALESCE(bind_num, 0), goods_num-?) END"
                )
                params.extend((quantity, quantity, quantity, quantity))
            for column in ("update_time", "action_time"):
                if column in columns:
                    updates.append(f"{column}=CURRENT_TIMESTAMP")
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(updates)} "
                "WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (*params, user_id, item_id, quantity),
            )
            updated = uow.execute(
                "UPDATE user_xiuxian SET "
                "exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL), "
                "hp=CAST(COALESCE(hp,0) AS REAL)+CAST(? AS REAL), "
                "mp=CAST(COALESCE(mp,0) AS REAL)+CAST(? AS REAL), "
                "atk=CAST(COALESCE(atk,0) AS REAL)+CAST(? AS REAL), "
                "power=ROUND((COALESCE(exp,0)+?)*?, 0) WHERE user_id=?",
                (exp_gain, hp_gain, mp_gain, atk_gain, exp_gain, power_multiplier, user_id),
            )
            if consumed.rowcount != 1 or updated.rowcount != 1:
                return result("state_changed")
            uow.execute(
                "INSERT INTO cultivation_item_operations "
                "(operation_id,user_id,item_id,quantity,exp_gain,hp_gain,mp_gain,atk_gain,power_multiplier) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    item_id,
                    quantity,
                    exp_gain,
                    hp_gain,
                    mp_gain,
                    atk_gain,
                    power_multiplier,
                ),
            )
            return result("applied")


__all__ = ["CultivationItemResult", "CultivationItemSqlRepository"]
