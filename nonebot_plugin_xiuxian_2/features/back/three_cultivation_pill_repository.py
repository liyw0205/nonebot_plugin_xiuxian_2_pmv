from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ThreeCultivationPillResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    requested_exp: int
    exp_gain: int
    hp_before: int
    hp_after: int
    mp_before: int
    mp_after: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ThreeCultivationPillSqlRepository:
    """Atomically consume pills and apply capped cultivation and recovery."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        requested_exp: int,
        *,
        max_exp: int,
        power_multiplier: float,
    ) -> ThreeCultivationPillResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        requested_exp = int(requested_exp)
        max_exp = int(max_exp)
        power_multiplier = float(power_multiplier)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0 or requested_exp < 0 or max_exp < 0:
            raise ValueError("quantity must be positive and experience non-negative")
        if power_multiplier < 0:
            raise ValueError("power_multiplier must be non-negative")

        def result(status: str, values=None) -> ThreeCultivationPillResult:
            if values is None:
                values = (quantity, requested_exp, 0, 0, 0, 0, 0)
            return ThreeCultivationPillResult(
                status,
                user_id,
                item_id,
                *(int(value) for value in values),
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity,requested_exp,exp_gain,hp_before,hp_after,mp_before,mp_after "
                "FROM three_cultivation_pill_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result(
                    "duplicate",
                    (
                        int(previous["quantity"]),
                        int(previous["requested_exp"]),
                        int(previous["exp_gain"]),
                        int(previous["hp_before"]),
                        int(previous["hp_after"]),
                        int(previous["mp_before"]),
                        int(previous["mp_after"]),
                    ),
                )

            user = uow.query_one(
                "SELECT exp,hp,mp FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            if user is None:
                return result("user_missing")
            item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None or int(item["goods_num"] or 0) < quantity:
                return result("item_insufficient")

            current_exp = int(user["exp"] or 0)
            hp_before = int(user["hp"] or 0)
            mp_before = int(user["mp"] or 0)
            exp_gain = min(requested_exp, max(0, max_exp - current_exp))
            final_exp = current_exp + exp_gain
            max_hp = int(final_exp / 2)
            hp_after = (
                min(hp_before + int(current_exp / 10), max_hp)
                if hp_before < max_hp
                else hp_before
            )
            mp_after = (
                min(mp_before + int(current_exp / 20), final_exp)
                if mp_before < final_exp
                else mp_before
            )

            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            updates = ["goods_num=goods_num-?"]
            params: list[object] = [quantity]
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
                "WHERE user_id=? AND goods_id=? AND goods_num>?",
                (*params, user_id, item_id, quantity - 1),
            )
            updated = uow.execute(
                "UPDATE user_xiuxian SET exp=?,hp=?,mp=?,power=ROUND(?*?,0) WHERE user_id=?",
                (final_exp, hp_after, mp_after, final_exp, power_multiplier, user_id),
            )
            if consumed.rowcount != 1 or updated.rowcount != 1:
                return result("state_changed")

            values = (
                quantity,
                requested_exp,
                exp_gain,
                hp_before,
                hp_after,
                mp_before,
                mp_after,
            )
            uow.execute(
                "INSERT INTO three_cultivation_pill_operations "
                "(operation_id,user_id,item_id,quantity,requested_exp,exp_gain,hp_before,hp_after,mp_before,mp_after,power_multiplier) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (operation_id, user_id, item_id, *values, power_multiplier),
            )
            return result("applied", values)


__all__ = ["ThreeCultivationPillResult", "ThreeCultivationPillSqlRepository"]
