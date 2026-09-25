from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class RecoveryItemResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    hp_before: int
    hp_after: int
    mp_before: int
    mp_after: int
    stamina_before: int
    stamina_after: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RecoveryItemSqlRepository:
    """Atomically consume recovery elixirs and update character state."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        *,
        mode: str,
        hp_gain: int = 0,
        mp_gain: int = 0,
        atk_after: int | None = None,
        stamina_gain: int = 0,
        max_stamina: int = 0,
    ) -> RecoveryItemResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        mode = str(mode)
        hp_gain = int(hp_gain)
        mp_gain = int(mp_gain)
        stamina_gain = int(stamina_gain)
        max_stamina = int(max_stamina)
        if atk_after is not None:
            atk_after = int(atk_after)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if mode not in {"hp_mp", "full", "stamina"}:
            raise ValueError("unsupported recovery mode")

        def result(status: str, values=None) -> RecoveryItemResult:
            values = values or (quantity, 0, 0, 0, 0, 0, 0)
            return RecoveryItemResult(
                status,
                user_id,
                item_id,
                *(int(value) for value in values),
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity,hp_before,hp_after,mp_before,mp_after,"
                "stamina_before,stamina_after FROM recovery_item_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result(
                    "duplicate",
                    (
                        previous["quantity"],
                        previous["hp_before"],
                        previous["hp_after"],
                        previous["mp_before"],
                        previous["mp_after"],
                        previous["stamina_before"],
                        previous["stamina_after"],
                    ),
                )

            user = uow.query_one(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                "WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return result("user_missing")
            item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None or int(item["goods_num"] or 0) < quantity:
                return result("item_insufficient")

            exp = int(user["exp"] or 0)
            hp_before = int(user["hp"] or 0)
            mp_before = int(user["mp"] or 0)
            atk_before = int(user["atk"] or 0)
            stamina_before = int(user["user_stamina"] or 0)
            hp_after = hp_before
            mp_after = mp_before
            atk_after_value = atk_before
            stamina_after = stamina_before
            if mode == "hp_mp":
                max_hp = int(exp / 2)
                hp_after = min(hp_before + hp_gain, max_hp) if hp_before < max_hp else hp_before
                mp_after = min(mp_before + mp_gain, exp) if mp_before < exp else mp_before
            elif mode == "full":
                hp_after = int(exp / 2)
                mp_after = exp
                atk_after_value = int(exp / 10) if atk_after is None else atk_after
            else:
                stamina_after = min(stamina_before + stamina_gain, max_stamina)

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
            if mode == "stamina":
                updated = uow.execute(
                    "UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?",
                    (stamina_after, user_id),
                )
            else:
                updated = uow.execute(
                    "UPDATE user_xiuxian SET hp=?,mp=?,atk=? WHERE user_id=?",
                    (hp_after, mp_after, atk_after_value, user_id),
                )
            if consumed.rowcount != 1 or updated.rowcount != 1:
                return result("state_changed")

            values = (
                quantity,
                hp_before,
                hp_after,
                mp_before,
                mp_after,
                stamina_before,
                stamina_after,
            )
            uow.execute(
                "INSERT INTO recovery_item_operations "
                "(operation_id,user_id,item_id,quantity,mode,hp_before,hp_after,"
                "mp_before,mp_after,stamina_before,stamina_after) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (operation_id, user_id, item_id, values[0], mode, *values[1:]),
            )
            return result("applied", values)


__all__ = ["RecoveryItemResult", "RecoveryItemSqlRepository"]
