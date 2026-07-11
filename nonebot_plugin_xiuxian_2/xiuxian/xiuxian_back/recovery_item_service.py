from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class RecoveryItemUse:
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


class RecoveryItemService:
    """Consume recovery elixirs and update character state atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recovery_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                mode TEXT NOT NULL,
                hp_before INTEGER NOT NULL,
                hp_after INTEGER NOT NULL,
                mp_before INTEGER NOT NULL,
                mp_after INTEGER NOT NULL,
                stamina_before INTEGER NOT NULL,
                stamina_after INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        *,
        mode,
        hp_gain=0,
        mp_gain=0,
        atk_after=None,
        stamina_gain=0,
        max_stamina=0,
    ) -> RecoveryItemUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        mode = str(mode)
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if mode not in {"hp_mp", "full", "stamina"}:
            raise ValueError("unsupported recovery mode")

        def result(status: str, values=None) -> RecoveryItemUse:
            values = values or (quantity, 0, 0, 0, 0, 0, 0)
            return RecoveryItemUse(status, user_id, item_id, *(int(v) for v in values))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, hp_before, hp_after, mp_before, mp_after, "
                    "stamina_before, stamina_after FROM recovery_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)

                user = conn.execute(
                    "SELECT exp, hp, mp, atk, user_stamina FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                exp, hp_before, mp_before, atk_before, stamina_before = (
                    int(value or 0) for value in user
                )
                next_hp = hp_before
                next_mp = mp_before
                next_atk = atk_before
                next_stamina = stamina_before
                if mode == "hp_mp":
                    max_hp = int(exp / 2)
                    next_hp = (
                        min(hp_before + int(hp_gain), max_hp)
                        if hp_before < max_hp
                        else hp_before
                    )
                    next_mp = (
                        min(mp_before + int(mp_gain), exp)
                        if mp_before < exp
                        else mp_before
                    )
                elif mode == "full":
                    next_hp = int(exp / 2)
                    next_mp = exp
                    next_atk = int(exp / 10) if atk_after is None else int(atk_after)
                elif mode == "stamina":
                    next_stamina = min(
                        stamina_before + int(stamina_gain), int(max_stamina)
                    )

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "day_num" in columns:
                    updates.append("day_num=COALESCE(day_num, 0)+%s")
                    params.append(quantity)
                if "all_num" in columns:
                    updates.append("all_num=COALESCE(all_num, 0)+%s")
                    params.append(quantity)
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                if mode == "stamina":
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                        (next_stamina, user_id),
                    )
                else:
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET hp=%s, mp=%s, atk=%s WHERE user_id=%s",
                        (next_hp, next_mp, next_atk, user_id),
                    )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                values = (
                    quantity,
                    hp_before,
                    next_hp,
                    mp_before,
                    next_mp,
                    stamina_before,
                    next_stamina,
                )
                conn.execute(
                    "INSERT INTO recovery_item_operations "
                    "(operation_id, user_id, item_id, quantity, mode, hp_before, "
                    "hp_after, mp_before, mp_after, stamina_before, stamina_after) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, mode, *values[1:]),
                )
                conn.commit()
                return result("applied", values)
            except Exception:
                conn.rollback()
                raise


__all__ = ["RecoveryItemService", "RecoveryItemUse"]
