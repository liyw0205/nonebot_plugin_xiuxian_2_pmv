from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ThreeCultivationPillUse:
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


class ThreeCultivationPillService:
    """Apply capped pill cultivation, recovery, and consumption atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS three_cultivation_pill_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                requested_exp INTEGER NOT NULL,
                exp_gain INTEGER NOT NULL,
                hp_before INTEGER NOT NULL,
                hp_after INTEGER NOT NULL,
                mp_before INTEGER NOT NULL,
                mp_after INTEGER NOT NULL,
                power_multiplier REAL NOT NULL,
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
        requested_exp,
        *,
        max_exp,
        power_multiplier,
    ) -> ThreeCultivationPillUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        requested_exp = int(requested_exp)
        max_exp = int(max_exp)
        power_multiplier = float(power_multiplier)
        if quantity <= 0 or requested_exp < 0 or max_exp < 0:
            raise ValueError("quantity must be positive and experience non-negative")
        if power_multiplier < 0:
            raise ValueError("power_multiplier must be non-negative")

        def result(status: str, values=None) -> ThreeCultivationPillUse:
            if values is None:
                values = (quantity, requested_exp, 0, 0, 0, 0, 0)
            return ThreeCultivationPillUse(
                status,
                user_id,
                item_id,
                *(int(value) for value in values),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, requested_exp, exp_gain, hp_before, hp_after, "
                    "mp_before, mp_after FROM three_cultivation_pill_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)

                user = conn.execute(
                    "SELECT exp, hp, mp FROM user_xiuxian WHERE user_id=%s",
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

                current_exp, hp_before, mp_before = (int(value or 0) for value in user)
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

                back_columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in back_columns:
                    bind_update = (
                        ", bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                updated = conn.execute(
                    "UPDATE user_xiuxian SET exp=%s, hp=%s, mp=%s, "
                    "power=ROUND(%s*%s, 0) WHERE user_id=%s",
                    (
                        final_exp,
                        hp_after,
                        mp_after,
                        final_exp,
                        power_multiplier,
                        user_id,
                    ),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
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
                conn.execute(
                    "INSERT INTO three_cultivation_pill_operations "
                    "(operation_id, user_id, item_id, quantity, requested_exp, "
                    "exp_gain, hp_before, hp_after, mp_before, mp_after, "
                    "power_multiplier) VALUES (%s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s, %s)",
                    (operation_id, user_id, item_id, *values, power_multiplier),
                )
                conn.commit()
                return result("applied", values)
            except Exception:
                conn.rollback()
                raise


__all__ = ["ThreeCultivationPillService", "ThreeCultivationPillUse"]
