from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class CultivationItemUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    exp_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class CultivationItemService:
    """Consume cultivation items and update character attributes atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cultivation_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                exp_gain INTEGER NOT NULL,
                hp_gain INTEGER NOT NULL,
                mp_gain INTEGER NOT NULL,
                atk_gain INTEGER NOT NULL,
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
        exp_gain,
        *,
        hp_gain,
        mp_gain,
        atk_gain,
        power_multiplier,
    ) -> CultivationItemUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        exp_gain = int(exp_gain)
        hp_gain = int(hp_gain)
        mp_gain = int(mp_gain)
        atk_gain = int(atk_gain)
        power_multiplier = float(power_multiplier)
        if quantity <= 0 or min(exp_gain, hp_gain, mp_gain, atk_gain) < 0:
            raise ValueError("quantity and gains must be non-negative")
        if power_multiplier < 0:
            raise ValueError("power_multiplier must be non-negative")

        def result(status: str, result_quantity=quantity, result_exp=exp_gain):
            return CultivationItemUse(
                status, user_id, item_id, int(result_quantity), int(result_exp)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, exp_gain FROM cultivation_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], previous[1])

                if conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                back_columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "bind_num" in back_columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, item_id, quantity),
                )
                updated = conn.execute(
                    """
                    UPDATE user_xiuxian
                    SET exp=COALESCE(exp, 0)+%s,
                        hp=COALESCE(hp, 0)+%s,
                        mp=COALESCE(mp, 0)+%s,
                        atk=COALESCE(atk, 0)+%s,
                        power=ROUND((COALESCE(exp, 0)+%s)*%s, 0)
                    WHERE user_id=%s
                    """,
                    (
                        exp_gain,
                        hp_gain,
                        mp_gain,
                        atk_gain,
                        exp_gain,
                        power_multiplier,
                        user_id,
                    ),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                conn.execute(
                    """
                    INSERT INTO cultivation_item_operations (
                        operation_id, user_id, item_id, quantity, exp_gain,
                        hp_gain, mp_gain, atk_gain, power_multiplier
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
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
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["CultivationItemService", "CultivationItemUse"]
