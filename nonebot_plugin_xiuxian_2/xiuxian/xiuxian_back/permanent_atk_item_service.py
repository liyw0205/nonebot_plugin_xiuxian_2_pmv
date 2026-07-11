from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PermanentAtkItemUse:
    status: str
    user_id: str
    item_id: int
    quantity: int
    atk_gain: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PermanentAtkItemService:
    """Consume an elixir and increase permanent attack atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS permanent_atk_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                atk_gain INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(self, operation_id, user_id, item_id, quantity, atk_gain):
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        atk_gain = int(atk_gain)
        if quantity <= 0 or atk_gain < 0:
            raise ValueError("quantity must be positive and attack gain non-negative")

        def result(status: str, values=None) -> PermanentAtkItemUse:
            values = values or (quantity, atk_gain)
            return PermanentAtkItemUse(
                status, user_id, item_id, int(values[0]), int(values[1])
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, atk_gain FROM permanent_atk_item_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous)
                if conn.execute(
                    "SELECT 1 FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return result("buff_missing")
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

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
                updated = conn.execute(
                    "UPDATE BuffInfo SET atk_buff=COALESCE(atk_buff, 0)+%s "
                    "WHERE user_id=%s",
                    (atk_gain, user_id),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO permanent_atk_item_operations "
                    "(operation_id, user_id, item_id, quantity, atk_gain) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, atk_gain),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["PermanentAtkItemService", "PermanentAtkItemUse"]
