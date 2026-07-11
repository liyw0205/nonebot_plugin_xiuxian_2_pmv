from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class UnbindItemResult:
    status: str
    user_id: str
    charm_item_id: int
    target_item_id: int
    quantity: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class UnbindItemService:
    """Consume charms and reduce a target item's bound quantity atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS unbind_item_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                charm_item_id INTEGER NOT NULL,
                target_item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        charm_item_id,
        target_item_id,
        requested_quantity,
    ) -> UnbindItemResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        charm_item_id = int(charm_item_id)
        target_item_id = int(target_item_id)
        requested_quantity = int(requested_quantity)
        if requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if charm_item_id == target_item_id:
            raise ValueError("charm and target item must differ")

        def result(status: str, quantity=0) -> UnbindItemResult:
            return UnbindItemResult(
                status, user_id, charm_item_id, target_item_id, int(quantity)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity FROM unbind_item_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0])

                charm = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, charm_item_id),
                ).fetchone()
                if charm is None or int(charm[0] or 0) <= 0:
                    conn.rollback()
                    return result("charm_missing")
                target = conn.execute(
                    "SELECT goods_num, bind_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, target_item_id),
                ).fetchone()
                if target is None or int(target[0] or 0) <= 0:
                    conn.rollback()
                    return result("target_missing")
                if int(target[1] or 0) <= 0:
                    conn.rollback()
                    return result("not_bound")

                quantity = min(
                    requested_quantity,
                    int(charm[0] or 0),
                    int(target[1] or 0),
                )
                charm_columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [quantity]
                if "bind_num" in charm_columns:
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
                    (*params, user_id, charm_item_id, quantity),
                )
                unbound = conn.execute(
                    "UPDATE back SET bind_num=bind_num-%s "
                    "WHERE user_id=%s AND goods_id=%s AND bind_num>=%s",
                    (quantity, user_id, target_item_id, quantity),
                )
                if consumed.rowcount != 1 or unbound.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO unbind_item_operations "
                    "(operation_id, user_id, charm_item_id, target_item_id, quantity) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, charm_item_id, target_item_id, quantity),
                )
                conn.commit()
                return result("applied", quantity)
            except Exception:
                conn.rollback()
                raise


__all__ = ["UnbindItemResult", "UnbindItemService"]
