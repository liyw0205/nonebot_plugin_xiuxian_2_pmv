from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PillFusionResult:
    status: str
    user_id: str
    source_item_id: int
    source_quantity: int
    target_item_id: int
    target_quantity: int
    successful: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PillFusionService:
    """Consume fusion materials and grant a pre-rolled result atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pill_fusion_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                source_item_id INTEGER NOT NULL,
                source_quantity INTEGER NOT NULL,
                target_item_id INTEGER NOT NULL,
                target_quantity INTEGER NOT NULL,
                successful INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        source_item_id,
        source_quantity,
        target_item_id,
        target_name,
        target_type,
        *,
        successful,
        target_quantity=1,
        max_goods_num,
    ) -> PillFusionResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        source_item_id = int(source_item_id)
        source_quantity = int(source_quantity)
        target_item_id = int(target_item_id)
        target_name = str(target_name)
        target_type = str(target_type)
        successful = bool(successful)
        target_quantity = int(target_quantity) if successful else 0
        max_goods_num = int(max_goods_num)
        if source_quantity <= 0 or max_goods_num <= 0:
            raise ValueError("source_quantity and max_goods_num must be positive")
        if successful and target_quantity <= 0:
            raise ValueError("target_quantity must be positive after successful fusion")
        if source_item_id == target_item_id:
            raise ValueError("source and target items must differ")

        def result(status: str, success=successful, reward_quantity=target_quantity):
            return PillFusionResult(
                status,
                user_id,
                source_item_id,
                source_quantity,
                target_item_id,
                int(reward_quantity),
                bool(success),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT target_quantity, successful FROM pill_fusion_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", bool(previous[1]), previous[0])

                source = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, source_item_id),
                ).fetchone()
                if source is None or int(source[0] or 0) < source_quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-%s"]
                params: list[object] = [source_quantity]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>=%s "
                        "THEN COALESCE(bind_num, 0)-%s "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-%s) END"
                    )
                    params.extend((source_quantity,) * 4)
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, source_item_id, source_quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                if successful:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, target_item_id, target_name, target_type,
                            target_quantity, target_quantity, max_goods_num, max_goods_num,
                        ),
                    )
                conn.execute(
                    "INSERT INTO pill_fusion_operations "
                    "(operation_id, user_id, source_item_id, source_quantity, "
                    "target_item_id, target_quantity, successful) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        operation_id, user_id, source_item_id, source_quantity,
                        target_item_id, target_quantity, int(successful),
                    ),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["PillFusionResult", "PillFusionService"]
