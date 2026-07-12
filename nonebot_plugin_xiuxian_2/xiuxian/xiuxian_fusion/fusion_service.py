from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class FusionResult:
    status: str
    successful: bool
    protected: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class FusionService:
    """Apply fusion costs, material consumption and reward in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        stone_cost,
        materials,
        target_id,
        target_name,
        target_type,
        *,
        successful,
        protection_item_id=None,
        reserved_items=None,
        max_goods_num,
    ) -> FusionResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        stone_cost = int(stone_cost)
        target_id = int(target_id)
        successful = bool(successful)
        max_goods_num = int(max_goods_num)
        materials = {int(key): int(value) for key, value in materials.items() if int(value) > 0}
        reserved = {int(key): int(value) for key, value in (reserved_items or {}).items()}
        protection_item_id = int(protection_item_id) if protection_item_id is not None else None
        if not operation_id or stone_cost < 0 or max_goods_num <= 0:
            raise ValueError("valid operation, non-negative cost and positive capacity are required")

        payload = json.dumps(
            [user_id, stone_cost, sorted(materials.items()), target_id, str(target_name),
             str(target_type), protection_item_id, sorted(reserved.items()), max_goods_num],
            ensure_ascii=True,
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS fusion_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, successful INTEGER NOT NULL, "
                    "protected INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, successful, protected FROM fusion_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return FusionResult("state_changed", successful, False)
                    return FusionResult("duplicate", bool(previous[1]), bool(previous[2]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return FusionResult("user_missing", successful, False)

                protected = False
                if not successful and protection_item_id is not None:
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, protection_item_id),
                    ).fetchone()
                    protected = row is not None and int(row[0]) > 0

                if protected:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1 WHERE user_id=%s AND goods_id=%s AND goods_num>=1",
                        (user_id, protection_item_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return FusionResult("state_changed", successful, False)
                else:
                    if int(user[0]) < stone_cost:
                        conn.rollback()
                        return FusionResult("stone_insufficient", successful, False)
                    columns = set(conn.column_names("back"))
                    for item_id, quantity in materials.items():
                        row = conn.execute(
                            "SELECT COALESCE(goods_num, 0)" +
                            (", COALESCE(bind_num, 0)" if "bind_num" in columns else "") +
                            " FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id),
                        ).fetchone()
                        available = 0 if row is None else int(row[0]) - (int(row[1]) if len(row) > 1 else 0)
                        if available - max(0, reserved.get(item_id, 0)) < quantity:
                            conn.rollback()
                            return FusionResult("item_insufficient", successful, False)
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone-%s WHERE user_id=%s AND stone>=%s",
                        (stone_cost, user_id, stone_cost),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return FusionResult("state_changed", successful, False)
                    for item_id, quantity in materials.items():
                        consumed = conn.execute(
                            "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s",
                            (quantity, user_id, item_id),
                        )
                        if consumed.rowcount != 1:
                            conn.rollback()
                            return FusionResult("state_changed", successful, False)

                if successful:
                    if "bind_num" in set(conn.column_names("back")):
                        conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                            "VALUES (%s, %s, %s, %s, 1, 1) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=MIN(COALESCE(back.goods_num, 0)+1, %s), "
                            "bind_num=MIN(COALESCE(back.bind_num, 0)+1, %s)",
                            (user_id, target_id, str(target_name), str(target_type), max_goods_num, max_goods_num),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num) "
                            "VALUES (%s, %s, %s, %s, 1) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                            "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                            "goods_num=MIN(COALESCE(back.goods_num, 0)+1, %s)",
                            (user_id, target_id, str(target_name), str(target_type), max_goods_num),
                        )
                conn.execute(
                    "INSERT INTO fusion_operations (operation_id, payload, successful, protected) "
                    "VALUES (%s, %s, %s, %s)",
                    (operation_id, payload, int(successful), int(protected)),
                )
                conn.commit()
                return FusionResult("applied", successful, protected)
            except Exception:
                conn.rollback()
                raise


__all__ = ["FusionResult", "FusionService"]
