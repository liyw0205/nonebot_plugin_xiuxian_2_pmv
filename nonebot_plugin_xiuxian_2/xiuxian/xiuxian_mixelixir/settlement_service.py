from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MixelixirSettlementResult:
    status: str
    reward_quantity: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MixelixirSettlementService:
    """Consume recipe materials and grant the elixir in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> MixelixirSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS mixelixir_settlement_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, reward_quantity INTEGER NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload, reward_quantity FROM mixelixir_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return MixelixirSettlementResult("duplicate", int(previous[1]))

    def settle(
        self,
        operation_id,
        user_id,
        materials,
        reward_id,
        reward_name,
        reward_quantity,
        *,
        max_goods_num,
    ) -> MixelixirSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        reward_id = int(reward_id)
        reward_quantity = int(reward_quantity)
        max_goods_num = int(max_goods_num)
        normalized_materials: dict[int, int] = {}
        for item_id, quantity in materials.items():
            item_id = int(item_id)
            quantity = int(quantity)
            if quantity > 0:
                normalized_materials[item_id] = normalized_materials.get(item_id, 0) + quantity
        if not operation_id or not normalized_materials or reward_quantity <= 0 or max_goods_num <= 0:
            raise ValueError("operation, materials, reward quantity and capacity are required")

        # Request identity only — reward_name/max_goods_num are display/config.
        payload = json.dumps(
            [user_id, sorted(normalized_materials.items()), reward_id, reward_quantity],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_settlement_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, reward_quantity INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, reward_quantity FROM mixelixir_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirSettlementResult("state_changed", 0)
                    return MixelixirSettlementResult("duplicate", int(previous[1]))

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return MixelixirSettlementResult("user_missing", 0)

                for item_id, quantity in normalized_materials.items():
                    row = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if row is None or int(row[0]) < quantity:
                        conn.rollback()
                        return MixelixirSettlementResult("item_insufficient", 0)

                for item_id, quantity in normalized_materials.items():
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                        (quantity, user_id, item_id, quantity),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return MixelixirSettlementResult("state_changed", 0)

                columns = set(conn.column_names("back"))
                insert_columns = "user_id, goods_id, goods_name, goods_type, goods_num"
                insert_values = "%s, %s, %s, %s, %s"
                if "bind_num" in columns:
                    insert_columns += ", bind_num"
                    insert_values += ", 0"
                conn.execute(
                    f"INSERT INTO back ({insert_columns}) VALUES ({insert_values}) "
                    "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                    "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s)",
                    (user_id, reward_id, str(reward_name), "丹药", reward_quantity, max_goods_num),
                )
                updated = conn.execute(
                    "UPDATE user_xiuxian SET mixelixir_num=COALESCE(mixelixir_num, 0)+1 WHERE user_id=%s",
                    (user_id,),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return MixelixirSettlementResult("state_changed", 0)
                conn.execute(
                    "INSERT INTO mixelixir_settlement_operations (operation_id, payload, reward_quantity) VALUES (%s, %s, %s)",
                    (operation_id, payload, reward_quantity),
                )
                conn.commit()
                return MixelixirSettlementResult("applied", reward_quantity)
            except Exception:
                conn.rollback()
                raise


__all__ = ["MixelixirSettlementResult", "MixelixirSettlementService"]
