from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class SeedPurchaseResult:
    status: str
    quantity: int
    cost: int
    stone: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SeedPurchaseService:
    """Purchase map seeds without splitting stone and inventory writes."""

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    def purchase(self, operation_id, user_id, item_id, item_name, quantity, unit_cost, expected_stone, max_goods_num) -> SeedPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, unit_cost, expected_stone, max_goods_num = map(int, (item_id, quantity, unit_cost, expected_stone, max_goods_num))
        item_name = str(item_name)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, expected_stone, max_goods_num) < 0:
            raise ValueError("valid operation, seed and purchase state are required")
        payload = json.dumps([user_id, item_id, item_name, quantity, unit_cost, expected_stone, max_goods_num], ensure_ascii=True, sort_keys=True)

        def result(status, stone=expected_stone, inventory=0):
            succeeded = status in {"applied", "duplicate"}
            return SeedPurchaseResult(status, quantity if succeeded else 0, quantity * unit_cost if succeeded else 0, int(stone), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS map_seed_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, stone INTEGER NOT NULL, inventory INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,quantity,cost,stone,inventory FROM map_seed_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return result("state_changed")
                    return SeedPurchaseResult("duplicate", *(int(value) for value in old[1:]))
                user = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                stone = int(user[0] or 0)
                if stone != expected_stone:
                    conn.rollback()
                    return result("state_changed")
                cost = quantity * unit_cost
                if stone < cost:
                    conn.rollback()
                    return result("stone_insufficient")
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory = int(item[0]) if item else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full")
                stone -= cost
                inventory += quantity
                now = datetime.now()
                conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (stone, user_id))
                conn.execute(
                    "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name, "
                    "goods_type=EXCLUDED.goods_type, goods_num=back.goods_num+EXCLUDED.goods_num, "
                    "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num, update_time=EXCLUDED.update_time",
                    (user_id, item_id, item_name, "特殊物品", quantity, now, now, quantity),
                )
                conn.execute(
                    "INSERT INTO map_seed_purchase_operations (operation_id,payload,quantity,cost,stone,inventory) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, quantity, cost, stone, inventory),
                )
                conn.commit()
                return SeedPurchaseResult("applied", quantity, cost, stone, inventory)
            except Exception:
                conn.rollback()
                raise


__all__ = ["SeedPurchaseResult", "SeedPurchaseService"]
