from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DungeonPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    stone: int = 0
    inventory: int = 0


class DungeonPurchaseService:
    """Exchange spirit stones for a dungeon-shop item atomically."""

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(game_database)
        self._lock = lock or RLock()

    def purchase(self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost, expected_stone, max_goods, bind_flag=1):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, unit_cost, expected_stone, max_goods = map(int, (item_id, quantity, unit_cost, expected_stone, max_goods))
        if not operation_id or min(quantity, unit_cost) <= 0 or min(expected_stone, max_goods) < 0:
            raise ValueError("valid purchase is required")
        payload = json.dumps([user_id, item_id, str(item_name), str(item_type), quantity, unit_cost, expected_stone, int(bool(bind_flag))], ensure_ascii=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dungeon_purchase_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,stone INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,quantity,cost,stone,inventory FROM dungeon_purchase_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    return DungeonPurchaseResult("duplicate" if str(previous[0]) == payload else "state_changed", *map(int, previous[1:]))
                user = conn.execute("SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback(); return DungeonPurchaseResult("user_missing")
                if int(user[0]) != expected_stone:
                    conn.rollback(); return DungeonPurchaseResult("state_changed")
                cost = quantity * unit_cost
                if expected_stone < cost:
                    conn.rollback(); return DungeonPurchaseResult("stone_insufficient", quantity, cost, expected_stone)
                item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                inventory = int(item[0]) if item else 0
                if inventory + quantity > max_goods:
                    conn.rollback(); return DungeonPurchaseResult("inventory_full", quantity, cost, expected_stone, inventory)
                new_stone, new_inventory = expected_stone - cost, inventory + quantity
                if conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s AND COALESCE(stone,0)=%s", (new_stone, user_id, expected_stone)).rowcount != 1:
                    conn.rollback(); return DungeonPurchaseResult("state_changed")
                now = datetime.now()
                conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time", (user_id,item_id,item_name,item_type,quantity,now,now,quantity if bind_flag else 0))
                conn.execute("INSERT INTO dungeon_purchase_operations VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id,payload,quantity,cost,new_stone,new_inventory))
                conn.commit(); return DungeonPurchaseResult("applied", quantity, cost, new_stone, new_inventory)
            except Exception:
                conn.rollback(); raise


__all__ = ["DungeonPurchaseResult", "DungeonPurchaseService"]
