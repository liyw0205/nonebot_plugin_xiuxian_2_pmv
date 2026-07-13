from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DungeonManualRewardResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DungeonManualRewardService:
    """Award every eligible dungeon member in one game-database transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def award(self, operation_id, rewards, max_goods_num) -> DungeonManualRewardResult:
        operation_id = str(operation_id).strip()
        max_goods_num = int(max_goods_num)
        normalized = tuple(
            (str(reward["user_id"]), int(reward.get("stone", 0)), int(reward.get("exp", 0)),
             tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in reward.get("items", []) if int(item.get("amount", 0)) > 0))
            for reward in rewards
        )
        if not operation_id or max_goods_num < 0 or not normalized or any(min(stone, exp) < 0 for _, stone, exp, _ in normalized):
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps(normalized, ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dungeon_manual_reward_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM dungeon_manual_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return DungeonManualRewardResult("duplicate" if str(previous[0]) == payload else "state_changed")
                for user_id, _, _, _ in normalized:
                    if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                        conn.rollback()
                        return DungeonManualRewardResult("user_missing")
                for user_id, _, _, reward_items in normalized:
                    for item_id, _, _, amount in reward_items:
                        row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                        if (int(row[0]) if row else 0) + amount > max_goods_num:
                            conn.rollback()
                            return DungeonManualRewardResult("inventory_full")
                now = datetime.now()
                for user_id, stone, exp, reward_items in normalized:
                    conn.execute("UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                    for item_id, name, item_type, amount in reward_items:
                        conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, update_time=EXCLUDED.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
                conn.execute("INSERT INTO dungeon_manual_reward_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return DungeonManualRewardResult("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["DungeonManualRewardResult", "DungeonManualRewardService"]
