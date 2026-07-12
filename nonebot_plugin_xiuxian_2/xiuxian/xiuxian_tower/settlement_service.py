from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TowerSettlementResult:
    status: str
    score: int
    stone: int
    exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TowerSettlementService:
    """Commit one tower-floor result and all rewards across both databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_tower, floor, score, stone, exp, items, max_goods_num) -> TowerSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = {key: int(dict(expected_tower)[key]) for key in ("current_floor", "max_floor", "score")}
        floor, score, stone, exp, max_goods_num = map(int, (floor, score, stone, exp, max_goods_num))
        rewards = tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in items if int(item.get("amount", 0)) > 0)
        if not operation_id or floor <= 0 or min(score, stone, exp, max_goods_num, *expected.values()) < 0:
            raise ValueError("valid operation, tower state and rewards are required")
        payload = json.dumps([user_id, expected, floor, score, stone, exp, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)

        def result(status: str) -> TowerSettlementResult:
            return TowerSettlementResult(status, score if status in {"applied", "duplicate"} else 0, stone if status in {"applied", "duplicate"} else 0, exp if status in {"applied", "duplicate"} else 0)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS tower_settlement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM tower_settlement_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate" if str(previous[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                columns = {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(tower)").fetchall()}
                if not {"current_floor", "max_floor", "score"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                tower = conn.execute("SELECT current_floor, max_floor, score FROM player_data.tower WHERE user_id=%s", (user_id,)).fetchone()
                if tower is None or tuple(map(int, tower)) != (expected["current_floor"], expected["max_floor"], expected["score"]):
                    conn.rollback()
                    return result("state_changed")
                for item_id, _, _, amount in rewards:
                    inventory = conn.execute("SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(inventory[0]) if inventory else 0) + amount > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")
                new_max_floor = max(expected["max_floor"], floor)
                if conn.execute("UPDATE player_data.tower SET current_floor=%s, max_floor=%s, score=%s WHERE user_id=%s", (floor, new_max_floor, expected["score"] + score, user_id)).rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, update_time=EXCLUDED.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
                conn.execute("INSERT INTO tower_settlement_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["TowerSettlementResult", "TowerSettlementService"]
