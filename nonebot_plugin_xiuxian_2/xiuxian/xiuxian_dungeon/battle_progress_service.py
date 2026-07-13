from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DungeonBattleProgressResult:
    status: str
    current_layer: int = 0
    dungeon_status: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DungeonBattleProgressService:
    """Grant battle rewards and advance the leader's progress in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, leader_id, expected_status, rewards, complete, max_goods_num):
        operation_id, leader_id = str(operation_id).strip(), str(leader_id)
        expected = {str(key): str(value) for key, value in dict(expected_status).items()}
        normalized = tuple(
            (str(reward["user_id"]), int(reward["expected_stone"]), int(reward["expected_exp"]),
             int(reward.get("stone", 0)), int(reward.get("exp", 0)),
             tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]), int(item.get("expected_num", 0))) for item in reward.get("items", [])))
            for reward in rewards
        )
        complete, max_goods_num = bool(complete), int(max_goods_num)
        if not operation_id or not normalized or max_goods_num < 0:
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps([leader_id, expected, normalized, complete, max_goods_num], ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dungeon_battle_progress_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,current_layer INTEGER NOT NULL,dungeon_status TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload,current_layer,dungeon_status FROM dungeon_battle_progress_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return DungeonBattleProgressResult("state_changed")
                    return DungeonBattleProgressResult("duplicate", int(old[1]), str(old[2]))
                columns = tuple(expected)
                row = conn.execute("SELECT " + ",".join(columns) + " FROM player_data.player_dungeon_status WHERE user_id=%s", (leader_id,)).fetchone()
                if row is None or tuple(str(value) for value in row) != tuple(expected.values()):
                    conn.rollback()
                    return DungeonBattleProgressResult("state_changed")
                if expected.get("dungeon_status") != "exploring":
                    conn.rollback()
                    return DungeonBattleProgressResult("not_exploring")
                for user_id, expected_stone, expected_exp, _, _, reward_items in normalized:
                    user = conn.execute("SELECT stone,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                    if user is None:
                        conn.rollback()
                        return DungeonBattleProgressResult("user_missing")
                    if (int(user[0]), int(user[1])) != (expected_stone, expected_exp):
                        conn.rollback()
                        return DungeonBattleProgressResult("state_changed")
                    for item_id, _, _, amount, expected_num in reward_items:
                        item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                        current_num = int(item[0]) if item else 0
                        if current_num != expected_num:
                            conn.rollback()
                            return DungeonBattleProgressResult("state_changed")
                        if current_num + amount > max_goods_num:
                            conn.rollback()
                            return DungeonBattleProgressResult("inventory_full")
                now = datetime.now()
                for user_id, _, _, stone, exp, reward_items in normalized:
                    conn.execute("UPDATE user_xiuxian SET stone=stone+%s,exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                    for item_id, name, item_type, amount, _ in reward_items:
                        conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num,update_time=EXCLUDED.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
                current_layer = int(expected["total_layers"]) if complete else min(int(expected["current_layer"]) + 1, int(expected["total_layers"]))
                dungeon_status = "completed" if complete or current_layer >= int(expected["total_layers"]) else "exploring"
                conn.execute("UPDATE player_data.player_dungeon_status SET current_layer=%s,dungeon_status=%s WHERE user_id=%s", (current_layer, dungeon_status, leader_id))
                conn.execute("INSERT INTO dungeon_battle_progress_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, current_layer, dungeon_status))
                conn.commit()
                return DungeonBattleProgressResult("applied", current_layer, dungeon_status)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["DungeonBattleProgressResult", "DungeonBattleProgressService"]
