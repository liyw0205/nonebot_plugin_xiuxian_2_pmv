from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DungeonExploreEventResult:
    status: str
    current_layer: int = 0
    dungeon_status: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DungeonExploreEventService:
    """Settle a non-combat event and advance dungeon progress atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, leader_id, expected_status, event, members, max_goods_num):
        operation_id, leader_id = str(operation_id).strip(), str(leader_id)
        expected = {str(key): str(value) for key, value in dict(expected_status).items()}
        event_type = str(event.get("type", "nothing"))
        stone = int(event.get("stone", 0))
        item = event.get("item")
        normalized_item = None if item is None else (
            int(item["id"]), str(item["name"]), str(item["type"]), int(item.get("amount", 1)), int(item.get("expected_num", 0))
        )
        normalized_members = tuple(
            (str(member["user_id"]), int(member["expected_hp"]), int(member["expected_mp"]), int(member.get("hp_delta", 0)))
            for member in members
        )
        max_goods_num = int(max_goods_num)
        if not operation_id or event_type not in {"trap", "treasure", "spirit_stone", "nothing"} or max_goods_num < 0:
            raise ValueError("valid operation and non-combat event are required")
        payload = json.dumps([leader_id, expected, event_type, stone, normalized_item, normalized_members, max_goods_num], ensure_ascii=True, sort_keys=True)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dungeon_explore_event_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,current_layer INTEGER NOT NULL,"
                    "dungeon_status TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,current_layer,dungeon_status FROM dungeon_explore_event_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return DungeonExploreEventResult("state_changed")
                    return DungeonExploreEventResult("duplicate", int(old[1]), str(old[2]))

                columns = tuple(expected)
                status_row = conn.execute(
                    "SELECT " + ",".join(columns) + " FROM player_data.player_dungeon_status WHERE user_id=%s",
                    (leader_id,),
                ).fetchone()
                if status_row is None or tuple(str(value) for value in status_row) != tuple(expected.values()):
                    conn.rollback()
                    return DungeonExploreEventResult("state_changed")
                if expected.get("dungeon_status") != "exploring":
                    conn.rollback()
                    return DungeonExploreEventResult("not_exploring")

                for user_id, expected_hp, expected_mp, _ in normalized_members:
                    row = conn.execute("SELECT hp,mp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                    if row is None:
                        conn.rollback()
                        return DungeonExploreEventResult("user_missing")
                    if (int(row[0]), int(row[1])) != (expected_hp, expected_mp):
                        conn.rollback()
                        return DungeonExploreEventResult("state_changed")

                leader = conn.execute("SELECT stone FROM user_xiuxian WHERE user_id=%s", (leader_id,)).fetchone()
                if leader is None:
                    conn.rollback()
                    return DungeonExploreEventResult("user_missing")
                if event_type == "spirit_stone" and int(leader[0]) != int(event.get("expected_stone", leader[0])):
                    conn.rollback()
                    return DungeonExploreEventResult("state_changed")
                if normalized_item is not None:
                    item_id, _, _, amount, expected_num = normalized_item
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (leader_id, item_id)).fetchone()
                    current_num = int(row[0]) if row else 0
                    if current_num != expected_num:
                        conn.rollback()
                        return DungeonExploreEventResult("state_changed")
                    if current_num + amount > max_goods_num:
                        conn.rollback()
                        return DungeonExploreEventResult("inventory_full")

                for user_id, _, _, hp_delta in normalized_members:
                    if hp_delta:
                        conn.execute("UPDATE user_xiuxian SET hp=hp+%s WHERE user_id=%s", (hp_delta, user_id))
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s", (stone, leader_id))
                if normalized_item is not None:
                    item_id, name, item_type, amount, _ = normalized_item
                    now = datetime.now()
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num,update_time=EXCLUDED.update_time",
                        (leader_id, item_id, name, item_type, amount, now, now, amount),
                    )

                current_layer = min(int(expected["current_layer"]) + 1, int(expected["total_layers"]))
                dungeon_status = "completed" if current_layer >= int(expected["total_layers"]) else "exploring"
                conn.execute(
                    "UPDATE player_data.player_dungeon_status SET current_layer=%s,dungeon_status=%s WHERE user_id=%s",
                    (current_layer, dungeon_status, leader_id),
                )
                conn.execute(
                    "INSERT INTO dungeon_explore_event_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, current_layer, dungeon_status),
                )
                conn.commit()
                return DungeonExploreEventResult("applied", current_layer, dungeon_status)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["DungeonExploreEventResult", "DungeonExploreEventService"]
