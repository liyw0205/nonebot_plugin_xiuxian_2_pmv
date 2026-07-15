from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PetFeedResult:
    status: str
    stars: int = 0
    exp: int = 0
    total_exp: int = 0

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}


class PetFeedService:
    def __init__(self, game_db: str | Path, player_db: str | Path, lock: RLock | None = None):
        self.game_db = Path(game_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()

    def feed(self, operation_id, user_id, uid, item_id, count, expected, updated):
        operation_id, user_id, uid = map(str, (operation_id, user_id, uid))
        item_id, count = int(item_id), int(count)
        expected = tuple(map(int, expected))
        updated = tuple(map(int, updated))
        # Request identity only — exp/stars snapshots are concurrency checks.
        payload = json.dumps([user_id, uid, item_id, count], ensure_ascii=True, separators=(",", ":"))
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS pet_feed_operations(operation_id TEXT PRIMARY KEY,payload TEXT,stars INTEGER,exp INTEGER,total_exp INTEGER)")
                old = conn.execute("SELECT payload,stars,exp,total_exp FROM pet_feed_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback()
                    return PetFeedResult("duplicate" if old[0] == payload else "state_changed", *map(int, old[1:]))
                pet = conn.execute("SELECT stars,exp,total_exp,is_active FROM player_data.player_pet_item WHERE user_id=%s AND uid=%s", (user_id, uid)).fetchone()
                if pet is None or tuple(map(int, pet[:3])) != expected or int(pet[3]) != 1:
                    conn.rollback(); return PetFeedResult("state_changed")
                item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                if item is None or int(item[0]) < count:
                    conn.rollback(); return PetFeedResult("item_missing")
                conn.execute("UPDATE back SET goods_num=goods_num-%s WHERE user_id=%s AND goods_id=%s", (count, user_id, item_id))
                conn.execute("UPDATE player_data.player_pet_item SET stars=%s,exp=%s,total_exp=%s,updated_at=strftime('%%s','now') WHERE user_id=%s AND uid=%s", (*updated, user_id, uid))
                conn.execute("INSERT INTO pet_feed_operations VALUES (%s,%s,%s,%s,%s)", (operation_id, payload, *updated))
                conn.commit()
                return PetFeedResult("applied", *updated)
            except Exception:
                conn.rollback(); raise


__all__ = ["PetFeedResult", "PetFeedService"]
