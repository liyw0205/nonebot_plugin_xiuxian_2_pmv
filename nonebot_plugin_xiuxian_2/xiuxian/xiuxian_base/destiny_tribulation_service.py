from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_buff.relation_transaction_utils import increment_stat
from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DestinyTribulationResult:
    status: str
    target_level: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DestinyTribulationService:
    """Consume the destiny pill and promote the player in one transaction."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, *, expected_level, expected_exp, target_level, power, occurred_at):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, target_level = str(expected_level), str(target_level)
        expected_exp, power, occurred_at = int(expected_exp), int(power), str(occurred_at)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps([user_id, expected_level, expected_exp, target_level, power, occurred_at], separators=(",", ":"))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS destiny_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,target_level TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,target_level FROM destiny_tribulation_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    return DestinyTribulationResult("duplicate" if str(previous[0]) == payload else "operation_conflict", str(previous[1]))
                user = conn.execute("SELECT level,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or str(user[0]) != expected_level or int(user[1] or 0) != expected_exp:
                    conn.rollback(); return DestinyTribulationResult("state_changed", target_level)
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                    "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                    "WHERE user_id=%s AND goods_id=1997 AND goods_num>0", (occurred_at, occurred_at, user_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback(); return DestinyTribulationResult("item_missing", target_level)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET level=%s,power=%s WHERE user_id=%s AND level=%s AND exp=%s",
                    (target_level, power, user_id, expected_level, expected_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback(); return DestinyTribulationResult("state_changed", target_level)
                conn.execute("DELETE FROM user_tribulation WHERE user_id=%s", (user_id,))
                increment_stat(conn, user_id, "渡劫次数", 1)
                increment_stat(conn, user_id, "渡劫成功", 1)
                increment_stat(conn, user_id, "天命渡劫丹消耗", 1)
                conn.execute("INSERT INTO destiny_tribulation_operations(operation_id,payload,target_level) VALUES(%s,%s,%s)", (operation_id, payload, target_level))
                conn.commit(); return DestinyTribulationResult("applied", target_level)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass


__all__ = ["DestinyTribulationResult", "DestinyTribulationService"]
