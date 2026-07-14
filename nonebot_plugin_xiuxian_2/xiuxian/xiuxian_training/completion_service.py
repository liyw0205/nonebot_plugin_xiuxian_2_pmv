from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TrainingCompletionResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TrainingCompletionService:
    """Atomically save a completed training cycle and its game rewards."""

    _STATE_FIELDS = ("progress", "last_time", "points", "completed", "max_progress", "last_event", "weekly_purchases")

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def complete(self, operation_id, user_id, expected_state, state, stone, exp, items, max_goods_num) -> TrainingCompletionResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected, updated = dict(expected_state), dict(state)
        stone, exp, max_goods_num = map(int, (stone, exp, max_goods_num))
        rewards = tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in items if int(item.get("amount", 0)) > 0)
        if not operation_id or min(stone, exp, max_goods_num) < 0 or not all(key in expected and key in updated for key in self._STATE_FIELDS):
            raise ValueError("valid operation, training state and rewards are required")
        def state_value(key, value):
            return json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "weekly_purchases" else str(value)
        def state_matches(current):
            for key, actual in zip(self._STATE_FIELDS, current):
                if key == "weekly_purchases":
                    try:
                        if json.loads(str(actual)) == expected[key]:
                            continue
                    except (TypeError, ValueError):
                        pass
                elif str(actual) == str(expected[key]):
                    continue
                return False
            return True
        payload = json.dumps([user_id, expected, updated, stone, exp, rewards, max_goods_num], ensure_ascii=True, sort_keys=True, default=str)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS training_completion_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM training_completion_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    return TrainingCompletionResult("duplicate" if str(previous[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return TrainingCompletionResult("user_missing")
                current = conn.execute("SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases FROM player_data.training WHERE user_id=%s", (user_id,)).fetchone()
                if current is None or not state_matches(current):
                    conn.rollback()
                    return TrainingCompletionResult("state_changed")
                for item_id, _, _, amount in rewards:
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(row[0]) if row else 0) + amount > max_goods_num:
                        conn.rollback()
                        return TrainingCompletionResult("inventory_full")
                conn.execute("UPDATE player_data.training SET progress=%s,last_time=%s,points=%s,completed=%s,max_progress=%s,last_event=%s,weekly_purchases=%s WHERE user_id=%s", tuple(state_value(key, updated[key]) for key in self._STATE_FIELDS) + (user_id,))
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s", (stone, exp, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, update_time=EXCLUDED.update_time", (user_id,item_id,name,item_type,amount,now,now,amount))
                conn.execute("INSERT INTO training_completion_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id,payload))
                conn.commit()
                return TrainingCompletionResult("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")
