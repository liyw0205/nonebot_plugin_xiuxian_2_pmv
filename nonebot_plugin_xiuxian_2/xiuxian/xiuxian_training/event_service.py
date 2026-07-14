from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TrainingEventResult:
    status: str
    message: str = ""

    @property
    def succeeded(self):
        return self.status in {"applied", "duplicate"}


class TrainingEventService:
    _FIELDS = ("progress", "last_time", "points", "completed", "max_progress", "last_event", "weekly_purchases")

    def __init__(self, game_db, player_db, lock=None):
        self.game_db, self.player_db = Path(game_db), Path(player_db)
        self.lock = lock or RLock()

    @staticmethod
    def _value(key, value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True) if key == "weekly_purchases" else str(value)

    @classmethod
    def _state_matches(cls, current, expected_state):
        for key, actual in zip(cls._FIELDS, current):
            expected = expected_state[key]
            if key == "weekly_purchases":
                try:
                    if json.loads(str(actual)) == expected:
                        continue
                except (TypeError, ValueError):
                    pass
            elif str(actual) == str(expected):
                continue
            return False
        return True

    @staticmethod
    def _stored_result(payload, user_id):
        try:
            stored = json.loads(str(payload))
            if str(stored[0]) != user_id or not isinstance(stored[2], dict):
                return TrainingEventResult("operation_conflict")
            return TrainingEventResult("duplicate", str(stored[2].get("last_event", "")))
        except (IndexError, TypeError, ValueError):
            return TrainingEventResult("operation_conflict")

    def apply(self, operation_id, user_id, expected_state, state, expected_user, stone_delta=0,
              exp_delta=0, hp_delta=0, items=(), max_goods_num=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_state, state, expected_user = dict(expected_state), dict(state), dict(expected_user)
        stone_delta, exp_delta, hp_delta, max_goods_num = map(int, (stone_delta, exp_delta, hp_delta, max_goods_num))
        rewards = tuple((int(x["id"]), str(x["name"]), str(x["type"]), int(x["amount"])) for x in items if int(x["amount"]) != 0)
        payload = json.dumps([user_id, expected_state, state, expected_user, stone_delta, exp_delta, hp_delta, rewards, max_goods_num], ensure_ascii=True, sort_keys=True, default=str)
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),)); conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS training_event_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
                old = conn.execute("SELECT payload FROM training_event_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback(); return self._stored_result(old[0], user_id)
                user = conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                wanted = tuple(int(expected_user[k]) for k in ("stone", "exp", "hp", "mp"))
                if user is None or tuple(map(int, user)) != wanted:
                    conn.rollback(); return TrainingEventResult("state_changed")
                current = conn.execute("SELECT progress,last_time,points,completed,max_progress,last_event,weekly_purchases FROM player_data.training WHERE user_id=%s", (user_id,)).fetchone()
                if current is None or not self._state_matches(current, expected_state):
                    conn.rollback(); return TrainingEventResult("state_changed")
                for item_id, _, _, amount in rewards:
                    row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    count = int(row[0]) if row else 0
                    if count + amount < 0:
                        conn.rollback(); return TrainingEventResult("item_missing")
                    if amount > 0 and count + amount > max_goods_num:
                        conn.rollback(); return TrainingEventResult("inventory_full")
                if wanted[0] + stone_delta < 0 or wanted[1] + exp_delta < 0 or wanted[2] + hp_delta < 0:
                    conn.rollback(); return TrainingEventResult("resource_missing")
                conn.execute("UPDATE player_data.training SET progress=%s,last_time=%s,points=%s,completed=%s,max_progress=%s,last_event=%s,weekly_purchases=%s WHERE user_id=%s", tuple(self._value(k, state[k]) for k in self._FIELDS) + (user_id,))
                conn.execute("UPDATE user_xiuxian SET stone=stone+%s,exp=exp+%s,hp=hp+%s WHERE user_id=%s", (stone_delta, exp_delta, hp_delta, user_id))
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    if amount > 0:
                        conn.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num,update_time=EXCLUDED.update_time", (user_id,item_id,name,item_type,amount,now,now,amount))
                    else:
                        conn.execute("UPDATE back SET goods_num=goods_num+%s,bind_num=MIN(COALESCE(bind_num,0),goods_num+%s) WHERE user_id=%s AND goods_id=%s", (amount, amount, user_id, item_id))
                conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
                try: conn.execute('ALTER TABLE player_data.statistics ADD COLUMN "历练次数" INTEGER DEFAULT 0')
                except db_backend.Error: pass
                conn.execute('INSERT INTO player_data.statistics(user_id,"历练次数") VALUES (%s,1) ON CONFLICT(user_id) DO UPDATE SET "历练次数"=COALESCE(statistics."历练次数",0)+1', (user_id,))
                conn.execute("INSERT INTO training_event_operations VALUES (%s,%s)", (operation_id, payload)); conn.commit()
                return TrainingEventResult("applied", str(state.get("last_event", "")))
            except Exception:
                conn.rollback(); raise


__all__ = ["TrainingEventResult", "TrainingEventService"]
