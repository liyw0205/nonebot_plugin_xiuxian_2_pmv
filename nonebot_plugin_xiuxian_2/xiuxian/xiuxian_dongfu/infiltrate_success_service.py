from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class InfiltrateSuccessResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class InfiltrateSuccessService:
    """Settle every state change produced by a successful infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database = Path(game_database), Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _canonical(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def settle(self, operation_id, visitor_id, target_id, day, mode_field, mode_limit, target_limit,
               expected_slots, slot_no, new_finish, rewards, stone, consume_guard, max_goods_num):
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(str, (visitor_id, target_id, day, mode_field))
        mode_limit, target_limit, slot_no, stone, max_goods_num = map(int, (mode_limit, target_limit, slot_no, stone, max_goods_num))
        consume_guard = int(bool(consume_guard))
        try:
            expected_slots = self._canonical(json.loads(expected_slots))
        except (TypeError, ValueError):
            raise ValueError("expected slots must be valid JSON")
        reward_rows = tuple((int(row[0]), str(row[1]), str(row[2]), int(row[3])) for row in rewards)
        new_finish = str(new_finish or "")
        if not operation_id or visitor_id == target_id or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}:
            raise ValueError("valid infiltration success operation is required")
        payload = self._canonical((visitor_id, target_id, day, mode_field, mode_limit, target_limit, expected_slots, slot_no, new_finish, reward_rows, stone, consume_guard, max_goods_num))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_infiltrate_success_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,infiltrate_left INTEGER NOT NULL,intrude_left INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload,infiltrate_left,intrude_left FROM dongfu_infiltrate_success_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return InfiltrateSuccessResult("duplicate", int(old[1]), int(old[2])) if str(old[0]) == payload else InfiltrateSuccessResult("state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (visitor_id,)).fetchone() is None:
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                visitor = conn.execute(f'SELECT built,infiltrate_date,{mode_field} FROM player_data."dongfu_status" WHERE user_id=%s', (visitor_id,)).fetchone()
                target = conn.execute('SELECT built,intrude_date,intrude_count,patrol_guard,plant_slots FROM player_data."dongfu_status" WHERE user_id=%s', (target_id,)).fetchone()
                if visitor is None or target is None or int(visitor[0] or 0) != 1 or int(target[0] or 0) != 1:
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                try:
                    slots = json.loads(str(target[4] or ""))
                except (TypeError, ValueError):
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                if self._canonical(slots) != expected_slots or slot_no < 1 or slot_no > len(slots):
                    conn.rollback(); return InfiltrateSuccessResult("state_changed")
                mode_count = int(visitor[2] or 0) if str(visitor[1] or "") == day else 0
                intrude_count = int(target[2] or 0) if str(target[1] or "") == day else 0
                if mode_count >= mode_limit or intrude_count >= target_limit:
                    conn.rollback(); return InfiltrateSuccessResult("daily_limit")
                totals, metadata = {}, {}
                for item_id, name, item_type, amount in reward_rows:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = name, item_type
                for item_id, amount in totals.items():
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (visitor_id, item_id)).fetchone()
                    if (int(item[0]) if item else 0) + amount > max_goods_num:
                        conn.rollback(); return InfiltrateSuccessResult("inventory_full")
                if new_finish:
                    slots[slot_no - 1]["plant_finish"] = new_finish
                mode_count, intrude_count = mode_count + 1, intrude_count + 1
                conn.execute(f'UPDATE player_data."dongfu_status" SET infiltrate_date=%s,{mode_field}=%s WHERE user_id=%s', (day, mode_count, visitor_id))
                conn.execute('UPDATE player_data."dongfu_status" SET intrude_date=%s,intrude_count=%s,patrol_guard=MAX(patrol_guard-%s,0),plant_slots=%s WHERE user_id=%s', (day, intrude_count, consume_guard, self._canonical(slots), target_id))
                if stone:
                    conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s", (stone, visitor_id))
                now = datetime.now()
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time", (visitor_id, item_id, name, item_type, amount, now, now, amount))
                left = max(0, mode_limit - mode_count), max(0, target_limit - intrude_count)
                conn.execute("INSERT INTO dongfu_infiltrate_success_operations (operation_id,payload,infiltrate_left,intrude_left) VALUES (%s,%s,%s,%s)", (operation_id, payload, *left))
                conn.commit()
                return InfiltrateSuccessResult("settled", *left)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["InfiltrateSuccessResult", "InfiltrateSuccessService"]
