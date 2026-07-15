from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuPatrolResult:
    status: str
    patrol_count: int = 0
    patrol_guard: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"patrolled", "duplicate"}


class DongfuPatrolService:
    """Settle stamina, patrol state and fixed patrol rewards atomically."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, day) -> str:
        return json.dumps((str(user_id), str(day)), ensure_ascii=False, separators=(",", ":"))

    def get_result(self, operation_id: str) -> DongfuPatrolResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS dongfu_patrol_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,patrol_count INTEGER NOT NULL,patrol_guard INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            old = conn.execute("SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=%s", (operation_id,)).fetchone()
            if old is None:
                return None
            return DongfuPatrolResult("duplicate", int(old[1]), int(old[2]))

    def patrol(self, operation_id, user_id, day, stamina_cost, daily_limit, stone_gain, reward=None, max_goods_num=999999999):
        operation_id, user_id, day = str(operation_id).strip(), str(user_id), str(day)
        stamina_cost, daily_limit, stone_gain, max_goods_num = map(int, (stamina_cost, daily_limit, stone_gain, max_goods_num))
        reward = tuple(reward) if reward else None
        if not operation_id or not day or stamina_cost < 0 or daily_limit < 1 or stone_gain < 0:
            raise ValueError("valid patrol operation is required")
        if reward is not None and (len(reward) != 3 or int(reward[0]) <= 0 or int(reward[2]) <= 0):
            raise ValueError("reward must contain item id, name and amount")
        # Request identity only — stone/item gains are outcomes; limits/stamina are concurrency checks.
        payload = self._payload(user_id, day)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_patrol_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,patrol_count INTEGER NOT NULL,patrol_guard INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback()
                    return DongfuPatrolResult("duplicate", int(old[1]), int(old[2])) if str(old[0]) == payload else DongfuPatrolResult("state_changed")
                user = conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return DongfuPatrolResult("user_missing")
                if int(user[0] or 0) < stamina_cost:
                    conn.rollback()
                    return DongfuPatrolResult("stamina_insufficient")
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(dongfu_status)").fetchall()}
                if not {"built", "patrol_date", "patrol_count", "patrol_guard"}.issubset(columns):
                    conn.rollback()
                    return DongfuPatrolResult("state_changed")
                row = conn.execute('SELECT built,patrol_date,patrol_count,patrol_guard FROM player_data."dongfu_status" WHERE user_id=%s', (user_id,)).fetchone()
                if row is None or int(row[0] or 0) != 1:
                    conn.rollback()
                    return DongfuPatrolResult("dongfu_missing")
                count, guard = (int(row[2] or 0), int(row[3] or 0)) if str(row[1] or "") == day else (0, 0)
                if count >= daily_limit:
                    conn.rollback()
                    return DongfuPatrolResult("daily_limit", count, guard)
                if reward:
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, int(reward[0]))).fetchone()
                    if (int(item[0]) if item else 0) + int(reward[2]) > max_goods_num:
                        conn.rollback()
                        return DongfuPatrolResult("inventory_full", count, guard)
                count, guard = count + 1, min(3, guard + 1)
                stamina = conn.execute("UPDATE user_xiuxian SET user_stamina=user_stamina-%s,stone=stone+%s WHERE user_id=%s AND user_stamina>=%s", (stamina_cost, stone_gain, user_id, stamina_cost))
                dongfu = conn.execute('UPDATE player_data."dongfu_status" SET patrol_date=%s,patrol_count=%s,patrol_guard=%s WHERE user_id=%s', (day, count, guard, user_id))
                if stamina.rowcount != 1 or dongfu.rowcount != 1:
                    conn.rollback()
                    return DongfuPatrolResult("state_changed")
                if reward:
                    item_id, item_name, amount = int(reward[0]), str(reward[1]), int(reward[2])
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num", (user_id, item_id, item_name, "特殊物品", amount, amount))
                conn.execute("INSERT INTO dongfu_patrol_operations (operation_id,payload,patrol_count,patrol_guard) VALUES (%s,%s,%s,%s)", (operation_id, payload, count, guard))
                conn.commit()
                return DongfuPatrolResult("patrolled", count, guard)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuPatrolResult", "DongfuPatrolService"]
