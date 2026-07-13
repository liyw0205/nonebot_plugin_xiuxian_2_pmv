from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BossRewardResult:
    status: str
    exp: int
    stone: int
    integral: int
    item_quantity: int
    total_exp: int
    wallet_stone: int
    daily_stone: int
    daily_integral: int
    total_integral: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BossRewardService:
    """Apply all personal world-boss rewards in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def grant(self, operation_id, user_id, expected_daily_stone, expected_daily_integral,
              expected_total_integral, expected_exp, stone, integral, exp=0, item_id=0,
              item_name="", item_type="", item_quantity=0, item_bind=0, max_goods_num=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        values = [expected_daily_stone, expected_daily_integral, expected_total_integral, expected_exp,
                  stone, integral, exp, item_id, item_quantity, max_goods_num]
        (expected_daily_stone, expected_daily_integral, expected_total_integral, expected_exp,
         stone, integral, exp, item_id, item_quantity, max_goods_num) = map(int, values)
        item_bind = 1 if int(item_bind) == 1 else 0
        if not operation_id or min(values) < 0:
            raise ValueError("valid operation, snapshots and rewards are required")
        payload = json.dumps([user_id, *values, str(item_name), str(item_type), item_bind], ensure_ascii=True)

        def rejected(status, wallet=0):
            return BossRewardResult(status, 0, 0, 0, 0, expected_exp, int(wallet),
                                    expected_daily_stone, expected_daily_integral, expected_total_integral)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS boss_reward_operations (operation_id TEXT PRIMARY KEY, "
                    "payload TEXT NOT NULL, exp INTEGER NOT NULL, stone INTEGER NOT NULL, integral INTEGER NOT NULL, "
                    "item_quantity INTEGER NOT NULL, total_exp INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, "
                    "daily_stone INTEGER NOT NULL, daily_integral INTEGER NOT NULL, total_integral INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,exp,stone,integral,item_quantity,total_exp,wallet_stone,daily_stone,daily_integral,total_integral "
                    "FROM boss_reward_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    return rejected("state_changed") if str(previous[0]) != payload else BossRewardResult(
                        "duplicate", *(int(value) for value in previous[1:]))
                user = conn.execute("SELECT COALESCE(stone,0),COALESCE(exp,0) FROM user_xiuxian WHERE user_id=%s",
                                    (user_id,)).fetchone()
                if user is None:
                    conn.rollback(); return rejected("user_missing")
                for table, fields in (("boss", {"boss_stone", "boss_integral"}), ("boss_limit", {"integral"})):
                    exists = conn.execute("SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", (table,)).fetchone()
                    columns = {str(row[1]) for row in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()} if exists else set()
                    if not fields.issubset(columns):
                        conn.rollback(); return rejected("state_changed", user[0])
                daily = conn.execute("SELECT COALESCE(boss_stone,0),COALESCE(boss_integral,0) FROM player_data.boss WHERE user_id=%s", (user_id,)).fetchone()
                total = conn.execute("SELECT COALESCE(integral,0) FROM player_data.boss_limit WHERE user_id=%s", (user_id,)).fetchone()
                current = (int(daily[0]) if daily else 0, int(daily[1]) if daily else 0,
                           int(total[0]) if total else 0, int(user[1]))
                if current != (expected_daily_stone, expected_daily_integral, expected_total_integral, expected_exp):
                    conn.rollback(); return rejected("state_changed", user[0])
                if item_quantity:
                    item = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                    if (int(item[0]) if item else 0) + item_quantity > max_goods_num:
                        conn.rollback(); return rejected("inventory_full", user[0])
                total_exp, wallet = expected_exp + exp, int(user[0]) + stone
                if conn.execute("UPDATE user_xiuxian SET stone=%s,exp=%s WHERE user_id=%s AND COALESCE(stone,0)=%s AND COALESCE(exp,0)=%s",
                                (wallet, total_exp, user_id, int(user[0]), expected_exp)).rowcount != 1:
                    conn.rollback(); return rejected("state_changed", user[0])
                daily_stone, daily_integral, total_integral = (expected_daily_stone + stone,
                                                               expected_daily_integral + integral,
                                                               expected_total_integral + integral)
                if daily is None:
                    conn.execute("INSERT INTO player_data.boss (user_id,boss_stone,boss_integral) VALUES (%s,%s,%s)", (user_id, daily_stone, daily_integral))
                else:
                    conn.execute("UPDATE player_data.boss SET boss_stone=%s,boss_integral=%s WHERE user_id=%s", (daily_stone, daily_integral, user_id))
                if total is None:
                    conn.execute("INSERT INTO player_data.boss_limit (user_id,integral) VALUES (%s,%s)", (user_id, total_integral))
                else:
                    conn.execute("UPDATE player_data.boss_limit SET integral=%s WHERE user_id=%s", (total_integral, user_id))
                if item_quantity:
                    now, bound = datetime.now(), item_quantity if item_bind else 0
                    conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num,bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,update_time=EXCLUDED.update_time",
                                 (user_id, item_id, str(item_name), str(item_type), item_quantity, now, now, bound))
                conn.execute("INSERT INTO boss_reward_operations VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                             (operation_id, payload, exp, stone, integral, item_quantity, total_exp, wallet,
                              daily_stone, daily_integral, total_integral))
                conn.commit()
                return BossRewardResult("applied", exp, stone, integral, item_quantity, total_exp, wallet,
                                        daily_stone, daily_integral, total_integral)
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BossRewardResult", "BossRewardService"]
