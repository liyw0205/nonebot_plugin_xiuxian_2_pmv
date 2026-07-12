from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaPurchaseResult:
    status: str
    quantity: int
    cost: int
    honor_points: int
    purchased: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaPurchaseService:
    """Exchange honor points for an inventory item in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def purchase(
        self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost,
        weekly_limit, expected_honor, expected_weekly_purchases, max_goods_num, bind_flag=1,
    ) -> ArenaPurchaseResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        item_id, quantity, unit_cost, weekly_limit = map(int, (item_id, quantity, unit_cost, weekly_limit))
        expected_honor, max_goods_num = map(int, (expected_honor, max_goods_num))
        item_name, item_type = str(item_name), str(item_type)
        bind_flag = 1 if int(bind_flag) == 1 else 0
        weekly = {str(key): value for key, value in dict(expected_weekly_purchases).items()}
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_honor, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        payload = json.dumps(
            [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_honor, weekly, max_goods_num, bind_flag],
            ensure_ascii=True, sort_keys=True,
        )

        def result(status: str, honor_points=expected_honor, purchased=0, inventory=0) -> ArenaPurchaseResult:
            succeeded = status in {"applied", "duplicate"}
            return ArenaPurchaseResult(status, quantity if succeeded else 0, quantity * unit_cost if succeeded else 0, int(honor_points), int(purchased), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, honor_points INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, honor_points, purchased, inventory FROM arena_purchase_operations "
                    "WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return ArenaPurchaseResult("duplicate", *(int(value) for value in previous[1:]))
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return result("user_missing")
                columns = {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()}
                if not {"honor_points", "weekly_purchases"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                arena = conn.execute(
                    "SELECT COALESCE(honor_points, 0), COALESCE(weekly_purchases, '{}') FROM player_data.arena WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if arena is None:
                    conn.rollback()
                    return result("state_changed")
                try:
                    current_weekly = json.loads(str(arena[1])) if arena[1] else {}
                except (TypeError, ValueError):
                    conn.rollback()
                    return result("state_changed")
                if int(arena[0]) != expected_honor or current_weekly != weekly:
                    conn.rollback()
                    return result("state_changed")
                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return result("limit_reached", purchased=purchased)
                cost = quantity * unit_cost
                if expected_honor < cost:
                    conn.rollback()
                    return result("honor_insufficient", purchased=purchased)
                inventory_row = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                inventory = int(inventory_row[0]) if inventory_row else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", purchased=purchased, inventory=inventory)
                honor_points, purchased, inventory = expected_honor - cost, purchased + quantity, inventory + quantity
                weekly[str(item_id)] = purchased
                if conn.execute(
                    "UPDATE player_data.arena SET honor_points=%s, weekly_purchases=%s WHERE user_id=%s AND COALESCE(honor_points, 0)=%s",
                    (honor_points, json.dumps(weekly, ensure_ascii=False), user_id, expected_honor),
                ).rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                now = datetime.now()
                conn.execute(
                    "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                    "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=CASE WHEN %s=1 THEN "
                    "COALESCE(back.bind_num, 0)+EXCLUDED.goods_num ELSE COALESCE(back.bind_num, 0) END",
                    (user_id, item_id, item_name, item_type, quantity, now, now, quantity if bind_flag else 0, bind_flag),
                )
                conn.execute(
                    "INSERT INTO arena_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, honor_points, purchased, inventory),
                )
                conn.commit()
                return ArenaPurchaseResult("applied", quantity, cost, honor_points, purchased, inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["ArenaPurchaseResult", "ArenaPurchaseService"]
