from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


def normalize_weekly_purchases(value, today=None):
    today = today or date.today()
    weekly = {str(key): item for key, item in dict(value or {}).items()}
    try:
        reset = date.fromisoformat(str(weekly.get("_last_reset", "")))
    except ValueError:
        reset = None
    if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
        return {"_last_reset": today.isoformat()}
    return weekly


@dataclass(frozen=True)
class BossPurchaseResult:
    status: str
    quantity: int
    cost: int
    integral: int
    purchased: int
    inventory: int


class BossPurchaseService:
    """Exchange world-boss integral for an item in one transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def purchase(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        unit_cost,
        weekly_limit,
        expected_integral,
        expected_weekly_purchases,
        max_goods_num,
        today=None,
    ) -> BossPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        unit_cost = int(unit_cost)
        weekly_limit = int(weekly_limit)
        expected_integral = int(expected_integral)
        max_goods_num = int(max_goods_num)
        today = today or date.today()
        weekly = normalize_weekly_purchases(expected_weekly_purchases, today)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_integral, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        payload = json.dumps(
            [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_integral, weekly, max_goods_num],
            ensure_ascii=True,
            sort_keys=True,
        )

        def rejected(status: str, purchased=0, inventory=0) -> BossPurchaseResult:
            return BossPurchaseResult(status, 0, 0, expected_integral, int(purchased), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS boss_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, integral INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, integral, purchased, inventory FROM boss_purchase_operations "
                    "WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return BossPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return rejected("user_missing")
                required = (("boss", {"weekly_purchases"}), ("boss_limit", {"integral"}))
                for table, fields in required:
                    exists = conn.execute(
                        "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", (table,)
                    ).fetchone()
                    columns = (
                        {str(column[1]) for column in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}
                        if exists is not None else set()
                    )
                    if not fields.issubset(columns):
                        conn.rollback()
                        return rejected("state_changed")
                integral_row = conn.execute(
                    "SELECT COALESCE(integral, 0) FROM player_data.boss_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                weekly_row = conn.execute(
                    "SELECT COALESCE(weekly_purchases, '{}') FROM player_data.boss WHERE user_id=%s", (user_id,)
                ).fetchone()
                if integral_row is None or weekly_row is None:
                    conn.rollback()
                    return rejected("state_changed")
                try:
                    current_weekly = json.loads(str(weekly_row[0])) if weekly_row[0] else {}
                except (TypeError, ValueError):
                    conn.rollback()
                    return rejected("state_changed")
                current_weekly = normalize_weekly_purchases(current_weekly, today)
                if int(integral_row[0]) != expected_integral or current_weekly != weekly:
                    conn.rollback()
                    return rejected("state_changed")

                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return rejected("limit_reached", purchased)
                cost = quantity * unit_cost
                if expected_integral < cost:
                    conn.rollback()
                    return rejected("integral_insufficient", purchased)
                item = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                inventory = int(item[0]) if item else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return rejected("inventory_full", purchased, inventory)

                new_integral = expected_integral - cost
                new_purchased = purchased + quantity
                new_inventory = inventory + quantity
                weekly[str(item_id)] = new_purchased
                if conn.execute(
                    "UPDATE player_data.boss_limit SET integral=%s WHERE user_id=%s AND COALESCE(integral, 0)=%s",
                    (new_integral, user_id, expected_integral),
                ).rowcount != 1:
                    conn.rollback()
                    return rejected("state_changed")
                conn.execute(
                    "UPDATE player_data.boss SET weekly_purchases=%s WHERE user_id=%s",
                    (json.dumps(weekly, ensure_ascii=False), user_id),
                )
                now = datetime.now()
                conn.execute(
                    "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, update_time=EXCLUDED.update_time, "
                    "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num, 0)+EXCLUDED.goods_num",
                    (user_id, item_id, item_name, item_type, quantity, now, now, quantity),
                )
                conn.execute(
                    "INSERT INTO boss_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, new_integral, new_purchased, new_inventory),
                )
                conn.commit()
                return BossPurchaseResult("applied", quantity, cost, new_integral, new_purchased, new_inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BossPurchaseResult", "BossPurchaseService", "normalize_weekly_purchases"]
