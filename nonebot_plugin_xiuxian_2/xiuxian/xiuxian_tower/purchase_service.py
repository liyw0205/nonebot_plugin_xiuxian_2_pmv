from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TowerPurchaseResult:
    status: str
    quantity: int
    cost: int
    score: int
    purchased: int
    inventory: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}



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

class TowerPurchaseService:
    """Exchange tower score for inventory items in one transaction."""

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
        expected_score,
        expected_weekly_purchases,
        max_goods_num,
        bind_flag=1,
    ) -> TowerPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        item_name = str(item_name)
        item_type = str(item_type)
        quantity = int(quantity)
        unit_cost = int(unit_cost)
        weekly_limit = int(weekly_limit)
        expected_score = int(expected_score)
        max_goods_num = int(max_goods_num)
        bind_flag = 1 if int(bind_flag) == 1 else 0
        weekly = normalize_weekly_purchases(expected_weekly_purchases)
        if not operation_id or quantity <= 0 or min(item_id, unit_cost, weekly_limit, expected_score, max_goods_num) < 0:
            raise ValueError("valid operation, item, quantity and purchase limits are required")
        payload = json.dumps(
            [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, expected_score, weekly, max_goods_num, bind_flag],
            ensure_ascii=True,
            sort_keys=True,
        )

        def result(status: str, score=expected_score, purchased=0, inventory=0) -> TowerPurchaseResult:
            return TowerPurchaseResult(status, quantity if status in {"applied", "duplicate"} else 0, quantity * unit_cost if status in {"applied", "duplicate"} else 0, int(score), int(purchased), int(inventory))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS tower_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, quantity INTEGER NOT NULL, "
                    "cost INTEGER NOT NULL, score INTEGER NOT NULL, purchased INTEGER NOT NULL, "
                    "inventory INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, quantity, cost, score, purchased, inventory FROM tower_purchase_operations "
                    "WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return TowerPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                user = conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", ("tower",)
                ).fetchone()
                columns = (
                    {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(tower)").fetchall()}
                    if table is not None else set()
                )
                if not {"score", "weekly_purchases"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                tower = conn.execute(
                    "SELECT COALESCE(score, 0), COALESCE(weekly_purchases, '{}') FROM player_data.tower WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if tower is None:
                    conn.rollback()
                    return result("state_changed")
                try:
                    current_weekly = json.loads(str(tower[1])) if tower[1] else {}
                except (TypeError, ValueError):
                    conn.rollback()
                    return result("state_changed")
                current_weekly = normalize_weekly_purchases(current_weekly)
                if int(tower[0]) != expected_score or current_weekly != weekly:
                    conn.rollback()
                    return result("state_changed")

                purchased = int(weekly.get(str(item_id), 0))
                if purchased + quantity > weekly_limit:
                    conn.rollback()
                    return result("limit_reached", purchased=purchased)
                cost = quantity * unit_cost
                if expected_score < cost:
                    conn.rollback()
                    return result("score_insufficient", purchased=purchased)
                inventory_row = conn.execute(
                    "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)
                ).fetchone()
                inventory = int(inventory_row[0]) if inventory_row else 0
                if inventory + quantity > max_goods_num:
                    conn.rollback()
                    return result("inventory_full", purchased=purchased, inventory=inventory)

                new_score = expected_score - cost
                new_purchased = purchased + quantity
                new_inventory = inventory + quantity
                weekly[str(item_id)] = new_purchased
                if conn.execute(
                    "UPDATE player_data.tower SET score=%s, weekly_purchases=%s WHERE user_id=%s AND COALESCE(score, 0)=%s",
                    (new_score, json.dumps(weekly, ensure_ascii=False), user_id, expected_score),
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
                    "INSERT INTO tower_purchase_operations VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, quantity, cost, new_score, new_purchased, new_inventory),
                )
                conn.commit()
                return TowerPurchaseResult("applied", quantity, cost, new_score, new_purchased, new_inventory)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["TowerPurchaseResult", "TowerPurchaseService", "normalize_weekly_purchases"]
