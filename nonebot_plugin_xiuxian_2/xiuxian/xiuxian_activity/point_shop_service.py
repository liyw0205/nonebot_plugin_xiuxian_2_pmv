from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivityPointShopPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    points: int = 0
    personal_count: int = 0
    total_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivityPointShopPurchaseService:
    """Settle an activity point purchase and all rewards atomically."""

    def __init__(self, activity_database: str | Path, game_database: str | Path, lock: RLock | None = None) -> None:
        self._activity_database = Path(activity_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _reward_rows(rewards) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list] = {}
        for reward in rewards:
            quantity = int(reward["quantity"])
            if quantity <= 0:
                raise ValueError("reward quantity must be positive")
            if str(reward["type"]) == "stone":
                stone += quantity
                continue
            item_id = int(reward["id"])
            item_type = str(reward["type"])
            if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                item_type = "技能"
            elif item_type in {"法器", "防具"}:
                item_type = "装备"
            metadata = [str(reward["name"]), item_type]
            if item_id in items and items[item_id][:2] != metadata:
                raise ValueError("conflicting reward metadata")
            items.setdefault(item_id, metadata + [0])[2] += quantity
        return stone, tuple(
            (item_id, values[0], values[1], values[2])
            for item_id, values in sorted(items.items())
        )

    def purchase(
        self, operation_id, user_id, activity_key, item_key, quantity, unit_cost,
        personal_limit, stock_limit, rewards, max_goods_num,
    ) -> ActivityPointShopPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key, item_key = map(str, (user_id, activity_key, item_key))
        quantity, unit_cost, personal_limit, stock_limit, max_goods_num = map(
            int, (quantity, unit_cost, personal_limit, stock_limit, max_goods_num)
        )
        stone, item_rows = self._reward_rows(rewards)
        if not operation_id or not activity_key or not item_key or quantity <= 0 or unit_cost <= 0 or min(personal_limit, stock_limit, max_goods_num) < 0:
            raise ValueError("valid activity point purchase is required")
        cost = quantity * unit_cost
        payload = json.dumps(
            [user_id, activity_key, item_key, quantity, unit_cost, personal_limit,
             stock_limit, stone, item_rows, max_goods_num],
            ensure_ascii=True, separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS activity_point_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,"
                    "cost INTEGER NOT NULL,points INTEGER NOT NULL,personal_count INTEGER NOT NULL,"
                    "total_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,quantity,cost,points,personal_count,total_count "
                    "FROM activity_point_purchase_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityPointShopPurchaseResult("operation_conflict")
                    return ActivityPointShopPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                balance = conn.execute(
                    "SELECT COALESCE(points,0) FROM activity_point_balance WHERE activity_key=%s AND user_id=%s",
                    (activity_key, user_id),
                ).fetchone()
                if balance is None or int(balance[0]) < cost:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("points_insufficient", points=int(balance[0]) if balance else 0)
                personal = conn.execute(
                    "SELECT COALESCE(count,0) FROM activity_point_purchase "
                    "WHERE activity_key=%s AND user_id=%s AND item_key=%s",
                    (activity_key, user_id, item_key),
                ).fetchone()
                personal_count = int(personal[0]) if personal else 0
                total_count = int(conn.execute(
                    "SELECT COALESCE(SUM(count),0) FROM activity_point_purchase WHERE activity_key=%s AND item_key=%s",
                    (activity_key, item_key),
                ).fetchone()[0])
                if personal_limit > 0 and personal_count + quantity > personal_limit:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("personal_limit", points=int(balance[0]), personal_count=personal_count, total_count=total_count)
                if stock_limit > 0 and total_count + quantity > stock_limit:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("stock_insufficient", points=int(balance[0]), personal_count=personal_count, total_count=total_count)
                if conn.execute("SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("user_missing")
                for item_id, _, _, amount in item_rows:
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + amount > max_goods_num:
                        conn.rollback()
                        return ActivityPointShopPurchaseResult("inventory_full")

                points = int(balance[0]) - cost
                personal_count += quantity
                total_count += quantity
                now = datetime.now()
                changed = conn.execute(
                    "UPDATE activity_point_balance SET points=points-%s,update_time=%s "
                    "WHERE activity_key=%s AND user_id=%s AND points >= %s",
                    (cost, now, activity_key, user_id, cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("state_changed")
                conn.execute(
                    "INSERT INTO activity_point_purchase(activity_key,user_id,item_key,count,update_time) "
                    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT(activity_key,user_id,item_key) DO UPDATE SET "
                    "count=activity_point_purchase.count+excluded.count,update_time=excluded.update_time",
                    (activity_key, user_id, item_key, quantity, now),
                )
                if stone:
                    conn.execute("UPDATE game_data.user_xiuxian SET stone=COALESCE(stone,0)+%s WHERE user_id=%s", (stone, user_id))
                for item_id, name, item_type, amount in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute(
                    "INSERT INTO activity_point_purchase_operations(operation_id,payload,quantity,cost,points,personal_count,total_count) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, quantity, cost, points, personal_count, total_count),
                )
                conn.commit()
                return ActivityPointShopPurchaseResult("applied", quantity, cost, points, personal_count, total_count)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE game_data")


__all__ = ["ActivityPointShopPurchaseResult", "ActivityPointShopPurchaseService"]
