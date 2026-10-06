from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ._operation_payload import operation_payload_matches


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


class ActivityPointShopPurchaseSqlRepository:
    """Settle point purchases and rewards in the existing game_db tables."""

    operation_table = "activity_point_purchase_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _assert_tables(uow: DatabaseUnitOfWork) -> None:
        required = {
            "activity_point_balance",
            "activity_point_purchase",
            "activity_point_purchase_operations",
            "user_xiuxian",
            "back",
        }
        existing = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError("activity_state.001 schema_missing: " + ", ".join(missing))

    @staticmethod
    def _reward_rows(
        rewards: Any,
    ) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list[Any]] = {}
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
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        item_key: str,
        quantity: int,
        unit_cost: int,
        personal_limit: int,
        stock_limit: int,
        rewards: Any,
        max_goods_num: int,
    ) -> ActivityPointShopPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key, item_key = map(
            str, (user_id, activity_key, item_key)
        )
        quantity, unit_cost, personal_limit, stock_limit, max_goods_num = map(
            int, (quantity, unit_cost, personal_limit, stock_limit, max_goods_num)
        )
        stone, item_rows = self._reward_rows(rewards)
        if (
            not operation_id
            or not activity_key
            or not item_key
            or quantity <= 0
            or unit_cost <= 0
            or min(personal_limit, stock_limit, max_goods_num) < 0
        ):
            raise ValueError("valid activity point purchase is required")

        cost = quantity * unit_cost
        payload = json.dumps(
            [
                user_id,
                activity_key,
                item_key,
                quantity,
                unit_cost,
                personal_limit,
                stock_limit,
                stone,
                item_rows,
                max_goods_num,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_tables(uow)
            previous = uow.query_one(
                f"SELECT payload,quantity,cost,points,personal_count,total_count "
                f"FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if not operation_payload_matches(previous["payload"], payload):
                    return ActivityPointShopPurchaseResult("operation_conflict")
                return ActivityPointShopPurchaseResult(
                    "duplicate",
                    *(int(previous[key]) for key in (
                        "quantity", "cost", "points", "personal_count", "total_count"
                    )),
                )

            balance = uow.query_one(
                "SELECT COALESCE(points,0) AS points FROM activity_point_balance "
                "WHERE activity_key=? AND user_id=?",
                (activity_key, user_id),
            )
            current_points = int((balance or {}).get("points") or 0)
            if current_points < cost:
                return ActivityPointShopPurchaseResult(
                    "points_insufficient", points=current_points
                )

            personal = uow.query_one(
                "SELECT COALESCE(count,0) AS count FROM activity_point_purchase "
                "WHERE activity_key=? AND user_id=? AND item_key=?",
                (activity_key, user_id, item_key),
            )
            personal_count = int((personal or {}).get("count") or 0)
            total_row = uow.query_one(
                "SELECT COALESCE(SUM(count),0) AS count FROM activity_point_purchase "
                "WHERE activity_key=? AND item_key=?",
                (activity_key, item_key),
            )
            total_count = int((total_row or {}).get("count") or 0)
            if personal_limit > 0 and personal_count + quantity > personal_limit:
                return ActivityPointShopPurchaseResult(
                    "personal_limit",
                    points=current_points,
                    personal_count=personal_count,
                    total_count=total_count,
                )
            if stock_limit > 0 and total_count + quantity > stock_limit:
                return ActivityPointShopPurchaseResult(
                    "stock_insufficient",
                    points=current_points,
                    personal_count=personal_count,
                    total_count=total_count,
                )
            if uow.query_one(
                "SELECT 1 AS found FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return ActivityPointShopPurchaseResult("user_missing")
            for item_id, _, _, amount in item_rows:
                inventory = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                if int((inventory or {}).get("goods_num") or 0) + amount > max_goods_num:
                    return ActivityPointShopPurchaseResult(
                        "inventory_full",
                        points=current_points,
                        personal_count=personal_count,
                        total_count=total_count,
                    )

            points = current_points - cost
            personal_count += quantity
            total_count += quantity
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            changed = uow.execute(
                "UPDATE activity_point_balance SET points=CAST(COALESCE(points,0) AS REAL)-"
                "CAST(? AS REAL) "
                "WHERE activity_key=? AND user_id=? AND points>=?",
                (cost, activity_key, user_id, cost),
            )
            if changed.rowcount != 1:
                return ActivityPointShopPurchaseResult("state_changed")
            uow.execute(
                "INSERT INTO activity_point_purchase(activity_key,user_id,item_key,count,update_time) "
                "VALUES(?,?,?,?,?) ON CONFLICT(activity_key,user_id,item_key) DO UPDATE SET "
                "count=activity_point_purchase.count+excluded.count,update_time=excluded.update_time",
                (activity_key, user_id, item_key, quantity, now),
            )
            if stone:
                uow.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? "
                    "WHERE user_id=?",
                    (stone, user_id),
                )
            for item_id, name, item_type, amount in item_rows:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                    "goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                    "update_time=excluded.update_time",
                    (user_id, item_id, name, item_type, amount, now, now, amount),
                )
            uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload,quantity,cost,points,"
                "personal_count,total_count) VALUES(?,?,?,?,?,?,?)",
                (
                    operation_id,
                    payload,
                    quantity,
                    cost,
                    points,
                    personal_count,
                    total_count,
                ),
            )
        return ActivityPointShopPurchaseResult(
            "applied", quantity, cost, points, personal_count, total_count
        )


__all__ = ["ActivityPointShopPurchaseResult", "ActivityPointShopPurchaseSqlRepository"]
