from __future__ import annotations

import json
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import PackageReward


class PackageRewardRepository:
    """Owns the game database projection used by package opening."""

    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            """
            CREATE TABLE IF NOT EXISTS package_reward_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                package_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _rewards(payload: str) -> tuple[PackageReward, ...]:
        return tuple(PackageReward(**item) for item in json.loads(payload))

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> dict[str, Any] | None:
        self.ensure_schema(uow)
        row = uow.query_one(
            "SELECT operation_id, user_id, package_id, quantity, rewards_json "
            "FROM package_reward_operations WHERE operation_id = ?",
            (operation_id,),
        )
        if row is None:
            return None
        return {
            "operation_id": str(row["operation_id"]),
            "user_id": str(row["user_id"]),
            "package_id": int(row["package_id"]),
            "quantity": int(row["quantity"]),
            "rewards": self._rewards(row["rewards_json"]),
        }

    def snapshot(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, int]:
        row = uow.query_one(
            "SELECT COUNT(*) AS rows, "
            "CAST(COALESCE(SUM(CAST(COALESCE(stone, 0) AS REAL)), 0) AS INTEGER) AS stone "
            "FROM user_xiuxian WHERE user_id = ?",
            (user_id,),
        )
        return {"rows": int(row["rows"] if row else 0), "stone": int(row["stone"] if row else 0)}

    def apply(
        self,
        uow: DatabaseUnitOfWork,
        *,
        operation_id: str,
        user_id: str,
        package_id: int,
        quantity: int,
        rewards: tuple[PackageReward, ...],
        max_goods_num: int,
    ) -> str:
        if self.snapshot(uow, user_id)["rows"] == 0:
            return "user_missing"
        package = uow.query_one(
            "SELECT goods_num FROM back WHERE user_id = ? AND goods_id = ?",
            (user_id, package_id),
        )
        if package is None or int(package["goods_num"] or 0) < quantity:
            return "item_insufficient"

        columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
        if "bind_num" in columns:
            consumed = uow.execute(
                "UPDATE back SET goods_num = goods_num - ?, "
                "bind_num = MIN(COALESCE(bind_num, 0), goods_num - ?) "
                "WHERE user_id = ? AND goods_id = ? AND goods_num >= ?",
                (quantity, quantity, user_id, package_id, quantity),
            )
        else:
            consumed = uow.execute(
                "UPDATE back SET goods_num = goods_num - ? "
                "WHERE user_id = ? AND goods_id = ? AND goods_num >= ?",
                (quantity, user_id, package_id, quantity),
            )
        if consumed.rowcount != 1:
            return "state_changed"

        stone_delta = sum(item.quantity for item in rewards if item.name == "灵石")
        if stone_delta:
            changed = uow.execute(
                "UPDATE user_xiuxian SET stone = CAST(COALESCE(stone, 0) AS REAL) + CAST(? AS REAL) "
                "WHERE user_id = ? AND CAST(COALESCE(stone, 0) AS REAL) + CAST(? AS REAL) >= 0",
                (stone_delta, user_id, stone_delta),
            )
            if changed.rowcount != 1:
                return "stone_insufficient"

        totals: dict[int, int] = {}
        for reward in rewards:
            if reward.name != "灵石":
                totals[reward.item_id] = totals.get(reward.item_id, 0) + reward.quantity  # type: ignore[index]
        for item_id, amount in totals.items():
            current = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id = ? AND goods_id = ?",
                (user_id, item_id),
            )
            if int(current["goods_num"] or 0) + amount > max_goods_num if current else amount > max_goods_num:
                return "inventory_full"

        for reward in rewards:
            if reward.name == "灵石":
                continue
            uow.execute(
                "INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, bind_num) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                "goods_name = excluded.goods_name, goods_type = excluded.goods_type, "
                "goods_num = MIN(COALESCE(back.goods_num, 0) + excluded.goods_num, ?), "
                "bind_num = MIN(COALESCE(back.bind_num, 0) + excluded.goods_num, "
                "MIN(COALESCE(back.goods_num, 0) + excluded.goods_num, ?))",
                (user_id, reward.item_id, reward.name, reward.item_type, reward.quantity, reward.quantity, max_goods_num, max_goods_num),
            )

        uow.execute(
            "INSERT INTO package_reward_operations(operation_id, user_id, package_id, quantity, rewards_json) "
            "VALUES (?, ?, ?, ?, ?)",
            (operation_id, user_id, package_id, quantity, json.dumps([item.to_dict() for item in rewards], ensure_ascii=False)),
        )
        return "applied"


__all__ = ["PackageRewardRepository"]
