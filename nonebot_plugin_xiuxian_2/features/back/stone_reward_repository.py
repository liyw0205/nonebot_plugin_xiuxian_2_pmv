from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class StoneRewardResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    rewards: tuple[int, ...] = ()
    reward_type: str = ""

    @property
    def total_stone(self) -> int:
        return sum(self.rewards)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class StoneRewardSqlRepository:
    """Atomically consume a reward item and grant pre-rolled spirit stones."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        *,
        reward_type: str,
        item_id: int,
        rewards: list[int] | tuple[int, ...],
    ) -> StoneRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        reward_type = str(reward_type)
        item_id = int(item_id)
        fixed_rewards = tuple(int(value) for value in rewards)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not reward_type:
            raise ValueError("reward_type must not be empty")
        if not fixed_rewards or any(value < 0 for value in fixed_rewards):
            raise ValueError("rewards must contain non-negative values")
        quantity = len(fixed_rewards)

        def result(
            status: str,
            result_user_id: str = user_id,
            result_item_id: int = item_id,
            result_quantity: int = quantity,
            result_rewards: tuple[int, ...] = fixed_rewards,
            result_reward_type: str = reward_type,
        ) -> StoneRewardResult:
            return StoneRewardResult(
                status,
                str(result_user_id),
                int(result_item_id),
                int(result_quantity),
                tuple(result_rewards),
                str(result_reward_type),
            )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT user_id,item_id,quantity,reward_type,rewards_json "
                "FROM stone_item_reward_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result(
                    "duplicate",
                    previous["user_id"],
                    int(previous["item_id"]),
                    int(previous["quantity"]),
                    tuple(int(value) for value in json.loads(str(previous["rewards_json"]))),
                    str(previous["reward_type"]),
                )

            if uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return result("user_missing")
            item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None or int(item["goods_num"] or 0) < quantity:
                return result("item_insufficient")

            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            updates = ["goods_num=goods_num-?"]
            params: list[object] = [quantity]
            if "bind_num" in columns:
                updates.append("bind_num=MIN(COALESCE(bind_num, 0), goods_num-?)")
                params.append(quantity)
            for column in ("update_time", "action_time"):
                if column in columns:
                    updates.append(f"{column}=CURRENT_TIMESTAMP")
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(updates)} "
                "WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (*params, user_id, item_id, quantity),
            )
            granted = uow.execute(
                "UPDATE user_xiuxian SET "
                "stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) "
                "WHERE user_id=?",
                (sum(fixed_rewards), user_id),
            )
            if consumed.rowcount != 1:
                return result("item_changed")
            if granted.rowcount != 1:
                return result("user_changed")

            uow.execute(
                "INSERT INTO stone_item_reward_operations "
                "(operation_id,user_id,reward_type,item_id,quantity,rewards_json,total_stone) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    reward_type,
                    item_id,
                    quantity,
                    json.dumps(fixed_rewards),
                    sum(fixed_rewards),
                ),
            )
            return result("applied")


__all__ = ["StoneRewardResult", "StoneRewardSqlRepository"]
