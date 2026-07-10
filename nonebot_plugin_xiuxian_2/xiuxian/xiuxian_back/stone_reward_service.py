from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class StoneItemReward:
    status: str
    user_id: str
    item_id: int
    quantity: int
    rewards: tuple[int, ...] = ()

    @property
    def total_stone(self) -> int:
        return sum(self.rewards)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class StoneItemRewardService:
    """Consume an item and grant a pre-rolled stone reward atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stone_item_reward_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                reward_type TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                total_stone INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def apply(
        self,
        operation_id,
        user_id,
        *,
        reward_type: str,
        item_id: int,
        rewards: list[int] | tuple[int, ...],
    ) -> StoneItemReward:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        item_id = int(item_id)
        fixed_rewards = tuple(int(value) for value in rewards)
        if not fixed_rewards or any(value < 0 for value in fixed_rewards):
            raise ValueError("rewards must contain non-negative values")
        quantity = len(fixed_rewards)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    """
                    SELECT user_id, item_id, quantity, rewards_json
                    FROM stone_item_reward_operations WHERE operation_id=%s
                    """,
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return StoneItemReward(
                        "duplicate",
                        str(previous[0]),
                        int(previous[1]),
                        int(previous[2]),
                        tuple(int(value) for value in json.loads(previous[3])),
                    )

                user = conn.execute(
                    "SELECT stone FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return StoneItemReward("user_missing", user_id, item_id, quantity)
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return StoneItemReward("item_insufficient", user_id, item_id, quantity)

                columns = set(conn.column_names("back"))
                bind_update = ""
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                consumed = conn.execute(
                    f"""
                    UPDATE back SET goods_num=goods_num-%s{bind_update}
                    WHERE user_id=%s AND goods_id=%s AND goods_num >= %s
                    """,
                    ((quantity, quantity, user_id, item_id, quantity) if bind_update else
                     (quantity, user_id, item_id, quantity)),
                )
                granted = conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s",
                    (sum(fixed_rewards), user_id),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return StoneItemReward("item_changed", user_id, item_id, quantity)
                if granted.rowcount != 1:
                    conn.rollback()
                    return StoneItemReward("user_changed", user_id, item_id, quantity)

                conn.execute(
                    """
                    INSERT INTO stone_item_reward_operations (
                        operation_id, user_id, reward_type, item_id, quantity,
                        rewards_json, total_stone
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        operation_id,
                        user_id,
                        str(reward_type),
                        item_id,
                        quantity,
                        json.dumps(fixed_rewards),
                        sum(fixed_rewards),
                    ),
                )
                conn.commit()
                return StoneItemReward(
                    "applied", user_id, item_id, quantity, fixed_rewards
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["StoneItemReward", "StoneItemRewardService"]
