from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class LotteryReward:
    item_id: int
    name: str
    item_type: str
    quantity: int


@dataclass(frozen=True)
class LotteryTalismanUse:
    status: str
    user_id: str
    talisman_id: int
    quantity: int
    rewards: tuple[LotteryReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class LotteryTalismanService:
    """Consume talismans and grant pre-rolled rewards atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lottery_talisman_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                talisman_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rewards_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[LotteryReward, ...]:
        return tuple(LotteryReward(**item) for item in json.loads(payload))

    def apply(
        self,
        operation_id,
        user_id,
        talisman_id,
        quantity,
        rewards,
        *,
        max_goods_num,
    ) -> LotteryTalismanUse:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        talisman_id = int(talisman_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        if quantity <= 0 or max_goods_num <= 0:
            raise ValueError("quantity and max_goods_num must be positive")
        normalized = tuple(
            reward if isinstance(reward, LotteryReward) else LotteryReward(
                int(reward[0]), str(reward[1]), str(reward[2]), int(reward[3])
            )
            for reward in rewards
        )
        if any(reward.quantity <= 0 for reward in normalized):
            raise ValueError("reward quantities must be positive")

        def result(status: str, result_quantity=quantity, result_rewards=normalized):
            return LotteryTalismanUse(
                status, user_id, talisman_id, int(result_quantity), tuple(result_rewards)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT quantity, rewards_json FROM lottery_talisman_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], self._decode_rewards(previous[1]))
                item = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, talisman_id),
                ).fetchone()
                if item is None or int(item[0] or 0) < quantity:
                    conn.rollback()
                    return result("item_insufficient")

                columns = set(conn.column_names("back"))
                bind_update = ""
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    bind_update = ", bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s)"
                    params.append(quantity)
                consumed = conn.execute(
                    f"UPDATE back SET goods_num=goods_num-%s{bind_update} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (*params, user_id, talisman_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")

                for reward in normalized:
                    conn.execute(
                        "INSERT INTO back (user_id, goods_id, goods_name, goods_type, "
                        "goods_num, bind_num) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s), "
                        "bind_num=MIN(COALESCE(back.bind_num, 0)+EXCLUDED.goods_num, "
                        "MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s))",
                        (
                            user_id, reward.item_id, reward.name, reward.item_type,
                            reward.quantity, reward.quantity, max_goods_num, max_goods_num,
                        ),
                    )
                payload = json.dumps(
                    [reward.__dict__ for reward in normalized], ensure_ascii=False
                )
                conn.execute(
                    "INSERT INTO lottery_talisman_operations "
                    "(operation_id, user_id, talisman_id, quantity, rewards_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, talisman_id, quantity, payload),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise


__all__ = ["LotteryReward", "LotteryTalismanService", "LotteryTalismanUse"]
