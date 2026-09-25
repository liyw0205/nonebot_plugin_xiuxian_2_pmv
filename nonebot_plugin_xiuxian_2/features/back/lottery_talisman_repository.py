from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class LotteryReward:
    item_id: int
    name: str
    item_type: str
    quantity: int


@dataclass(frozen=True)
class LotteryTalismanResult:
    status: str
    user_id: str
    talisman_id: int
    quantity: int
    rewards: tuple[LotteryReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class LotteryTalismanSqlRepository:
    """Atomically consume talismans and merge pre-rolled item rewards."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[LotteryReward, ...]:
        return tuple(LotteryReward(**item) for item in json.loads(payload))

    def apply(
        self,
        operation_id: str,
        user_id: str,
        talisman_id: int,
        quantity: int,
        rewards,
        *,
        max_goods_num: int,
    ) -> LotteryTalismanResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        talisman_id = int(talisman_id)
        quantity = int(quantity)
        max_goods_num = int(max_goods_num)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if quantity <= 0 or max_goods_num <= 0:
            raise ValueError("quantity and max_goods_num must be positive")
        normalized = tuple(
            reward
            if isinstance(reward, LotteryReward)
            else LotteryReward(int(reward[0]), str(reward[1]), str(reward[2]), int(reward[3]))
            for reward in rewards
        )
        if any(reward.quantity <= 0 for reward in normalized):
            raise ValueError("reward quantities must be positive")

        def result(
            status: str,
            result_quantity: int = quantity,
            result_rewards: tuple[LotteryReward, ...] = normalized,
        ) -> LotteryTalismanResult:
            return LotteryTalismanResult(status, user_id, talisman_id, int(result_quantity), tuple(result_rewards))

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity,rewards_json FROM lottery_talisman_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result("duplicate", int(previous["quantity"]), self._decode_rewards(str(previous["rewards_json"])))

            item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, talisman_id),
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
                f"UPDATE back SET {', '.join(updates)} WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (*params, user_id, talisman_id, quantity),
            )
            if consumed.rowcount != 1:
                return result("state_changed")

            for reward in normalized:
                uow.execute(
                    "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type, "
                    "goods_num=MIN(COALESCE(back.goods_num,0)+excluded.goods_num,?), "
                    "bind_num=MIN(COALESCE(back.bind_num,0)+excluded.goods_num, "
                    "MIN(COALESCE(back.goods_num,0)+excluded.goods_num,?))",
                    (user_id, reward.item_id, reward.name, reward.item_type, reward.quantity, reward.quantity, max_goods_num, max_goods_num),
                )
            payload = json.dumps([asdict(reward) for reward in normalized], ensure_ascii=False)
            uow.execute(
                "INSERT INTO lottery_talisman_operations "
                "(operation_id,user_id,talisman_id,quantity,rewards_json) VALUES(?,?,?,?,?)",
                (operation_id, user_id, talisman_id, quantity, payload),
            )
            return result("applied")


__all__ = ["LotteryReward", "LotteryTalismanResult", "LotteryTalismanSqlRepository"]
