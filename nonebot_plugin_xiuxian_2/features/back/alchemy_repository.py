from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AlchemyResult:
    status: str
    user_id: str
    reward_stone: int
    consumed: tuple[tuple[int, int], ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AlchemySqlRepository:
    """Atomically consume backpack items and grant the alchemy reward."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _payload(user_id: str, reward_stone: int, consumed: tuple[tuple[int, int], ...]) -> str:
        return json.dumps([user_id, int(reward_stone), consumed], separators=(",", ":"), ensure_ascii=True)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        reward_stone: int,
        consume_items: list[tuple[int, int]] | tuple[tuple[int, int], ...],
    ) -> AlchemyResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        reward_stone = int(reward_stone)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        normalized: dict[int, int] = {}
        for goods_id, quantity in consume_items:
            goods_id, quantity = int(goods_id), int(quantity)
            if quantity <= 0:
                raise ValueError("alchemy quantity must be positive")
            normalized[goods_id] = normalized.get(goods_id, 0) + quantity
        consumed = tuple(sorted(normalized.items()))
        if reward_stone <= 0 or not consumed:
            raise ValueError("alchemy reward and consumed items must be positive")
        payload = self._payload(user_id, reward_stone, consumed)

        def result(status: str) -> AlchemyResult:
            return AlchemyResult(status, user_id, reward_stone, consumed)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT payload, user_id, reward_stone, consumed FROM alchemy_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return result("operation_conflict")
                return result("duplicate")

            if uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return result("user_missing")

            columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(back)")
            }
            state_expression = "COALESCE(state, 0)" if "state" in columns else "0"
            for goods_id, quantity in consumed:
                row = uow.query_one(
                    f"SELECT goods_num, {state_expression} AS state FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, goods_id),
                )
                if row is None or int(row["goods_num"] or 0) - int(row["state"] or 0) < quantity:
                    return result("item_insufficient")

            for goods_id, quantity in consumed:
                updates = ["goods_num=goods_num-?"]
                params: list[object] = [quantity]
                if "bind_num" in columns:
                    updates.append("bind_num=MIN(COALESCE(bind_num, 0), goods_num-?)")
                    params.append(quantity)
                if "update_time" in columns:
                    updates.append("update_time=CURRENT_TIMESTAMP")
                if "action_time" in columns:
                    updates.append("action_time=CURRENT_TIMESTAMP")
                updated = uow.execute(
                    f"UPDATE back SET {', '.join(updates)} WHERE user_id=? AND goods_id=? "
                    f"AND COALESCE(goods_num, 0)-{state_expression}>=?",
                    (*params, user_id, goods_id, quantity),
                )
                if updated.rowcount != 1:
                    return result("item_insufficient")

            granted = uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) WHERE user_id=?",
                (reward_stone, user_id),
            )
            if granted.rowcount != 1:
                return result("user_missing")
            uow.execute(
                "INSERT INTO alchemy_operations(operation_id,payload,user_id,reward_stone,consumed) VALUES(?,?,?,?,?)",
                (operation_id, payload, user_id, reward_stone, json.dumps(consumed)),
            )
            return result("applied")


__all__ = ["AlchemyResult", "AlchemySqlRepository"]
