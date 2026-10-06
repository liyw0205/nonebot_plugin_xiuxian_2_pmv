from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class ActivitySignSettlementResult:
    status: str
    sign_days: int = 0
    total_sign_days: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivitySignSettlementSqlRepository:
    """Atomically settle sign state, rewards, and the operation receipt."""

    operation_table = "activity_sign_settlement_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _normalize_rewards(rewards: Any) -> tuple[dict[str, Any], ...]:
        normalized = []
        for reward in rewards or ():
            quantity = int(reward["quantity"])
            if quantity <= 0:
                raise ValueError("reward quantity must be positive")
            reward_type = str(reward["type"])
            if reward_type == "stone":
                normalized.append(
                    {"type": "stone", "id": "stone", "name": "灵石", "quantity": quantity}
                )
                continue
            item_type = reward_type
            if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                item_type = "技能"
            elif item_type in {"法器", "防具"}:
                item_type = "装备"
            normalized.append(
                {
                    "type": item_type,
                    "id": int(reward["id"]),
                    "name": str(reward["name"]),
                    "quantity": quantity,
                }
            )
        return tuple(normalized)

    @staticmethod
    def _reward_text(rewards: tuple[dict[str, Any], ...]) -> str:
        return ",".join(f"{reward['name']}x{reward['quantity']}" for reward in rewards)

    @staticmethod
    def _reward_rows(
        daily_rewards: tuple[dict[str, Any], ...],
        milestone_rewards: tuple[dict[str, Any], ...],
    ) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list[Any]] = {}
        for reward in daily_rewards + milestone_rewards:
            if reward["type"] == "stone":
                stone += int(reward["quantity"])
                continue
            item_id = int(reward["id"])
            metadata = [str(reward["name"]), str(reward["type"])]
            if item_id in items and items[item_id][:2] != metadata:
                raise ValueError("conflicting reward metadata")
            items.setdefault(item_id, metadata + [0])[2] += int(reward["quantity"])
        return stone, tuple(
            (item_id, values[0], values[1], values[2])
            for item_id, values in sorted(items.items())
        )

    @classmethod
    def _assert_schema(cls, uow: DatabaseUnitOfWork) -> None:
        columns = {
            str(row["name"])
            for row in uow.query_all(f"PRAGMA table_info({cls.operation_table})")
        }
        if not {"operation_id", "payload", "sign_days", "total_sign_days"}.issubset(columns):
            raise RuntimeError(
                f"activity_state.001 schema_missing: {cls.operation_table}"
            )

    def get_result(self, operation_id: str) -> ActivitySignSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self._assert_schema(uow)
            previous = uow.query_one(
                f"SELECT sign_days,total_sign_days FROM {self.operation_table} "
                "WHERE operation_id=?",
                (operation_id,),
            )
        if previous is None:
            return None
        return ActivitySignSettlementResult(
            "duplicate", int(previous["sign_days"]), int(previous["total_sign_days"])
        )

    def settle(
        self,
        operation_id: str,
        user_id: str,
        sign_date: str,
        expected_sign_days: int,
        expected_total_sign_days: int,
        daily_rewards: Any,
        milestone_rewards: Any,
        max_goods_num: int,
        daily_reward_text: str = "",
        milestone_reward_text: str = "",
    ) -> ActivitySignSettlementResult:
        operation_id, user_id, sign_date = (
            str(operation_id).strip(), str(user_id), str(sign_date).strip()
        )
        expected_sign_days = int(expected_sign_days)
        expected_total_sign_days = int(expected_total_sign_days)
        max_goods_num = int(max_goods_num)
        daily_rewards = self._normalize_rewards(daily_rewards)
        milestone_rewards = self._normalize_rewards(milestone_rewards)
        if not operation_id or not user_id or not sign_date:
            raise ValueError("operation, user and sign date are required")
        if min(expected_sign_days, expected_total_sign_days, max_goods_num) < 0:
            raise ValueError("sign counters and inventory limit cannot be negative")

        daily_reward_text = str(daily_reward_text or self._reward_text(daily_rewards))
        milestone_reward_text = str(
            milestone_reward_text or self._reward_text(milestone_rewards)
        )
        stone, item_rows = self._reward_rows(daily_rewards, milestone_rewards)
        next_sign_days = expected_sign_days + 1
        next_total_sign_days = expected_total_sign_days + 1
        payload = json.dumps(
            [user_id, sign_date, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_schema(uow)
            previous = uow.query_one(
                f"SELECT payload,sign_days,total_sign_days FROM {self.operation_table} "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return ActivitySignSettlementResult("operation_conflict")
                return ActivitySignSettlementResult(
                    "duplicate",
                    int(previous["sign_days"]),
                    int(previous["total_sign_days"]),
                )

            row = uow.query_one(
                "SELECT sign_days,last_sign_date,total_sign_days FROM activity_user "
                "WHERE user_id=?",
                (user_id,),
            )
            current_sign_days = int(row["sign_days"] or 0) if row else 0
            last_sign_date = str(row["last_sign_date"] or "") if row else ""
            current_total_sign_days = int(row["total_sign_days"] or 0) if row else 0
            if last_sign_date == sign_date or uow.query_one(
                "SELECT 1 AS found FROM activity_sign_log WHERE user_id=? AND sign_date=?",
                (user_id, sign_date),
            ):
                return ActivitySignSettlementResult(
                    "already_signed", current_sign_days, current_total_sign_days
                )
            if (current_sign_days, current_total_sign_days) != (
                expected_sign_days,
                expected_total_sign_days,
            ):
                return ActivitySignSettlementResult(
                    "state_changed", current_sign_days, current_total_sign_days
                )
            if uow.query_one(
                "SELECT 1 AS found FROM user_xiuxian WHERE user_id=?", (user_id,)
            ) is None:
                return ActivitySignSettlementResult("user_missing")

            for item_id, _, _, quantity in item_rows:
                item = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back "
                    "WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                if (int(item["goods_num"] or 0) if item else 0) + quantity > max_goods_num:
                    return ActivitySignSettlementResult("inventory_full")

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,day_index,reward,"
                "milestone_reward,reward_status,reward_message,create_time,finish_time) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    user_id,
                    sign_date,
                    next_sign_days,
                    daily_reward_text,
                    milestone_reward_text,
                    "success",
                    self._reward_text(daily_rewards + milestone_rewards),
                    now,
                    now,
                ),
            )
            uow.execute(
                "INSERT INTO activity_user(user_id,sign_days,last_sign_date,total_sign_days,"
                "create_time,update_time) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
                "sign_days=excluded.sign_days,last_sign_date=excluded.last_sign_date,"
                "total_sign_days=excluded.total_sign_days,update_time=excluded.update_time",
                (user_id, next_sign_days, sign_date, next_total_sign_days, now, now),
            )
            if stone:
                uow.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+? "
                    "WHERE user_id=?",
                    (stone, user_id),
                )
            for item_id, name, item_type, quantity in item_rows:
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                    "goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                    "update_time=excluded.update_time",
                    (user_id, item_id, name, item_type, quantity, now, now, quantity),
                )
            uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload,sign_days,total_sign_days) "
                "VALUES(?,?,?,?)",
                (operation_id, payload, next_sign_days, next_total_sign_days),
            )
        return ActivitySignSettlementResult(
            "applied", next_sign_days, next_total_sign_days
        )


__all__ = ["ActivitySignSettlementResult", "ActivitySignSettlementSqlRepository"]
