from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivitySignSettlementResult:
    status: str
    sign_days: int = 0
    total_sign_days: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivitySignSettlementService:
    """Persist activity sign state and both reward groups atomically."""

    def __init__(
        self,
        activity_database: str | Path,
        game_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._activity_database = Path(activity_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_rewards(rewards) -> tuple[dict, ...]:
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
    def _reward_text(rewards: tuple[dict, ...]) -> str:
        return ",".join(f"{reward['name']}x{reward['quantity']}" for reward in rewards)

    @staticmethod
    def _reward_rows(
        daily_rewards: tuple[dict, ...], milestone_rewards: tuple[dict, ...]
    ) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list] = {}
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

    def get_result(self, operation_id: str) -> ActivitySignSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS activity_sign_settlement_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sign_days INTEGER NOT NULL,"
                "total_sign_days INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT sign_days,total_sign_days FROM activity_sign_settlement_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return ActivitySignSettlementResult(
                "duplicate", int(previous[0]), int(previous[1])
            )

    def settle(
        self,
        operation_id,
        user_id,
        sign_date,
        expected_sign_days,
        expected_total_sign_days,
        daily_rewards,
        milestone_rewards,
        max_goods_num,
        daily_reward_text: str = "",
        milestone_reward_text: str = "",
    ) -> ActivitySignSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        sign_date = str(sign_date).strip()
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
        # Request identity only — counters/rewards are concurrency checks / outcomes.
        payload = json.dumps(
            [user_id, sign_date, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS activity_sign_settlement_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sign_days INTEGER NOT NULL,"
                    "total_sign_days INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,sign_days,total_sign_days "
                    "FROM activity_sign_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivitySignSettlementResult("operation_conflict")
                    return ActivitySignSettlementResult(
                        "duplicate", int(previous[1]), int(previous[2])
                    )

                row = conn.execute(
                    "SELECT sign_days,last_sign_date,total_sign_days FROM activity_user "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_sign_days = int(row[0]) if row is not None else 0
                last_sign_date = str(row[1] or "") if row is not None else ""
                current_total_sign_days = int(row[2]) if row is not None else 0
                if last_sign_date == sign_date or conn.execute(
                    "SELECT 1 FROM activity_sign_log WHERE user_id=%s AND sign_date=%s",
                    (user_id, sign_date),
                ).fetchone() is not None:
                    conn.rollback()
                    return ActivitySignSettlementResult(
                        "already_signed", current_sign_days, current_total_sign_days
                    )
                if (
                    current_sign_days != expected_sign_days
                    or current_total_sign_days != expected_total_sign_days
                ):
                    conn.rollback()
                    return ActivitySignSettlementResult(
                        "state_changed", current_sign_days, current_total_sign_days
                    )
                if conn.execute(
                    "SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return ActivitySignSettlementResult("user_missing")
                for item_id, _, _, quantity in item_rows:
                    item = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(item[0]) if item else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return ActivitySignSettlementResult("inventory_full")

                now = datetime.now()
                conn.execute(
                    "INSERT INTO activity_sign_log("
                    "user_id,sign_date,day_index,reward,milestone_reward,reward_status,"
                    "reward_message,create_time,finish_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
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
                conn.execute(
                    "INSERT INTO activity_user("
                    "user_id,sign_days,last_sign_date,total_sign_days,create_time,update_time) "
                    "VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                    "sign_days=excluded.sign_days,last_sign_date=excluded.last_sign_date,"
                    "total_sign_days=excluded.total_sign_days,update_time=excluded.update_time",
                    (
                        user_id,
                        next_sign_days,
                        sign_date,
                        next_total_sign_days,
                        now,
                        now,
                    ),
                )
                if stone:
                    conn.execute(
                        "UPDATE game_data.user_xiuxian SET stone=COALESCE(stone,0)+%s "
                        "WHERE user_id=%s",
                        (stone, user_id),
                    )
                for item_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back("
                        "user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                        "goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,"
                        "update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                conn.execute(
                    "INSERT INTO activity_sign_settlement_operations("
                    "operation_id,payload,sign_days,total_sign_days) VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, next_sign_days, next_total_sign_days),
                )
                conn.commit()
                return ActivitySignSettlementResult(
                    "applied", next_sign_days, next_total_sign_days
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["ActivitySignSettlementResult", "ActivitySignSettlementService"]
