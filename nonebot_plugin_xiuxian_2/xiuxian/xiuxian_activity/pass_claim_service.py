from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivityPassClaimResult:
    status: str
    rewards: tuple[tuple[int, str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivityPassClaimService:
    """Claim battle-pass levels and grant their rewards atomically."""

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
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_pass_claim_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def get_result(self, operation_id, user_id=None) -> ActivityPassClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            previous = conn.execute(
                "SELECT payload,result_json FROM activity_pass_claim_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            payload = json.loads(str(previous[0]))
            if user_id is not None and str(payload[0]) != str(user_id):
                return ActivityPassClaimResult("operation_conflict")
            return ActivityPassClaimResult(
                "duplicate",
                tuple(tuple(row) for row in json.loads(str(previous[1]))),
            )

    @staticmethod
    def _normalize_rewards(rewards) -> tuple[tuple[int, str, str, tuple[dict, ...]], ...]:
        normalized = []
        levels = set()
        for reward in rewards:
            level = int(reward["level"])
            if level <= 0 or level in levels:
                raise ValueError("reward levels must be unique positive integers")
            levels.add(level)
            normalized.append(
                (
                    level,
                    str(reward.get("name") or "等级奖励"),
                    str(reward.get("reward") or ""),
                    tuple(reward.get("reward_items") or ()),
                )
            )
        return tuple(sorted(normalized, key=lambda row: row[0]))

    @staticmethod
    def _reward_rows(rewards) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list] = {}
        for _, _, _, reward_items in rewards:
            for reward in reward_items:
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

    def claim(
        self,
        operation_id,
        user_id,
        activity_key,
        current_level,
        rewards,
        max_goods_num,
    ) -> ActivityPassClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        activity_key = str(activity_key)
        current_level = int(current_level)
        max_goods_num = int(max_goods_num)
        normalized = self._normalize_rewards(rewards)
        if (
            not operation_id
            or not activity_key
            or current_level < 0
            or not normalized
            or max_goods_num < 0
            or any(level > current_level for level, *_ in normalized)
        ):
            raise ValueError("valid activity pass claim is required")

        stone, item_rows = self._reward_rows(normalized)
        result_rewards = tuple((level, name, reward_text) for level, name, reward_text, _ in normalized)
        payload = json.dumps(
            [user_id, activity_key, current_level, result_rewards, stone, item_rows, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM activity_pass_claim_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityPassClaimResult("operation_conflict")
                    return ActivityPassClaimResult(
                        "duplicate",
                        tuple(tuple(row) for row in json.loads(str(previous[1]))),
                    )

                balance = conn.execute(
                    "SELECT level FROM activity_pass_balance WHERE activity_key=%s AND user_id=%s",
                    (activity_key, user_id),
                ).fetchone()
                if balance is None or int(balance[0]) != current_level:
                    conn.rollback()
                    return ActivityPassClaimResult("state_changed")
                levels = tuple(level for level, *_ in normalized)
                placeholders = ",".join("%s" for _ in levels)
                claimed = conn.execute(
                    "SELECT level FROM activity_pass_reward_claim WHERE activity_key=%s AND user_id=%s "
                    f"AND level IN ({placeholders})",
                    (activity_key, user_id, *levels),
                ).fetchall()
                if claimed:
                    conn.rollback()
                    return ActivityPassClaimResult("state_changed")
                if conn.execute(
                    "SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return ActivityPassClaimResult("user_missing")
                for item_id, _, _, quantity in item_rows:
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return ActivityPassClaimResult("inventory_full")

                now = datetime.now()
                conn.executemany(
                    "INSERT INTO activity_pass_reward_claim(activity_key,user_id,level,create_time) "
                    "VALUES(%s,%s,%s,%s)",
                    [(activity_key, user_id, level, now) for level in levels],
                )
                if stone:
                    conn.execute(
                        "UPDATE game_data.user_xiuxian SET stone=COALESCE(stone,0)+%s WHERE user_id=%s",
                        (stone, user_id),
                    )
                for item_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                        "goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                result_json = json.dumps(result_rewards, ensure_ascii=True, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO activity_pass_claim_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return ActivityPassClaimResult("applied", result_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE game_data")


__all__ = ["ActivityPassClaimResult", "ActivityPassClaimService"]
