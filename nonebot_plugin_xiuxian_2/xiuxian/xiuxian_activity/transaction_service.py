from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from datetime import datetime
from typing import Callable, Mapping
import hashlib
from ..xiuxian_compensation.common import get_item_list
from ..xiuxian_config import XiuConfig

@dataclass(frozen=True)
class ActivityBossSettlementResult:
    status: str
    damage: int = 0
    hp_left: int = 0
    max_hp: int = 0
    fight_count: int = 0
    inventory: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ActivityBossSettlementService:
    """Atomic activity boss damage settlement shared by command entry points."""

    operation_table = "activity_boss_settlement_operations"

    def __init__(self, activity_database: str | Path, lock: RLock | None = None) -> None:
        self.activity_database = Path(activity_database)
        self.lock = lock or RLock()

    @staticmethod
    def _json(value) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {cls.operation_table}("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,damage INTEGER NOT NULL,"
            "hp_left INTEGER NOT NULL,max_hp INTEGER NOT NULL,fight_count INTEGER NOT NULL,"
            "inventory INTEGER,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _fight_count(conn, activity_key: str, user_id: str, fight_date: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) FROM activity_boss_fight_log "
            "WHERE activity_key=%s AND user_id=%s AND fight_date=%s "
            "AND source IN ('coop','world_boss','item')",
            (activity_key, user_id, fight_date),
        ).fetchone()
        return int(row[0] or 0)

    @staticmethod
    def _unlock_milestones(conn, activity_key: str, hp_left: int, max_hp: int, milestones, timestamp: str) -> None:
        if max_hp <= 0:
            return
        percent_left = 100.0 * hp_left / max_hp
        for milestone in milestones:
            threshold = float(milestone.get("hp_percent", 0))
            if threshold <= 0 or percent_left > threshold:
                continue
            key = str(milestone.get("key") or f"p{threshold}")
            conn.execute(
                "INSERT OR IGNORE INTO activity_boss_milestone(activity_key,milestone_key,unlocked_time) "
                "VALUES(%s,%s,%s)",
                (activity_key, key, timestamp),
            )

    def _settle(
        self,
        *,
        operation_id,
        user_id,
        activity_key,
        expected_hp,
        expected_max_hp,
        expected_fight_count,
        daily_limit,
        fixed_damage,
        fight_date,
        source,
        timestamp,
        milestones=(),
        item_id=None,
        expected_inventory=None,
        item_cost=0,
    ) -> ActivityBossSettlementResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key = str(user_id), str(activity_key).strip()
        fight_date, source, timestamp = str(fight_date), str(source), str(timestamp)
        expected_hp, expected_max_hp, expected_fight_count = map(
            int, (expected_hp, expected_max_hp, expected_fight_count)
        )
        daily_limit, fixed_damage, item_cost = map(int, (daily_limit, fixed_damage, item_cost))
        item_id = None if item_id is None else str(item_id)
        expected_inventory = None if expected_inventory is None else int(expected_inventory)
        milestone_rows = tuple(
            (str(row.get("key") or ""), float(row.get("hp_percent", 0))) for row in milestones
        )
        if (
            not operation_id
            or not activity_key
            or not fight_date
            or source not in {"coop", "item"}
            or expected_hp < 0
            or expected_max_hp <= 0
            or expected_fight_count < 0
            or daily_limit <= 0
            or fixed_damage <= 0
            or item_cost < 0
        ):
            raise ValueError("valid activity boss settlement inputs are required")
        if source == "item" and (not item_id or expected_inventory is None or item_cost <= 0):
            raise ValueError("item settlement requires inventory snapshot and cost")

        payload = self._json(
            [
                user_id, activity_key, expected_max_hp, daily_limit, fixed_damage,
                fight_date, source, milestone_rows, item_id, item_cost,
            ]
        )
        with self.lock, closing(db_backend.connect(self.activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    f"SELECT payload,damage,hp_left,max_hp,fight_count,inventory FROM {self.operation_table} "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityBossSettlementResult("operation_conflict")
                    return ActivityBossSettlementResult(
                        "duplicate", int(previous[1]), int(previous[2]), int(previous[3]),
                        int(previous[4]), None if previous[5] is None else int(previous[5]),
                    )

                state = conn.execute(
                    "SELECT hp_left,max_hp FROM activity_boss_state WHERE activity_key=%s",
                    (activity_key,),
                ).fetchone()
                if state is None:
                    if expected_hp != expected_max_hp:
                        conn.rollback()
                        return ActivityBossSettlementResult("state_changed")
                    conn.execute(
                        "INSERT INTO activity_boss_state(activity_key,hp_left,max_hp,update_time) VALUES(%s,%s,%s,%s)",
                        (activity_key, expected_hp, expected_max_hp, timestamp),
                    )
                elif (int(state[0]), int(state[1])) != (expected_hp, expected_max_hp):
                    conn.rollback()
                    return ActivityBossSettlementResult("state_changed")
                if expected_hp <= 0:
                    conn.rollback()
                    return ActivityBossSettlementResult("boss_defeated", 0, expected_hp, expected_max_hp, expected_fight_count)

                fight_count = self._fight_count(conn, activity_key, user_id, fight_date)
                if fight_count != expected_fight_count:
                    conn.rollback()
                    return ActivityBossSettlementResult("state_changed")
                if fight_count >= daily_limit:
                    conn.rollback()
                    return ActivityBossSettlementResult("limit_reached", 0, expected_hp, expected_max_hp, fight_count)

                inventory_left = None
                if source == "item":
                    inventory = conn.execute(
                        "SELECT count FROM activity_item_inventory "
                        "WHERE activity_key=%s AND user_id=%s AND item_id=%s",
                        (activity_key, user_id, item_id),
                    ).fetchone()
                    current_inventory = int(inventory[0] or 0) if inventory else 0
                    if current_inventory != expected_inventory:
                        conn.rollback()
                        return ActivityBossSettlementResult("state_changed")
                    if current_inventory < item_cost:
                        conn.rollback()
                        return ActivityBossSettlementResult("item_insufficient", inventory=current_inventory)
                    inventory_left = current_inventory - item_cost
                    updated = conn.execute(
                        "UPDATE activity_item_inventory SET count=count-%s,update_time=%s "
                        "WHERE activity_key=%s AND user_id=%s AND item_id=%s AND count=%s",
                        (item_cost, timestamp, activity_key, user_id, item_id, current_inventory),
                    )
                    if updated.rowcount != 1:
                        conn.rollback()
                        return ActivityBossSettlementResult("state_changed")

                damage = min(fixed_damage, expected_hp)
                hp_left = expected_hp - damage
                updated = conn.execute(
                    "UPDATE activity_boss_state SET hp_left=%s,update_time=%s "
                    "WHERE activity_key=%s AND hp_left=%s AND max_hp=%s",
                    (hp_left, timestamp, activity_key, expected_hp, expected_max_hp),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return ActivityBossSettlementResult("state_changed")
                conn.execute(
                    "INSERT INTO activity_boss_damage(activity_key,user_id,total_damage,update_time) "
                    "VALUES(%s,%s,%s,%s) ON CONFLICT(activity_key,user_id) DO UPDATE SET "
                    "total_damage=activity_boss_damage.total_damage+excluded.total_damage,update_time=excluded.update_time",
                    (activity_key, user_id, damage, timestamp),
                )
                conn.execute(
                    "INSERT INTO activity_boss_fight_log(activity_key,user_id,damage,fight_date,source,create_time) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    (activity_key, user_id, damage, fight_date, source, timestamp),
                )
                self._unlock_milestones(conn, activity_key, hp_left, expected_max_hp, milestones, timestamp)
                fight_count += 1
                conn.execute(
                    f"INSERT INTO {self.operation_table}(operation_id,payload,damage,hp_left,max_hp,fight_count,inventory) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, damage, hp_left, expected_max_hp, fight_count, inventory_left),
                )
                conn.commit()
                return ActivityBossSettlementResult(
                    "applied", damage, hp_left, expected_max_hp, fight_count, inventory_left
                )
            except Exception:
                conn.rollback()
                raise

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
                        "UPDATE game_data.user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) "
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

@dataclass(frozen=True)
class ActivityTaskClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ActivityTaskClaimService:
    """Claim activity task state and game rewards in one transaction."""

    def __init__(self, activity_database: str | Path, game_database: str | Path, lock: RLock | None = None) -> None:
        self._activity_database = Path(activity_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_task_claim_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def get_result(self, operation_id, user_id=None) -> ActivityTaskClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            previous = conn.execute(
                "SELECT payload,result_json FROM activity_task_claim_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            payload = json.loads(str(previous[0]))
            if user_id is not None and str(payload[0]) != str(user_id):
                return ActivityTaskClaimResult("operation_conflict")
            return ActivityTaskClaimResult(
                "duplicate",
                tuple(tuple(row) for row in json.loads(str(previous[1]))),
            )

    @staticmethod
    def _reward_rows(tasks) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list] = {}
        for task in tasks:
            for reward in task[5]:
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
        return stone, tuple((item_id, row[0], row[1], row[2]) for item_id, row in sorted(items.items()))

    def claim(self, operation_id, user_id, activity_key, tasks, max_goods_num) -> ActivityTaskClaimResult:
        operation_id, user_id, activity_key = map(str, (operation_id, user_id, activity_key))
        max_goods_num = int(max_goods_num)
        normalized = tuple(
            (str(task[0]), str(task[1]), str(task[2]), int(task[3]), str(task[4]), tuple(task[5]), str(task[6]))
            for task in tasks
        )
        if not operation_id.strip() or not activity_key or not normalized or max_goods_num < 0:
            raise ValueError("valid task claim is required")
        stone, item_rows = self._reward_rows(normalized)
        payload = json.dumps(
            [user_id, activity_key, [(row[0], row[1], row[2], row[3], row[4], row[6]) for row in normalized], stone, item_rows, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        rewards = tuple((row[6], row[4]) for row in normalized)

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM activity_task_claim_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityTaskClaimResult("operation_conflict")
                    return ActivityTaskClaimResult("duplicate", tuple(tuple(row) for row in json.loads(previous[1])))
                if conn.execute("SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return ActivityTaskClaimResult("user_missing")
                for task_key, scope_type, scope_key, target, _, _, _ in normalized:
                    row = conn.execute(
                        "SELECT progress,claimed FROM activity_task_progress WHERE activity_key=%s AND user_id=%s "
                        "AND scope_type=%s AND scope_key=%s AND task_key=%s",
                        (activity_key, user_id, scope_type, scope_key, task_key),
                    ).fetchone()
                    if row is None or int(row[1]) or int(row[0]) < target:
                        conn.rollback()
                        return ActivityTaskClaimResult("state_changed")
                for item_id, _, _, quantity in item_rows:
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(row[0]) if row else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return ActivityTaskClaimResult("inventory_full")

                now = datetime.now()
                for task_key, scope_type, scope_key, target, reward_text, _, _ in normalized:
                    changed = conn.execute(
                        "UPDATE activity_task_progress SET claimed=1,claim_time=%s,update_time=%s,target=%s "
                        "WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s "
                        "AND claimed=0 AND progress>=%s",
                        (now, now, target, activity_key, user_id, scope_type, scope_key, task_key, target),
                    )
                    if changed.rowcount != 1:
                        raise RuntimeError("activity task state changed")
                    conn.execute(
                        "INSERT INTO activity_task_claim_log(activity_key,user_id,scope_type,scope_key,task_key,reward,create_time) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                        (activity_key, user_id, scope_type, scope_key, task_key, reward_text, now),
                    )
                if stone:
                    conn.execute("UPDATE game_data.user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
                for item_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                result_json = json.dumps(rewards, ensure_ascii=True, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO activity_task_claim_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return ActivityTaskClaimResult("applied", rewards)
            except Exception:
                conn.rollback()
                raise

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
                        "UPDATE game_data.user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
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

class ActivityPointShopPurchaseService:
    """Settle an activity point purchase and all rewards atomically."""

    def __init__(self, activity_database: str | Path, game_database: str | Path, lock: RLock | None = None) -> None:
        self._activity_database = Path(activity_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _reward_rows(rewards) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list] = {}
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
        self, operation_id, user_id, activity_key, item_key, quantity, unit_cost,
        personal_limit, stock_limit, rewards, max_goods_num,
    ) -> ActivityPointShopPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key, item_key = map(str, (user_id, activity_key, item_key))
        quantity, unit_cost, personal_limit, stock_limit, max_goods_num = map(
            int, (quantity, unit_cost, personal_limit, stock_limit, max_goods_num)
        )
        stone, item_rows = self._reward_rows(rewards)
        if not operation_id or not activity_key or not item_key or quantity <= 0 or unit_cost <= 0 or min(personal_limit, stock_limit, max_goods_num) < 0:
            raise ValueError("valid activity point purchase is required")
        cost = quantity * unit_cost
        payload = json.dumps(
            [user_id, activity_key, item_key, quantity, unit_cost, personal_limit,
             stock_limit, stone, item_rows, max_goods_num],
            ensure_ascii=True, separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS activity_point_purchase_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,"
                    "cost INTEGER NOT NULL,points INTEGER NOT NULL,personal_count INTEGER NOT NULL,"
                    "total_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,quantity,cost,points,personal_count,total_count "
                    "FROM activity_point_purchase_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityPointShopPurchaseResult("operation_conflict")
                    return ActivityPointShopPurchaseResult("duplicate", *(int(value) for value in previous[1:]))

                balance = conn.execute(
                    "SELECT COALESCE(points,0) FROM activity_point_balance WHERE activity_key=%s AND user_id=%s",
                    (activity_key, user_id),
                ).fetchone()
                if balance is None or int(balance[0]) < cost:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("points_insufficient", points=int(balance[0]) if balance else 0)
                personal = conn.execute(
                    "SELECT COALESCE(count,0) FROM activity_point_purchase "
                    "WHERE activity_key=%s AND user_id=%s AND item_key=%s",
                    (activity_key, user_id, item_key),
                ).fetchone()
                personal_count = int(personal[0]) if personal else 0
                total_count = int(conn.execute(
                    "SELECT COALESCE(SUM(count),0) FROM activity_point_purchase WHERE activity_key=%s AND item_key=%s",
                    (activity_key, item_key),
                ).fetchone()[0])
                if personal_limit > 0 and personal_count + quantity > personal_limit:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("personal_limit", points=int(balance[0]), personal_count=personal_count, total_count=total_count)
                if stock_limit > 0 and total_count + quantity > stock_limit:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("stock_insufficient", points=int(balance[0]), personal_count=personal_count, total_count=total_count)
                if conn.execute("SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("user_missing")
                for item_id, _, _, amount in item_rows:
                    current = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(current[0]) if current else 0) + amount > max_goods_num:
                        conn.rollback()
                        return ActivityPointShopPurchaseResult("inventory_full")

                points = int(balance[0]) - cost
                personal_count += quantity
                total_count += quantity
                now = datetime.now()
                changed = conn.execute(
                    "UPDATE activity_point_balance SET points=CAST(COALESCE(points,0) AS REAL)-CAST(%s AS REAL),update_time=%s "
                    "WHERE activity_key=%s AND user_id=%s AND points >= %s",
                    (cost, now, activity_key, user_id, cost),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return ActivityPointShopPurchaseResult("state_changed")
                conn.execute(
                    "INSERT INTO activity_point_purchase(activity_key,user_id,item_key,count,update_time) "
                    "VALUES (%s,%s,%s,%s,%s) ON CONFLICT(activity_key,user_id,item_key) DO UPDATE SET "
                    "count=activity_point_purchase.count+excluded.count,update_time=excluded.update_time",
                    (activity_key, user_id, item_key, quantity, now),
                )
                if stone:
                    conn.execute("UPDATE game_data.user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
                for item_id, name, item_type, amount in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute(
                    "INSERT INTO activity_point_purchase_operations(operation_id,payload,quantity,cost,points,personal_count,total_count) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, quantity, cost, points, personal_count, total_count),
                )
                conn.commit()
                return ActivityPointShopPurchaseResult("applied", quantity, cost, points, personal_count, total_count)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE game_data")

@dataclass(frozen=True)
class ActivityCollectExchangeResult:
    status: str
    claim_count: int = 0
    missing: tuple[tuple[str, int], ...] = ()
    rewards: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ActivityCollectExchangeService:
    """Exchange collect-word tokens and grant rewards in one transaction."""

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
    def _reward_rows(rewards) -> tuple[int, tuple[tuple[int, str, str, int], ...], tuple[str, ...]]:
        stone = 0
        items: dict[int, list] = {}
        descriptions: list[str] = []
        for reward in rewards:
            quantity = int(reward["quantity"])
            if quantity <= 0:
                raise ValueError("reward quantity must be positive")
            descriptions.append(str(reward.get("desc") or f"获得 {reward.get('name', '')}x{quantity}"))
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
        ), tuple(descriptions)

    def exchange(
        self,
        operation_id,
        user_id,
        activity_key,
        phrase,
        required_tokens,
        limit,
        rewards,
        max_goods_num,
    ) -> ActivityCollectExchangeResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key, phrase = map(str, (user_id, activity_key, phrase))
        limit, max_goods_num = int(limit), int(max_goods_num)
        token_rows = tuple(sorted(
            (str(word_char), int(quantity))
            for word_char, quantity in dict(required_tokens).items()
        ))
        if (
            not operation_id
            or not activity_key
            or not phrase
            or not token_rows
            or any(not word_char or quantity <= 0 for word_char, quantity in token_rows)
            or limit < 0
            or max_goods_num < 0
        ):
            raise ValueError("valid collect exchange is required")

        stone, item_rows, reward_descriptions = self._reward_rows(rewards)
        payload = json.dumps(
            [user_id, activity_key, phrase, token_rows, limit, stone, item_rows, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS activity_collect_exchange_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM activity_collect_exchange_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityCollectExchangeResult("operation_conflict")
                    previous_result = json.loads(previous[1])
                    return ActivityCollectExchangeResult(
                        "duplicate",
                        int(previous_result[0]),
                        rewards=tuple(previous_result[1]),
                    )

                if conn.execute(
                    "SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return ActivityCollectExchangeResult("user_missing")

                claim_row = conn.execute(
                    "SELECT COALESCE(count,0) FROM activity_collect_claim "
                    "WHERE activity_key=%s AND user_id=%s AND phrase=%s",
                    (activity_key, user_id, phrase),
                ).fetchone()
                claim_count = int(claim_row[0]) if claim_row else 0
                if limit > 0 and claim_count >= limit:
                    conn.rollback()
                    return ActivityCollectExchangeResult("limit_reached", claim_count)

                missing: list[tuple[str, int]] = []
                for word_char, quantity in token_rows:
                    inventory = conn.execute(
                        "SELECT COALESCE(count,0) FROM activity_collect_inventory "
                        "WHERE activity_key=%s AND user_id=%s AND word_char=%s",
                        (activity_key, user_id, word_char),
                    ).fetchone()
                    owned = int(inventory[0]) if inventory else 0
                    if owned < quantity:
                        missing.append((word_char, quantity - owned))
                if missing:
                    conn.rollback()
                    return ActivityCollectExchangeResult("tokens_insufficient", claim_count, tuple(missing))

                for item_id, _, _, quantity in item_rows:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return ActivityCollectExchangeResult("inventory_full", claim_count)

                now = datetime.now()
                for word_char, quantity in token_rows:
                    changed = conn.execute(
                        "UPDATE activity_collect_inventory SET count=count-%s,update_time=%s "
                        "WHERE activity_key=%s AND user_id=%s AND word_char=%s AND count>=%s",
                        (quantity, now, activity_key, user_id, word_char, quantity),
                    )
                    if changed.rowcount != 1:
                        raise RuntimeError("collect inventory state changed")
                conn.execute(
                    "INSERT INTO activity_collect_claim(activity_key,user_id,phrase,count,update_time) "
                    "VALUES(%s,%s,%s,1,%s) ON CONFLICT(activity_key,user_id,phrase) DO UPDATE SET "
                    "count=activity_collect_claim.count+1,update_time=excluded.update_time",
                    (activity_key, user_id, phrase, now),
                )
                claim_count += 1

                if stone:
                    conn.execute(
                        "UPDATE game_data.user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s",
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

                result_json = json.dumps(
                    [claim_count, reward_descriptions], ensure_ascii=True, separators=(",", ":")
                )
                conn.execute(
                    "INSERT INTO activity_collect_exchange_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return ActivityCollectExchangeResult(
                    "applied", claim_count, rewards=reward_descriptions
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class ActivityClaimAllStepResult:
    name: str
    ok: bool
    text: str

@dataclass(frozen=True)
class ActivityClaimAllResult:
    status: str
    ok: bool = False
    text: str = ""
    steps: tuple[ActivityClaimAllStepResult, ...] = ()

    @property
    def completed(self) -> bool:
        return self.status in {"applied", "duplicate"}

class ActivityClaimAllService:
    """Run the fixed activity reward steps with durable retry progress."""

    STEP_LABELS = {
        "tasks": "任务",
        "pass": "战令",
        "boss_milestone": "首领进度",
        "boss_rank": "首领排行",
    }

    def __init__(self, activity_database: str | Path, lock: RLock | None = None) -> None:
        self._activity_database = Path(activity_database)
        self._lock = lock or RLock()

    @classmethod
    def step_names(cls) -> tuple[str, ...]:
        return tuple(cls.STEP_LABELS)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_claim_all_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "result_json TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_claim_all_steps("
            "operation_id TEXT NOT NULL,step_name TEXT NOT NULL,ordinal INTEGER NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,"
            "ok INTEGER,result_text TEXT NOT NULL DEFAULT '',error_text TEXT NOT NULL DEFAULT '',"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,step_name))"
        )

    @staticmethod
    def _encode_result(ok: bool, text: str, steps: tuple[ActivityClaimAllStepResult, ...]) -> str:
        return json.dumps(
            {
                "ok": bool(ok),
                "text": str(text),
                "steps": [
                    {"name": step.name, "ok": step.ok, "text": step.text}
                    for step in steps
                ],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode_result(status: str, raw: str) -> ActivityClaimAllResult:
        data = json.loads(raw)
        steps = tuple(
            ActivityClaimAllStepResult(str(row["name"]), bool(row["ok"]), str(row["text"]))
            for row in data.get("steps") or ()
        )
        return ActivityClaimAllResult(status, bool(data.get("ok")), str(data.get("text") or ""), steps)

    def _load_or_create(self, operation_id: str, user_id: str) -> ActivityClaimAllResult | None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT user_id,status,result_json FROM activity_claim_all_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO activity_claim_all_operations(operation_id,user_id,status) "
                        "VALUES(%s,%s,'pending')",
                        (operation_id, user_id),
                    )
                    conn.executemany(
                        "INSERT INTO activity_claim_all_steps(operation_id,step_name,ordinal) "
                        "VALUES(%s,%s,%s)",
                        [
                            (operation_id, step_name, ordinal)
                            for ordinal, step_name in enumerate(self.step_names())
                        ],
                    )
                    conn.commit()
                    return None
                if str(row[0]) != user_id:
                    conn.rollback()
                    return ActivityClaimAllResult("operation_conflict", text="领取请求冲突，请重新发送")
                names = tuple(
                    str(step[0])
                    for step in conn.execute(
                        "SELECT step_name FROM activity_claim_all_steps WHERE operation_id=%s "
                        "ORDER BY ordinal",
                        (operation_id,),
                    ).fetchall()
                )
                if names != self.step_names():
                    conn.rollback()
                    return ActivityClaimAllResult("operation_conflict", text="领取计划冲突，请重新发送")
                if str(row[1]) == "completed" and str(row[2]):
                    conn.rollback()
                    return self._decode_result("duplicate", str(row[2]))
                conn.commit()
                return None
            except Exception:
                conn.rollback()
                raise

    def _step_rows(self, operation_id: str) -> tuple[ActivityClaimAllStepResult, ...]:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                "SELECT step_name,ok,result_text FROM activity_claim_all_steps "
                "WHERE operation_id=%s AND status='completed' ORDER BY ordinal",
                (operation_id,),
            ).fetchall()
            return tuple(
                ActivityClaimAllStepResult(str(row[0]), bool(row[1]), str(row[2]))
                for row in rows
            )

    def _completed_step_names(self, operation_id: str) -> set[str]:
        return {step.name for step in self._step_rows(operation_id)}

    def _start_step(self, operation_id: str, step_name: str) -> None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_steps SET status='running',attempts=attempts+1,"
                    "error_text='',updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s "
                    "AND step_name=%s AND status!='completed'",
                    (operation_id, step_name),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _complete_step(self, operation_id: str, step_name: str, ok: bool, text: str) -> None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_steps SET status='completed',ok=%s,result_text=%s,"
                    "error_text='',updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s AND step_name=%s "
                    "AND status!='completed'",
                    (int(bool(ok)), str(text), operation_id, step_name),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _fail_step(self, operation_id: str, step_name: str, error: str) -> None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_steps SET status='failed_retryable',error_text=%s,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s AND step_name=%s "
                    "AND status!='completed'",
                    (str(error), operation_id, step_name),
                )
                conn.execute(
                    "UPDATE activity_claim_all_operations SET status='pending',"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (operation_id,),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @classmethod
    def _format_completed(cls, steps: tuple[ActivityClaimAllStepResult, ...]) -> tuple[bool, str]:
        successes = [step.text for step in steps if step.ok]
        if successes:
            return True, "\n\n".join(successes)
        lines = ["暂无可领取奖励"]
        lines.extend(f"{cls.STEP_LABELS[step.name]}：{step.text}" for step in steps)
        return False, "\n".join(lines)

    def _finish(self, operation_id: str) -> ActivityClaimAllResult:
        steps = self._step_rows(operation_id)
        if tuple(step.name for step in steps) != self.step_names():
            raise RuntimeError("activity claim-all plan is incomplete")
        ok, text = self._format_completed(steps)
        result_json = self._encode_result(ok, text, steps)
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_operations SET status='completed',result_json=%s,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (result_json, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return ActivityClaimAllResult("applied", ok, text, steps)

    def run(
        self,
        operation_id: str,
        user_id: str,
        runners: Mapping[str, Callable[[str], tuple[bool, str]]],
    ) -> ActivityClaimAllResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id or not user_id or tuple(runners) != self.step_names():
            raise ValueError("fixed activity claim-all operation is required")

        existing = self._load_or_create(operation_id, user_id)
        if existing is not None:
            return existing

        completed = self._completed_step_names(operation_id)
        for step_name, runner in runners.items():
            if step_name in completed:
                continue
            self._start_step(operation_id, step_name)
            child_operation_id = f"{operation_id}:{step_name.replace('_', '-')}"
            try:
                ok, result_text = runner(child_operation_id)
                self._complete_step(operation_id, step_name, ok, result_text)
            except Exception as exc:
                self._fail_step(operation_id, step_name, str(exc))
                label = self.STEP_LABELS[step_name]
                return ActivityClaimAllResult(
                    "retryable_failure",
                    False,
                    f"活动奖励领取未完成，请重试\n{label}：{exc}",
                    self._step_rows(operation_id),
                )
        return self._finish(operation_id)

@dataclass(frozen=True)
class BossRewardClaimResult:
    status: str
    names: tuple[str, ...] = ()
    rank: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class BossRewardClaimService:
    def __init__(self, activity_database, game_database, lock=None, max_goods_num=None):
        self.activity_database = Path(activity_database)
        self.game_database = Path(game_database)
        self.lock = lock or RLock()
        self.max_goods_num = int(max_goods_num or XiuConfig().max_goods_num)

    @staticmethod
    def _json(value) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_boss_reward_claim_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _operation_id(kind, payload) -> str:
        return f"activity-boss-{kind}:" + hashlib.sha256(payload.encode()).hexdigest()

    def get_result(self, operation_id, user_id=None) -> BossRewardClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self.lock, closing(db_backend.connect(self.activity_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            previous = conn.execute(
                "SELECT payload,result_json FROM activity_boss_reward_claim_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            payload = json.loads(str(previous[0]))
            if user_id is not None and str(payload[0]) != str(user_id):
                return BossRewardClaimResult("operation_conflict")
            data = json.loads(str(previous[1]))
            return BossRewardClaimResult(
                "duplicate",
                tuple(data["names"]),
                int(data["rank"]),
            )

    @staticmethod
    def _rewards(rows):
        merged = {}
        for reward_text in rows:
            for item in get_item_list(reward_text) if reward_text.strip() else []:
                if item["type"] == "stone":
                    key = ("stone", 0, "", "")
                else:
                    key = ("item", int(item["id"]), str(item["name"]), str(item["type"]))
                merged[key] = merged.get(key, 0) + int(item["quantity"])
        return tuple((*key, amount) for key, amount in sorted(merged.items()))

    def _grant(self, conn, user_id, rewards):
        if conn.execute("SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
            return "user_missing"
        for kind, item_id, _, _, amount in rewards:
            if kind != "item":
                continue
            row = conn.execute(
                "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                (user_id, item_id),
            ).fetchone()
            if (int(row[0]) if row else 0) + amount > self.max_goods_num:
                return "inventory_full"
        stone = sum(row[4] for row in rewards if row[0] == "stone")
        if stone:
            conn.execute("UPDATE game_data.user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, user_id))
        for kind, item_id, name, item_type, amount in rewards:
            if kind != "item":
                continue
            conn.execute(
                "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=game_data.back.goods_num+excluded.goods_num,"
                "bind_num=COALESCE(game_data.back.bind_num,0)+excluded.bind_num,update_time=CURRENT_TIMESTAMP",
                (user_id, item_id, name, item_type, amount, amount),
            )

    def _finish(self, conn, operation_id, payload, names, rank=0):
        result = self._json({"names": names, "rank": rank})
        conn.execute(
            "INSERT INTO activity_boss_reward_claim_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
            (operation_id, payload, result),
        )
        conn.commit()
        return BossRewardClaimResult("applied", tuple(names), rank)

    def claim_milestones(self, user_id, activity_key, milestones, operation_id=None):
        user_id, activity_key = str(user_id), str(activity_key)
        with self.lock, closing(db_backend.connect(self.activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self.game_database),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                if operation_id is not None:
                    operation_id = str(operation_id).strip()
                    if not operation_id:
                        raise ValueError("operation_id is required")
                    previous = conn.execute(
                        "SELECT payload,result_json FROM activity_boss_reward_claim_operations "
                        "WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchone()
                    if previous:
                        conn.rollback()
                        previous_payload = json.loads(str(previous[0]))
                        if previous_payload[:2] != [user_id, activity_key]:
                            return BossRewardClaimResult("operation_conflict")
                        data = json.loads(str(previous[1]))
                        return BossRewardClaimResult(
                            "duplicate", tuple(data["names"]), int(data["rank"])
                        )
                unlocked = {str(row[0]) for row in conn.execute("SELECT milestone_key FROM activity_boss_milestone WHERE activity_key=%s", (activity_key,)).fetchall()}
                if not unlocked:
                    conn.rollback()
                    return BossRewardClaimResult("not_unlocked")
                claimed = {str(row[0]) for row in conn.execute("SELECT milestone_key FROM activity_boss_milestone_claim WHERE activity_key=%s AND user_id=%s", (activity_key, user_id)).fetchall()}
                pending = [(str(row["key"]), str(row.get("name") or row["key"]), str(row.get("reward") or "")) for row in milestones if str(row["key"]) in unlocked and str(row["key"]) not in claimed]
                if not pending:
                    conn.rollback()
                    return BossRewardClaimResult("already_claimed")
                payload = self._json([user_id, activity_key, pending])
                operation_id = operation_id or self._operation_id("milestone", payload)
                previous = conn.execute("SELECT payload,result_json FROM activity_boss_reward_claim_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BossRewardClaimResult("operation_conflict")
                    data = json.loads(str(previous[1]))
                    return BossRewardClaimResult("duplicate", tuple(data["names"]), int(data["rank"]))
                error = self._grant(conn, user_id, self._rewards([row[2] for row in pending]))
                if error:
                    conn.rollback()
                    return BossRewardClaimResult(error)
                conn.executemany("INSERT INTO activity_boss_milestone_claim(activity_key,user_id,milestone_key,create_time) VALUES(%s,%s,%s,CURRENT_TIMESTAMP)", [(activity_key, user_id, row[0]) for row in pending])
                return self._finish(conn, operation_id, payload, [row[1] for row in pending])
            except Exception:
                conn.rollback()
                raise

    def claim_rank(self, user_id, activity_key, tiers, operation_id=None):
        user_id, activity_key = str(user_id), str(activity_key)
        with self.lock, closing(db_backend.connect(self.activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self.game_database),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                if operation_id is not None:
                    operation_id = str(operation_id).strip()
                    if not operation_id:
                        raise ValueError("operation_id is required")
                    previous = conn.execute(
                        "SELECT payload,result_json FROM activity_boss_reward_claim_operations "
                        "WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchone()
                    if previous:
                        conn.rollback()
                        previous_payload = json.loads(str(previous[0]))
                        if previous_payload[:2] != [user_id, activity_key]:
                            return BossRewardClaimResult("operation_conflict")
                        data = json.loads(str(previous[1]))
                        return BossRewardClaimResult(
                            "duplicate", tuple(data["names"]), int(data["rank"])
                        )
                ordered = [str(row[0]) for row in conn.execute("SELECT user_id FROM activity_boss_damage WHERE activity_key=%s ORDER BY total_damage DESC", (activity_key,)).fetchall()]
                if user_id not in ordered:
                    conn.rollback()
                    return BossRewardClaimResult("not_participant")
                rank = ordered.index(user_id) + 1
                tier = next((row for row in tiers if int(row["rank_min"]) <= rank <= int(row["rank_max"])), None)
                if tier is None:
                    conn.rollback()
                    return BossRewardClaimResult("not_eligible", rank=rank)
                tier_key = f"{tier['rank_min']}-{tier['rank_max']}"
                if conn.execute("SELECT 1 FROM activity_boss_rank_claim WHERE activity_key=%s AND user_id=%s AND tier_key=%s", (activity_key, user_id, tier_key)).fetchone():
                    conn.rollback()
                    return BossRewardClaimResult("already_claimed", rank=rank)
                payload = self._json([user_id, activity_key, rank, tier_key, tier.get("reward", "")])
                operation_id = operation_id or self._operation_id("rank", payload)
                previous = conn.execute("SELECT payload,result_json FROM activity_boss_reward_claim_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return BossRewardClaimResult("operation_conflict", rank=rank)
                    data = json.loads(str(previous[1]))
                    return BossRewardClaimResult("duplicate", tuple(data["names"]), int(data["rank"]))
                error = self._grant(conn, user_id, self._rewards([str(tier.get("reward") or "")]))
                if error:
                    conn.rollback()
                    return BossRewardClaimResult(error, rank=rank)
                conn.execute("INSERT INTO activity_boss_rank_claim(activity_key,user_id,tier_key,create_time) VALUES(%s,%s,%s,CURRENT_TIMESTAMP)", (activity_key, user_id, tier_key))
                return self._finish(conn, operation_id, payload, [str(tier.get("name") or "排行奖励")], rank)
            except Exception:
                conn.rollback()
                raise

class ActivityBossCoopSettlementService(ActivityBossSettlementService):
    def settle(
        self, operation_id, user_id, activity_key, expected_hp, expected_max_hp,
        expected_fight_count, daily_limit, fixed_damage, fight_date, timestamp,
        milestones=(),
    ) -> ActivityBossSettlementResult:
        return self._settle(
            operation_id=operation_id,
            user_id=user_id,
            activity_key=activity_key,
            expected_hp=expected_hp,
            expected_max_hp=expected_max_hp,
            expected_fight_count=expected_fight_count,
            daily_limit=daily_limit,
            fixed_damage=fixed_damage,
            fight_date=fight_date,
            source="coop",
            timestamp=timestamp,
            milestones=milestones,
        )

class ActivityBossItemRaidSettlementService(ActivityBossSettlementService):
    def settle(
        self, operation_id, user_id, activity_key, item_id, expected_inventory,
        item_cost, expected_hp, expected_max_hp, expected_fight_count, daily_limit,
        fixed_damage, fight_date, timestamp, milestones=(),
    ) -> ActivityBossSettlementResult:
        return self._settle(
            operation_id=operation_id,
            user_id=user_id,
            activity_key=activity_key,
            expected_hp=expected_hp,
            expected_max_hp=expected_max_hp,
            expected_fight_count=expected_fight_count,
            daily_limit=daily_limit,
            fixed_damage=fixed_damage,
            fight_date=fight_date,
            source="item",
            timestamp=timestamp,
            milestones=milestones,
            item_id=item_id,
            expected_inventory=expected_inventory,
            item_cost=item_cost,
        )

__all__ = [
    "ActivityBossSettlementResult",
    "ActivityBossSettlementService",
    "ActivitySignSettlementResult",
    "ActivitySignSettlementService",
    "ActivityTaskClaimResult",
    "ActivityTaskClaimService",
    "ActivityPassClaimResult",
    "ActivityPassClaimService",
    "ActivityPointShopPurchaseResult",
    "ActivityPointShopPurchaseService",
    "ActivityCollectExchangeResult",
    "ActivityCollectExchangeService",
    "ActivityClaimAllStepResult",
    "ActivityClaimAllResult",
    "ActivityClaimAllService",
    "BossRewardClaimResult",
    "BossRewardClaimService",
    "ActivityBossCoopSettlementService",
    "ActivityBossItemRaidSettlementService",
]
