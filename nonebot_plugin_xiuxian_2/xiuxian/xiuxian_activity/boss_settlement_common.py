from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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
