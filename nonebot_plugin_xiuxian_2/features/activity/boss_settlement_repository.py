from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ._operation_payload import operation_payload_matches


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


class ActivityBossSettlementSqlRepository:
    """Atomically settle cooperative and item boss actions in game_db."""

    operation_table = "activity_boss_settlement_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _assert_tables(cls, uow: DatabaseUnitOfWork, *, item: bool) -> None:
        required = {
            "activity_boss_state",
            "activity_boss_damage",
            "activity_boss_fight_log",
            "activity_boss_milestone",
            cls.operation_table,
        }
        if item:
            required.add("activity_item_inventory")
        existing = {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = sorted(required - existing)
        if missing:
            raise RuntimeError("activity_state.001 schema_missing: " + ", ".join(missing))

    @staticmethod
    def _fight_count(
        uow: DatabaseUnitOfWork, activity_key: str, user_id: str, fight_date: str
    ) -> int:
        row = uow.query_one(
            "SELECT COUNT(*) AS count FROM activity_boss_fight_log "
            "WHERE activity_key=? AND user_id=? AND fight_date=? "
            "AND source IN ('coop','world_boss','item')",
            (activity_key, user_id, fight_date),
        )
        return int((row or {}).get("count") or 0)

    @staticmethod
    def _unlock_milestones(
        uow: DatabaseUnitOfWork,
        activity_key: str,
        hp_left: int,
        max_hp: int,
        milestones: Any,
        timestamp: str,
    ) -> None:
        if max_hp <= 0:
            return
        percent_left = 100.0 * hp_left / max_hp
        for milestone in milestones or ():
            threshold = float(milestone.get("hp_percent", 0))
            if threshold <= 0 or percent_left > threshold:
                continue
            key = str(milestone.get("key") or f"p{threshold}")
            uow.execute(
                "INSERT OR IGNORE INTO activity_boss_milestone "
                "(activity_key,milestone_key,unlocked_time) VALUES(?,?,?)",
                (activity_key, key, timestamp),
            )

    def _settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        activity_key: str,
        expected_hp: int,
        expected_max_hp: int,
        expected_fight_count: int,
        daily_limit: int,
        fixed_damage: int,
        fight_date: str,
        timestamp: str,
        milestones: Any = (),
        item_id: str | None = None,
        expected_inventory: int | None = None,
        item_cost: int = 0,
    ) -> ActivityBossSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        activity_key = str(activity_key).strip()
        fight_date = str(fight_date)
        timestamp = str(timestamp)
        expected_hp, expected_max_hp, expected_fight_count = map(
            int, (expected_hp, expected_max_hp, expected_fight_count)
        )
        daily_limit, fixed_damage, item_cost = map(
            int, (daily_limit, fixed_damage, item_cost)
        )
        item_id = None if item_id is None else str(item_id)
        expected_inventory = (
            None if expected_inventory is None else int(expected_inventory)
        )
        milestone_rows = tuple(
            (str(row.get("key") or ""), float(row.get("hp_percent", 0)))
            for row in milestones or ()
        )
        is_item = item_id is not None
        if (
            not operation_id
            or not user_id
            or not activity_key
            or not fight_date
            or not timestamp
            or expected_hp < 0
            or expected_max_hp <= 0
            or expected_fight_count < 0
            or daily_limit <= 0
            or fixed_damage <= 0
            or item_cost < 0
            or (is_item and (not item_id or expected_inventory is None or item_cost <= 0))
        ):
            raise ValueError("valid activity boss settlement inputs are required")

        payload = self._json(
            [
                user_id,
                activity_key,
                expected_max_hp,
                daily_limit,
                fixed_damage,
                fight_date,
                "item" if is_item else "coop",
                milestone_rows,
                item_id,
                item_cost,
            ]
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._assert_tables(uow, item=is_item)
            previous = uow.query_one(
                f"SELECT payload,damage,hp_left,max_hp,fight_count,inventory "
                f"FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                uow.connection.rollback()
                if not operation_payload_matches(previous["payload"], payload):
                    return ActivityBossSettlementResult("operation_conflict")
                return ActivityBossSettlementResult(
                    "duplicate",
                    int(previous["damage"]),
                    int(previous["hp_left"]),
                    int(previous["max_hp"]),
                    int(previous["fight_count"]),
                    None if previous["inventory"] is None else int(previous["inventory"]),
                )

            state = uow.query_one(
                "SELECT hp_left,max_hp FROM activity_boss_state WHERE activity_key=?",
                (activity_key,),
            )
            if state is None:
                if expected_hp != expected_max_hp:
                    uow.connection.rollback()
                    return ActivityBossSettlementResult("state_changed")
                uow.execute(
                    "INSERT INTO activity_boss_state(activity_key,hp_left,max_hp,update_time) "
                    "VALUES(?,?,?,?)",
                    (activity_key, expected_hp, expected_max_hp, timestamp),
                )
            else:
                state_hp = max(0, int(state["hp_left"] or 0))
                state_max = max(1, int(state["max_hp"] or expected_max_hp))
                if state_max != expected_max_hp:
                    normalized_hp = int(expected_max_hp * state_hp / state_max)
                    if normalized_hp != expected_hp:
                        uow.connection.rollback()
                        return ActivityBossSettlementResult("state_changed")
                    changed = uow.execute(
                        "UPDATE activity_boss_state SET hp_left=?,max_hp=?,update_time=? "
                        "WHERE activity_key=? AND hp_left=? AND max_hp=?",
                        (
                            normalized_hp,
                            expected_max_hp,
                            timestamp,
                            activity_key,
                            state_hp,
                            state_max,
                        ),
                    )
                    if changed.rowcount != 1:
                        uow.connection.rollback()
                        return ActivityBossSettlementResult("state_changed")
                    state_hp, state_max = normalized_hp, expected_max_hp
                if (state_hp, state_max) != (expected_hp, expected_max_hp):
                    uow.connection.rollback()
                    return ActivityBossSettlementResult("state_changed")

            if expected_hp <= 0:
                uow.connection.rollback()
                return ActivityBossSettlementResult(
                    "boss_defeated", 0, expected_hp, expected_max_hp, expected_fight_count
                )

            fight_count = self._fight_count(uow, activity_key, user_id, fight_date)
            if fight_count != expected_fight_count:
                uow.connection.rollback()
                return ActivityBossSettlementResult("state_changed")
            if fight_count >= daily_limit:
                uow.connection.rollback()
                return ActivityBossSettlementResult(
                    "limit_reached", 0, expected_hp, expected_max_hp, fight_count
                )

            inventory_left = None
            if is_item:
                inventory_row = uow.query_one(
                    "SELECT count FROM activity_item_inventory "
                    "WHERE activity_key=? AND user_id=? AND item_id=?",
                    (activity_key, user_id, item_id),
                )
                current_inventory = int((inventory_row or {}).get("count") or 0)
                if current_inventory != expected_inventory:
                    uow.connection.rollback()
                    return ActivityBossSettlementResult("state_changed")
                if current_inventory < item_cost:
                    uow.connection.rollback()
                    return ActivityBossSettlementResult(
                        "item_insufficient", inventory=current_inventory
                    )
                inventory_left = current_inventory - item_cost
                changed = uow.execute(
                    "UPDATE activity_item_inventory SET count=count-?,update_time=? "
                    "WHERE activity_key=? AND user_id=? AND item_id=? AND count=?",
                    (
                        item_cost,
                        timestamp,
                        activity_key,
                        user_id,
                        item_id,
                        current_inventory,
                    ),
                )
                if changed.rowcount != 1:
                    uow.connection.rollback()
                    return ActivityBossSettlementResult("state_changed")

            damage = min(fixed_damage, expected_hp)
            hp_left = expected_hp - damage
            changed = uow.execute(
                "UPDATE activity_boss_state SET hp_left=?,update_time=? "
                "WHERE activity_key=? AND hp_left=? AND max_hp=?",
                (hp_left, timestamp, activity_key, expected_hp, expected_max_hp),
            )
            if changed.rowcount != 1:
                uow.connection.rollback()
                return ActivityBossSettlementResult("state_changed")
            uow.execute(
                "INSERT INTO activity_boss_damage "
                "(activity_key,user_id,total_damage,update_time) VALUES(?,?,?,?) "
                "ON CONFLICT(activity_key,user_id) DO UPDATE SET "
                "total_damage=activity_boss_damage.total_damage+excluded.total_damage,"
                "update_time=excluded.update_time",
                (activity_key, user_id, damage, timestamp),
            )
            uow.execute(
                "INSERT INTO activity_boss_fight_log "
                "(activity_key,user_id,damage,fight_date,source,create_time) "
                "VALUES(?,?,?,?,?,?)",
                (activity_key, user_id, damage, fight_date, "item" if is_item else "coop", timestamp),
            )
            self._unlock_milestones(
                uow, activity_key, hp_left, expected_max_hp, milestones, timestamp
            )
            fight_count += 1
            uow.execute(
                f"INSERT INTO {self.operation_table} "
                "(operation_id,payload,damage,hp_left,max_hp,fight_count,inventory) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    operation_id,
                    payload,
                    damage,
                    hp_left,
                    expected_max_hp,
                    fight_count,
                    inventory_left,
                ),
            )
            return ActivityBossSettlementResult(
                "applied", damage, hp_left, expected_max_hp, fight_count, inventory_left
            )

    def settle_cooperative(self, *args: Any, **kwargs: Any) -> ActivityBossSettlementResult:
        kwargs.pop("item_id", None)
        kwargs.pop("expected_inventory", None)
        kwargs["item_cost"] = 0
        return self._settle(*args, **kwargs)

    def settle_item(self, *args: Any, **kwargs: Any) -> ActivityBossSettlementResult:
        return self._settle(*args, **kwargs)


__all__ = ["ActivityBossSettlementResult", "ActivityBossSettlementSqlRepository"]
