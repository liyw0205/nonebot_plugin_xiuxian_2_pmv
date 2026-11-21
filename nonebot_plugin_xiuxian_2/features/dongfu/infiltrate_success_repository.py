from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from .plant_diagnostics import plant_diagnosis


class _InfiltrateSuccessStateChanged(Exception):
    def __init__(self, reason: str, subject: str):
        self.reason, self.subject = reason, subject


@dataclass(frozen=True)
class DongfuInfiltrateSuccessResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class DongfuInfiltrateSuccessSqlRepository:
    """Atomically persist one successful cave-dwelling infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _canonical(value: object) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _reject(suffix: str, subject: str) -> None:
        raise _InfiltrateSuccessStateChanged(
            f"dongfu_infiltrate_success_{suffix}", subject
        )

    @classmethod
    def _verify_player(cls, uow: DatabaseUnitOfWork, user_id: str, expected: dict) -> None:
        row = uow.query_one("SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,))
        if row is not None and row.get("stone") is not None:
            row["stone"] = require_exact_integer(row["stone"], field="stone", subject="actor")
        if row is None or row != expected:
            cls._reject("player_projection_mismatch", "actor")

    @classmethod
    def _verify_state(cls, uow: DatabaseUnitOfWork, user_id: str, expected: dict, subject: str) -> None:
        row = uow.query_one(
            "SELECT * FROM player_data.dongfu_status WHERE user_id=?", (user_id,)
        )
        if row is None or row != expected:
            cls._reject(f"{subject}_projection_mismatch", subject)

    @classmethod
    def _verify_inventory(cls, uow: DatabaseUnitOfWork, user_id: str, item_id: int, expected: dict) -> None:
        row = uow.query_one(
            "SELECT * FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)
        )
        if row is None or any(row.get(key) != value for key, value in expected.items()):
            cls._reject("inventory_projection_mismatch", f"item:{item_id}")

    @classmethod
    def _verify_receipt(cls, uow: DatabaseUnitOfWork, operation_id: str, expected: dict) -> None:
        row = uow.query_one(
            "SELECT payload,infiltrate_left,intrude_left "
            "FROM dongfu_infiltrate_success_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None or row != expected:
            cls._reject("receipt_projection_mismatch", "receipt")

    def settle(
        self,
        operation_id: str,
        visitor_id: str,
        target_id: str,
        day: str,
        mode_field: str,
        mode_limit: int,
        target_limit: int,
        expected_slots: str,
        slot_no: int,
        new_finish: str,
        rewards: Iterable[tuple[int, str, str, int]],
        stone: int,
        consume_guard: bool,
        max_goods_num: int,
    ) -> DongfuInfiltrateSuccessResult:
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(
            str, (visitor_id, target_id, day, mode_field)
        )
        mode_limit, target_limit, slot_no, stone, max_goods_num = map(
            int, (mode_limit, target_limit, slot_no, stone, max_goods_num)
        )
        consume_guard = int(bool(consume_guard))
        try:
            expected_slots_value = self._canonical(json.loads(expected_slots))
        except (TypeError, ValueError) as exc:
            raise ValueError("expected slots must be valid JSON") from exc
        reward_rows = tuple(
            (int(item_id), str(name), str(item_type), int(amount))
            for item_id, name, item_type, amount in rewards
        )
        new_finish = str(new_finish or "")
        if (
            not operation_id
            or visitor_id == target_id
            or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}
        ):
            raise ValueError("valid infiltration success operation is required")
        payload = self._canonical(
            (
                visitor_id,
                target_id,
                day,
                mode_field,
                mode_limit,
                target_limit,
                expected_slots_value,
                slot_no,
                new_finish,
                reward_rows,
                stone,
                consume_guard,
                max_goods_num,
            )
        )

        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.attach_database(self.player_database, "player_data")
                previous = uow.query_one(
                    "SELECT payload,infiltrate_left,intrude_left "
                    "FROM dongfu_infiltrate_success_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return DongfuInfiltrateSuccessResult(
                            "state_changed", reason="operation_conflict"
                        )
                    return DongfuInfiltrateSuccessResult(
                        "duplicate",
                        int(previous["infiltrate_left"]),
                        int(previous["intrude_left"]),
                    )

                user = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id=?", (visitor_id,)
                )
                if user is None:
                    return DongfuInfiltrateSuccessResult(
                        "state_changed", reason="user_missing", subject="visitor"
                    )
                visitor = uow.query_one(
                    "SELECT * FROM player_data.dongfu_status WHERE user_id=?",
                    (visitor_id,),
                )
                target = uow.query_one(
                    "SELECT * FROM player_data.dongfu_status WHERE user_id=?",
                    (target_id,),
                )
                if visitor is None or int(visitor["built"] or 0) != 1:
                    return DongfuInfiltrateSuccessResult(
                        "state_changed", reason="dongfu_missing", subject="visitor"
                    )
                if target is None or int(target["built"] or 0) != 1:
                    return DongfuInfiltrateSuccessResult(
                        "state_changed", reason="dongfu_missing", subject="target"
                    )
                try:
                    slots = json.loads(str(target["plant_slots"] or ""))
                except (TypeError, ValueError):
                    return DongfuInfiltrateSuccessResult(
                        "state_changed", reason="plant_record_invalid", subject="target"
                    )
                if (
                    self._canonical(slots) != expected_slots_value
                    or slot_no < 1
                    or slot_no > len(slots)
                ):
                    details = (
                        plant_diagnosis(slots, expected_slots_value)
                        if self._canonical(slots) != expected_slots_value
                        else {"reason": "plot_invalid", "subject": f"plot:{slot_no}"}
                    )
                    return DongfuInfiltrateSuccessResult("state_changed", **details)

                mode_count = (
                    int(visitor[mode_field] or 0)
                    if str(visitor["infiltrate_date"] or "") == day
                    else 0
                )
                intrude_count = (
                    int(target["intrude_count"] or 0)
                    if str(target["intrude_date"] or "") == day
                    else 0
                )
                if mode_count >= mode_limit or intrude_count >= target_limit:
                    return DongfuInfiltrateSuccessResult(
                        "daily_limit",
                        reason="infiltrate_limit" if mode_count >= mode_limit else "intrude_limit",
                        subject="visitor" if mode_count >= mode_limit else "target",
                    )

                totals: dict[int, int] = {}
                metadata: dict[int, tuple[str, str]] = {}
                items_before = {}
                for item_id, name, item_type, amount in reward_rows:
                    totals[item_id] = totals.get(item_id, 0) + amount
                    metadata[item_id] = (name, item_type)
                for item_id, amount in totals.items():
                    item = uow.query_one(
                        "SELECT * FROM back WHERE user_id=? AND goods_id=?",
                        (visitor_id, item_id),
                    )
                    current = int(item["goods_num"] or 0) if item is not None else 0
                    if current + amount > max_goods_num:
                        return DongfuInfiltrateSuccessResult(
                            "inventory_full", reason="inventory_full", subject=f"item:{item_id}"
                        )
                    items_before[item_id] = item

                expected_user = dict(user)
                if expected_user.get("stone") is not None:
                    expected_user["stone"] = require_exact_integer(
                        expected_user["stone"], field="stone", subject="actor"
                    )
                if stone:
                    expected_user["stone"] = require_exact_integer(
                        expected_user["stone"] if expected_user["stone"] is not None else 0,
                        field="stone",
                        subject="actor",
                    ) + stone
                    changed = uow.execute(
                        "UPDATE user_xiuxian SET stone=xiuxian_int_store("
                        "xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')) WHERE user_id=?",
                        (stone, visitor_id),
                    )
                    if changed.rowcount != 1:
                        self._reject("player_write_rejected", "actor")
                    self._verify_player(uow, visitor_id, expected_user)

                expected_visitor = dict(visitor)
                mode_count += 1
                expected_visitor.update(infiltrate_date=day, **{mode_field: mode_count})
                changed = uow.execute(
                    f"UPDATE player_data.dongfu_status SET infiltrate_date=?,{mode_field}=? WHERE user_id=?",
                    (day, mode_count, visitor_id),
                )
                if changed.rowcount != 1:
                    self._reject("visitor_write_rejected", "visitor")
                self._verify_state(uow, visitor_id, expected_visitor, "visitor")

                expected_target = dict(target)
                intrude_count += 1
                slots[slot_no - 1]["plant_finish"] = new_finish if new_finish else slots[slot_no - 1].get("plant_finish", "")
                expected_target.update(
                    intrude_date=day,
                    intrude_count=intrude_count,
                    patrol_guard=max(0, int(target["patrol_guard"] or 0) - consume_guard),
                    plant_slots=self._canonical(slots),
                )
                changed = uow.execute(
                    "UPDATE player_data.dongfu_status SET intrude_date=?,intrude_count=?,"
                    "patrol_guard=MAX(patrol_guard-?,0),plant_slots=? WHERE user_id=?",
                    (day, intrude_count, consume_guard, expected_target["plant_slots"], target_id),
                )
                if changed.rowcount != 1:
                    self._reject("target_write_rejected", "target")
                self._verify_state(uow, target_id, expected_target, "target")

                expected_items = {}
                for item_id, amount in totals.items():
                    name, item_type = metadata[item_id]
                    item = items_before[item_id]
                    expected_item = {
                        "user_id": visitor_id,
                        "goods_id": item_id,
                        "goods_name": name,
                        "goods_type": item_type,
                        "goods_num": (int(item["goods_num"] or 0) if item else 0) + amount,
                    }
                    if item is not None and "bind_num" in item:
                        expected_item["bind_num"] = int(item["bind_num"] or 0) + amount
                    elif item is None:
                        expected_item["bind_num"] = amount
                    changed = uow.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                        "goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (visitor_id, item_id, name, item_type, amount, amount),
                    )
                    if changed.rowcount != 1:
                        self._reject("inventory_write_rejected", f"item:{item_id}")
                    self._verify_inventory(uow, visitor_id, item_id, expected_item)
                    expected_items[item_id] = expected_item

                infiltrate_left = max(0, mode_limit - mode_count)
                intrude_left = max(0, target_limit - intrude_count)
                receipt = {
                    "payload": payload,
                    "infiltrate_left": infiltrate_left,
                    "intrude_left": intrude_left,
                }
                changed = uow.execute(
                    "INSERT INTO dongfu_infiltrate_success_operations("
                    "operation_id,payload,infiltrate_left,intrude_left) VALUES(?,?,?,?)",
                    (operation_id, payload, infiltrate_left, intrude_left),
                )
                if changed.rowcount != 1:
                    self._reject("receipt_write_rejected", "receipt")
                self._verify_receipt(uow, operation_id, receipt)
                self._verify_state(uow, visitor_id, expected_visitor, "visitor")
                self._verify_state(uow, target_id, expected_target, "target")
                if stone:
                    self._verify_player(uow, visitor_id, expected_user)
                for item_id, expected_item in expected_items.items():
                    self._verify_inventory(uow, visitor_id, item_id, expected_item)
                return DongfuInfiltrateSuccessResult(
                    "settled", infiltrate_left, intrude_left
                )
        except _InfiltrateSuccessStateChanged as exc:
            return DongfuInfiltrateSuccessResult(
                "state_changed", reason=exc.reason, subject=exc.subject
            )


__all__ = ["DongfuInfiltrateSuccessResult", "DongfuInfiltrateSuccessSqlRepository"]
