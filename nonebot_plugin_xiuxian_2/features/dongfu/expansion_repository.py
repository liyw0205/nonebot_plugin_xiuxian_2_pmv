from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready
from .plant_slots import canonical_plant_slots, empty_plant_slot, legacy_plant_fields, normalize_plant_slots


@dataclass(frozen=True)
class DongfuExpansionResult:
    status: str
    user_id: str = ""
    previous_count: int = 0
    current_count: int = 0
    deed_cost: int = 0
    stone_cost: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self):
        return self.status in {"expanded", "duplicate"}


class _ExpansionConflict(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason = reason
        self.subject = subject


class DongfuExpansionSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _as_int(value: Any, default: int = 0) -> int:
        try:
            return int(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _plant_slots(row, plot_count, base_plot_count, max_plot_count, seed_names):
        state = dict(row)
        state["plot_count"] = plot_count
        return normalize_plant_slots(
            state,
            base_plot_count=base_plot_count,
            max_plot_count=max_plot_count,
            fertilizer_max=3,
            seed_names=seed_names,
        )

    @staticmethod
    def _reject(suffix: str, subject: str) -> None:
        raise _ExpansionConflict(f"dongfu_expansion_{suffix}", subject)

    @classmethod
    def _verify_wallet(cls, uow, expected: Mapping[str, Any]) -> None:
        user_id = str(expected["user_id"])
        actual = uow.query_one("SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,))
        if actual is not None and actual.get("stone") is not None:
            actual["stone"] = require_exact_integer(actual["stone"], field="stone", subject="actor")
        if actual != dict(expected):
            cls._reject("stone_projection_mismatch", "stone_wallet")

    @classmethod
    def _verify_deed(cls, uow, user_id: str, deed_id: int, expected: Mapping[str, Any]) -> None:
        actual = uow.query_one(
            "SELECT * FROM back WHERE user_id=? AND goods_id=?", (user_id, deed_id)
        )
        if actual is not None:
            actual["goods_num"] = cls._as_int(actual.get("goods_num"))
            if "bind_num" in actual:
                actual["bind_num"] = cls._as_int(actual.get("bind_num"))
        if actual != dict(expected):
            cls._reject("deed_projection_mismatch", f"item:{deed_id}")

    @classmethod
    def _verify_cave(cls, uow, user_id: str, expected: Mapping[str, Any]) -> None:
        actual = uow.query_one(
            "SELECT built,plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish "
            "FROM player_data.dongfu_status WHERE user_id=?",
            (user_id,),
        )
        if actual is not None:
            actual["built"] = cls._as_int(actual.get("built"))
            actual["plot_count"] = cls._as_int(actual.get("plot_count"))
            actual["planting"] = cls._as_int(actual.get("planting"))
            actual["plant_seed_id"] = cls._as_int(actual.get("plant_seed_id"))
        if actual != dict(expected):
            cls._reject("state_projection_mismatch", "dongfu_status")

    @classmethod
    def _verify_receipt(cls, uow, operation_id: str, expected: Mapping[str, Any]) -> None:
        actual = uow.query_one(
            "SELECT user_id,previous_count,current_count,deed_cost,stone_cost "
            "FROM dongfu_expansion_operations WHERE operation_id=?",
            (operation_id,),
        )
        if actual != dict(expected):
            cls._reject("receipt_projection_mismatch", "receipt")

    @classmethod
    def _repair_projection(
        cls,
        uow: DatabaseUnitOfWork,
        user_id: str,
        row: Mapping[str, Any],
        base_plot_count: int,
        max_plot_count: int,
        seed_names: Mapping[int, str],
    ) -> None:
        raw_count = row.get("plot_count")
        count = min(max_plot_count, max(base_plot_count, cls._as_int(raw_count, base_plot_count)))
        slots = cls._plant_slots(row, count, base_plot_count, max_plot_count, seed_names)
        slot_json = canonical_plant_slots(slots)
        legacy = legacy_plant_fields(
            slots, valid_seed_ids=set(seed_names) if seed_names else None
        )
        if (
            str(row.get("plant_slots") or "") == slot_json
            and tuple(cls._as_int(row.get(key)) for key in ("planting", "plant_seed_id")) == legacy[:2]
            and str(row.get("plant_start") or "") == legacy[2]
            and str(row.get("plant_finish") or "") == legacy[3]
        ):
            return
        uow.execute(
            "UPDATE player_data.dongfu_status "
            "SET plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=? "
            "WHERE user_id=? AND built=1 AND plot_count IS ?",
            (slot_json, *legacy, user_id, raw_count),
        )

    def expand(
        self,
        operation_id,
        user_id,
        deed_id,
        base_plot_count,
        max_plot_count,
        stone_cost_per_level,
        seed_names=None,
        levels=1,
    ):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        deed_id, base_plot_count, max_plot_count, stone_cost_per_level, levels = map(
            int, (deed_id, base_plot_count, max_plot_count, stone_cost_per_level, levels)
        )
        seed_names = {int(key): str(value) for key, value in (seed_names or {}).items()}
        if levels < 1:
            return DongfuExpansionResult("invalid_count", user_id, reason="invalid_count")
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuExpansionResult("schema_missing", user_id)

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not operation_schema_ready(uow, "dongfu_expansion_operations"):
                return DongfuExpansionResult("schema_missing", user_id)

            old = uow.query_one(
                "SELECT user_id,previous_count,current_count,deed_cost,stone_cost "
                "FROM dongfu_expansion_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                try:
                    old_previous = int(old["previous_count"])
                    old_current = int(old["current_count"])
                    old_deed_cost = int(old["deed_cost"])
                    old_stone_cost = int(old["stone_cost"])
                except (TypeError, ValueError, OverflowError):
                    return DongfuExpansionResult(
                        "state_changed", user_id, reason="dongfu_expansion_receipt_invalid", subject="receipt"
                    )
                if str(old["user_id"]) != user_id or old_current - old_previous != levels:
                    return DongfuExpansionResult(
                        "state_changed", user_id, old_previous, old_current,
                        reason="operation_conflict", subject="receipt",
                    )
                row = uow.query_one(
                    "SELECT built,plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                    "FROM player_data.dongfu_status WHERE user_id=?",
                    (user_id,),
                )
                if row is not None and self._as_int(row.get("built")) == 1:
                    self._repair_projection(
                        uow, user_id, row, base_plot_count, max_plot_count, seed_names
                    )
                return DongfuExpansionResult(
                    "duplicate",
                    user_id,
                    old_previous,
                    old_current,
                    old_deed_cost,
                    old_stone_cost,
                )

            user = uow.query_one("SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,))
            row = uow.query_one(
                "SELECT built,plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                "FROM player_data.dongfu_status WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return DongfuExpansionResult("user_missing", user_id)
            if row is None or self._as_int(row.get("built")) != 1:
                return DongfuExpansionResult("dongfu_missing", user_id)

            raw_count = row.get("plot_count")
            previous = max(base_plot_count, self._as_int(raw_count, base_plot_count))
            if previous >= max_plot_count:
                return DongfuExpansionResult("max_plots", user_id, previous, previous)
            current = previous + levels
            if current > max_plot_count:
                return DongfuExpansionResult("max_plots", user_id, previous, previous)
            deed_cost = sum(level - base_plot_count for level in range(previous + 1, current + 1))
            stone_cost = stone_cost_per_level * deed_cost
            item = uow.query_one(
                "SELECT * FROM back WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (user_id, deed_id, deed_cost),
            )
            if item is None:
                return DongfuExpansionResult(
                    "deed_insufficient", user_id, previous, previous + levels, deed_cost, stone_cost
                )
            if self._as_int(user.get("stone")) < stone_cost:
                return DongfuExpansionResult(
                    "stone_insufficient", user_id, previous, previous + levels, deed_cost, stone_cost
                )

            slots = self._plant_slots(
                row, previous, base_plot_count, max_plot_count, seed_names
            )
            slots.extend(empty_plant_slot(slot) for slot in range(previous + 1, current + 1))
            legacy = legacy_plant_fields(
                slots, valid_seed_ids=set(seed_names) if seed_names else None
            )
            expected_user = dict(user)
            expected_user["stone"] = require_exact_integer(
                user["stone"], field="stone", subject="actor"
            ) - stone_cost
            expected_item = dict(item)
            expected_item["goods_num"] = self._as_int(item.get("goods_num")) - deed_cost
            bind_cost = 0
            if "bind_num" in item:
                bind_cost = min(max(self._as_int(item.get("bind_num")), 0), deed_cost)
                expected_item["bind_num"] = min(
                    max(self._as_int(item.get("bind_num")) - bind_cost, 0),
                    expected_item["goods_num"],
                )
            expected_cave = {
                "built": self._as_int(row.get("built")),
                "plot_count": current,
                "plant_slots": canonical_plant_slots(slots),
                "planting": legacy[0],
                "plant_seed_id": legacy[1],
                "plant_start": legacy[2],
                "plant_finish": legacy[3],
            }
            expected_receipt = {
                "user_id": user_id,
                "previous_count": previous,
                "current_count": current,
                "deed_cost": deed_cost,
                "stone_cost": stone_cost,
            }
            try:
                with uow.savepoint("dongfu_expansion"):
                    deed_sql = "UPDATE back SET goods_num=goods_num-?"
                    deed_params = [deed_cost]
                    if "bind_num" in item:
                        deed_sql += ",bind_num=MIN(MAX(COALESCE(bind_num,0)-?,0),goods_num-?)"
                        deed_params.extend((bind_cost, deed_cost))
                    deed_sql += " WHERE user_id=? AND goods_id=? AND goods_num>=?"
                    deed_params.extend((user_id, deed_id, deed_cost))
                    if uow.execute(deed_sql, tuple(deed_params)).rowcount != 1:
                        self._reject("deed_write_rejected", f"item:{deed_id}")
                    self._verify_deed(uow, user_id, deed_id, expected_item)

                    if uow.execute(
                        "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_sub(stone,?,'stone','actor')) WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')>=0",
                        (stone_cost, user_id, stone_cost),
                    ).rowcount != 1:
                        self._reject("stone_write_rejected", "stone_wallet")
                    self._verify_wallet(uow, expected_user)

                    if uow.execute(
                        "UPDATE player_data.dongfu_status "
                        "SET plot_count=?,plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=? "
                        "WHERE user_id=? AND built=1 AND plot_count IS ?",
                        (current, canonical_plant_slots(slots), *legacy, user_id, raw_count),
                    ).rowcount != 1:
                        self._reject("state_write_rejected", "dongfu_status")
                    self._verify_cave(uow, user_id, expected_cave)

                    if uow.execute(
                        "INSERT INTO dongfu_expansion_operations "
                        "(operation_id,user_id,previous_count,current_count,deed_cost,stone_cost) "
                        "VALUES(?,?,?,?,?,?)",
                        (operation_id, user_id, previous, current, deed_cost, stone_cost),
                    ).rowcount != 1:
                        self._reject("receipt_write_rejected", "receipt")
                    self._verify_receipt(uow, operation_id, expected_receipt)

                    self._verify_deed(uow, user_id, deed_id, expected_item)
                    self._verify_wallet(uow, expected_user)
                    self._verify_cave(uow, user_id, expected_cave)
            except _ExpansionConflict as conflict:
                return DongfuExpansionResult(
                    "state_changed", user_id, previous, previous, deed_cost, stone_cost,
                    conflict.reason, conflict.subject,
                )
            return DongfuExpansionResult(
                "expanded", user_id, previous, current, deed_cost, stone_cost
            )
