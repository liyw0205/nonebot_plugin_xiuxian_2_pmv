from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready


@dataclass(frozen=True)
class DongfuExpansionResult:
    status: str
    user_id: str = ""
    previous_count: int = 0
    current_count: int = 0
    deed_cost: int = 0
    stone_cost: int = 0

    @property
    def succeeded(self):
        return self.status in {"expanded", "duplicate"}


class _ExpansionConflict(Exception):
    def __init__(self, status: str) -> None:
        self.status = status


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
    def _canonical(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _plant_slots(
        cls,
        row: Mapping[str, Any],
        plot_count: int,
        seed_names: Mapping[int, str],
    ) -> list[dict[str, Any]]:
        raw_slots = row.get("plant_slots")
        if isinstance(raw_slots, str):
            try:
                raw_slots = json.loads(raw_slots)
            except (TypeError, ValueError):
                raw_slots = []
        if not isinstance(raw_slots, list):
            raw_slots = []

        slots = []
        for index in range(plot_count):
            raw = raw_slots[index] if index < len(raw_slots) and isinstance(raw_slots[index], dict) else {}
            seed_id = cls._as_int(raw.get("seed_id"))
            slots.append(
                {
                    "slot": index + 1,
                    "seed_id": seed_id,
                    "seed_name": str(raw.get("seed_name") or seed_names.get(seed_id, "")),
                    "plant_start": str(raw.get("plant_start") or ""),
                    "plant_finish": str(raw.get("plant_finish") or ""),
                    "fertilizer": min(3, max(0, cls._as_int(raw.get("fertilizer")))),
                }
            )

        legacy_seed_id = cls._as_int(row.get("plant_seed_id"))
        if (
            not any(slot["seed_id"] for slot in slots)
            and cls._as_int(row.get("planting")) == 1
            and legacy_seed_id
        ):
            slots[0].update(
                {
                    "seed_id": legacy_seed_id,
                    "seed_name": str(slots[0]["seed_name"] or seed_names.get(legacy_seed_id, "")),
                    "plant_start": str(row.get("plant_start") or ""),
                    "plant_finish": str(row.get("plant_finish") or ""),
                }
            )
        return slots

    @staticmethod
    def _legacy_fields(slots: list[dict[str, Any]]) -> tuple[int, int, str, str]:
        active = next((slot for slot in slots if slot["seed_id"] > 0), None)
        if active is None:
            return 0, 0, "", ""
        return 1, active["seed_id"], active["plant_start"], active["plant_finish"]

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
        slots = cls._plant_slots(row, count, seed_names)
        slot_json = cls._canonical(slots)
        legacy = cls._legacy_fields(slots)
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
    ):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        deed_id, base_plot_count, max_plot_count, stone_cost_per_level = map(
            int, (deed_id, base_plot_count, max_plot_count, stone_cost_per_level)
        )
        seed_names = {int(key): str(value) for key, value in (seed_names or {}).items()}
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuExpansionResult("schema_missing", user_id)

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not operation_schema_ready(uow, "dongfu_expansion_operations"):
                return DongfuExpansionResult("schema_missing", user_id)

            old = uow.query_one(
                "SELECT previous_count,current_count,deed_cost,stone_cost "
                "FROM dongfu_expansion_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
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
                    int(old["previous_count"]),
                    int(old["current_count"]),
                    int(old["deed_cost"]),
                    int(old["stone_cost"]),
                )

            user = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
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
            current = previous + 1
            deed_cost = current - base_plot_count
            stone_cost = stone_cost_per_level * deed_cost
            item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (user_id, deed_id, deed_cost),
            )
            if item is None:
                return DongfuExpansionResult(
                    "deed_insufficient", user_id, previous, previous, deed_cost, stone_cost
                )
            if self._as_int(user.get("stone")) < stone_cost:
                return DongfuExpansionResult(
                    "stone_insufficient", user_id, previous, previous, deed_cost, stone_cost
                )

            slots = self._plant_slots(row, previous, seed_names)
            slots.append(
                {
                    "slot": current,
                    "seed_id": 0,
                    "seed_name": "",
                    "plant_start": "",
                    "plant_finish": "",
                    "fertilizer": 0,
                }
            )
            legacy = self._legacy_fields(slots)
            try:
                with uow.savepoint("dongfu_expansion"):
                    if uow.execute(
                        "UPDATE back SET goods_num=goods_num-? "
                        "WHERE user_id=? AND goods_id=? AND goods_num>=?",
                        (deed_cost, user_id, deed_id, deed_cost),
                    ).rowcount != 1:
                        raise _ExpansionConflict("deed_changed")
                    if uow.execute(
                        "UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?",
                        (stone_cost, user_id, stone_cost),
                    ).rowcount != 1:
                        raise _ExpansionConflict("stone_changed")
                    if uow.execute(
                        "UPDATE player_data.dongfu_status "
                        "SET plot_count=?,plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=? "
                        "WHERE user_id=? AND built=1 AND plot_count IS ?",
                        (current, self._canonical(slots), *legacy, user_id, raw_count),
                    ).rowcount != 1:
                        raise _ExpansionConflict("dongfu_changed")
                    uow.execute(
                        "INSERT INTO dongfu_expansion_operations "
                        "(operation_id,user_id,previous_count,current_count,deed_cost,stone_cost) "
                        "VALUES(?,?,?,?,?,?)",
                        (operation_id, user_id, previous, current, deed_cost, stone_cost),
                    )
            except _ExpansionConflict as conflict:
                return DongfuExpansionResult(
                    conflict.status, user_id, previous, previous, deed_cost, stone_cost
                )
            return DongfuExpansionResult(
                "expanded", user_id, previous, current, deed_cost, stone_cost
            )
