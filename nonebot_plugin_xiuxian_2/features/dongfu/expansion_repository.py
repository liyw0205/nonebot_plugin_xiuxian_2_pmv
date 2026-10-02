from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

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

            slots = self._plant_slots(
                row, previous, base_plot_count, max_plot_count, seed_names
            )
            slots.append(empty_plant_slot(current))
            legacy = legacy_plant_fields(
                slots, valid_seed_ids=set(seed_names) if seed_names else None
            )
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
                        (current, canonical_plant_slots(slots), *legacy, user_id, raw_count),
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
