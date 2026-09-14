from __future__ import annotations

import json
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import decide_breakthrough, decide_stone_training


class TiantiProfileReader:
    """Read the immutable tianti level profile without loading legacy modules."""

    def __init__(self, data_directory: str | Path, *, closing_multiplier: float = 1.5) -> None:
        self.path = Path(data_directory) / "炼体" / "炼体境界.json"
        self.closing_multiplier = float(closing_multiplier)
        self._levels: dict[str, dict[str, Any]] | None = None

    def levels(self) -> dict[str, dict[str, Any]]:
        if self._levels is None:
            with self.path.open("r", encoding="utf-8") as stream:
                loaded = json.load(stream)
            if not isinstance(loaded, dict) or not loaded:
                raise ValueError("炼体境界配置无效")
            self._levels = {str(name): dict(value) for name, value in loaded.items() if isinstance(value, dict)}
        return self._levels

    def default_data(self) -> dict[str, Any]:
        first = min(self.levels(), key=lambda name: int(self.levels()[name].get("rank", 0)))
        return {
            "tianti_level": first, "tianti_hp": 0, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }

    def clean(self, row: Mapping[str, Any] | None) -> dict[str, Any]:
        data = self.default_data()
        for key in data:
            if row and row.get(key) is not None:
                data[key] = row[key]
        data["tianti_level"] = data["tianti_level"] if data["tianti_level"] in self.levels() else self.default_data()["tianti_level"]
        data["tianti_hp"] = max(0, int(data.get("tianti_hp", 0) or 0))
        return data

    def cap(self, data: Mapping[str, Any]) -> int:
        current = self.levels()[str(data["tianti_level"])]
        next_level = next((item for item in self.levels().values() if int(item.get("rank", 0)) == int(current.get("rank", 0)) + 1), None)
        if next_level is None:
            return 10**30
        return int(int(next_level.get("need_hp", 0)) * self.closing_multiplier)

    def next_level(self, level: str) -> tuple[str | None, dict[str, Any]]:
        levels = sorted(self.levels().items(), key=lambda item: int(item[1].get("rank", 0)))
        for index, (name, _) in enumerate(levels):
            if name == level:
                return levels[index + 1] if index + 1 < len(levels) else (None, {})
        return None, {}

    def cultivation_rank(self, level: str) -> int:
        with (self.path.parent.parent / "境界.json").open("r", encoding="utf-8") as stream:
            ranks = list(json.load(stream).keys())
        if level not in ranks:
            raise ValueError(f"未知修仙境界: {level}")
        return len(ranks) - ranks.index(level) - 1

@dataclass(frozen=True)
class StoneTrainingPersistenceResult:
    status: str
    user_id: str
    requested_stone: int
    stone_cost: int
    hp_gain: int
    new_hp: int


@dataclass(frozen=True)
class BreakthroughPersistenceResult:
    status: str
    user_id: str
    old_level: str
    new_level: str
    hp_cost: int
    new_hp: int
    success: bool


class TiantiTrainingRepository(Protocol):
    def train(self, operation_id: str, user_id: str, requested_stone: int) -> Any: ...

    def apply_bath(
        self,
        operation_id: str,
        user_id: str,
        consume_plan: Sequence[Mapping[str, Any]],
        effect: float,
        slot_name: str,
        started_at: datetime,
        duration_minutes: int,
        *,
        sect_fairyland_level: int = 0,
    ) -> Any: ...

    def breakthrough(self, operation_id: str, user_id: str, *, cultivation_rank: int, roll_success: bool) -> Any: ...

    def open_qiaoxue(self, operation_id: str, user_id: str, roll: int) -> Any: ...


class StoneTrainingSqlRepository:
    """Feature-owned stone training persistence on the catalogued databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path, *, data_manager: Any = None, cap_provider: Callable[[dict[str, Any]], int] | None = None, profile_reader: TiantiProfileReader | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self._manager = data_manager
        self._cap_provider = cap_provider
        self._profile_reader = profile_reader or TiantiProfileReader(Path(player_database).parent / "xiuxian")

    def train(self, operation_id: str, user_id: str, requested_stone: int) -> Any:
        if self._manager is None:
            self._manager = self._profile_reader

        operation_id, user_id = str(operation_id).strip(), str(user_id)
        requested_stone = int(requested_stone)
        if not operation_id or requested_stone <= 0:
            raise ValueError("operation_id and requested_stone must be positive")
        if self._cap_provider is None:
            self._cap_provider = self._profile_reader.cap

        fields = tuple(self._profile_reader.default_data().keys())
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            operation_table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='tianti_stone_training_operations'")
            player_table = uow.query_one("SELECT 1 AS present FROM player_data.sqlite_master WHERE type='table' AND name='tianti_info'")
            player_columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(tianti_info)")}
            if operation_table is None or player_table is None or not set(fields).issubset(player_columns):
                raise RuntimeError("tianti training schema is not ready; run migrations first")
            previous = uow.query_one("SELECT user_id, requested_stone, stone_cost, hp_gain, new_hp FROM tianti_stone_training_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["user_id"]) != user_id or int(previous["requested_stone"]) != requested_stone:
                    return StoneTrainingPersistenceResult("state_changed", user_id, requested_stone, 0, 0, 0)
                return StoneTrainingPersistenceResult("duplicate", user_id, requested_stone, int(previous["stone_cost"]), int(previous["hp_gain"]), int(previous["new_hp"]))
            user = uow.query_one("SELECT COALESCE(stone, 0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return StoneTrainingPersistenceResult("user_missing", user_id, requested_stone, 0, 0, 0)
            if int(user["stone"]) < requested_stone:
                return StoneTrainingPersistenceResult("stone_insufficient", user_id, requested_stone, 0, 0, 0)
            row = uow.query_one("SELECT * FROM player_data.tianti_info WHERE user_id=?", (user_id,))
            data = self._profile_reader.clean(dict(row) if row else {})
            cap = self._cap_provider(data)
            decision = decide_stone_training(old_hp=int(data["tianti_hp"]), requested_stone=requested_stone, hp_cap=cap)
            if decision.status == "at_cap":
                return StoneTrainingPersistenceResult("at_cap", user_id, requested_stone, 0, 0, int(data["tianti_hp"]))
            charged = uow.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-? WHERE user_id=? AND CAST(COALESCE(stone,0) AS REAL)>=?", (decision.stone_cost, user_id, decision.stone_cost))
            if charged.rowcount != 1:
                return StoneTrainingPersistenceResult("stone_changed", user_id, requested_stone, 0, 0, int(data["tianti_hp"]))
            data["tianti_hp"] = decision.new_hp
            values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field] for field in fields]
            columns = ", ".join(["user_id", *fields])
            placeholders = ", ".join("?" for _ in values)
            updates = ", ".join(f'"{field}"=excluded."{field}"' for field in fields)
            uow.execute(f'INSERT INTO player_data.tianti_info ({columns}) VALUES ({", ".join("?" for _ in range(len(values) + 1))}) ON CONFLICT(user_id) DO UPDATE SET {updates}', (user_id, *values))
            uow.execute("INSERT INTO tianti_stone_training_operations(operation_id,user_id,requested_stone,stone_cost,hp_gain,new_hp) VALUES(?,?,?,?,?,?)", (operation_id, user_id, requested_stone, decision.stone_cost, decision.hp_gain, decision.new_hp))
            return StoneTrainingPersistenceResult("trained", user_id, requested_stone, decision.stone_cost, decision.hp_gain, decision.new_hp)


class TiantiBreakthroughSqlRepository:
    """Feature-owned breakthrough persistence on player.db."""

    def __init__(self, player_database: str | Path, *, profile_reader: TiantiProfileReader | None = None) -> None:
        self.player_database = str(player_database)
        self.profile = profile_reader or TiantiProfileReader(Path(player_database).parent / "xiuxian")

    def breakthrough(self, operation_id: str, user_id: str, *, cultivation_rank: int, roll_success: bool) -> Any:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id is required")
        fields = tuple(self.profile.default_data().keys())
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            operation_table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='tianti_breakthrough_operations'")
            player_table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='tianti_info'")
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(tianti_info)")}
            if operation_table is None or player_table is None or not set(fields).issubset(columns):
                raise RuntimeError("tianti training schema is not ready; run migrations first")
            previous = uow.query_one("SELECT user_id, cultivation_rank, roll_success, old_level, new_level, hp_cost, new_hp, success FROM tianti_breakthrough_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["user_id"]) != user_id or int(previous["cultivation_rank"]) != int(cultivation_rank):
                    return BreakthroughPersistenceResult("state_changed", user_id, "", "", 0, 0, False)
                return BreakthroughPersistenceResult("duplicate", user_id, str(previous["old_level"]), str(previous["new_level"]), int(previous["hp_cost"]), int(previous["new_hp"]), bool(previous["success"]))
            row = uow.query_one("SELECT * FROM tianti_info WHERE user_id=?", (user_id,))
            if row is None:
                return BreakthroughPersistenceResult("user_missing", user_id, "", "", 0, 0, False)
            data = self.profile.clean(row)
            old_level = str(data["tianti_level"])
            next_level, next_config = self.profile.next_level(old_level)
            required_rank = self.profile.cultivation_rank(str(next_config["min_xx_level"])) if next_config else 0
            decision = decide_breakthrough(old_level=old_level, next_level=next_level, cultivation_rank=int(cultivation_rank), required_rank=required_rank, old_hp=int(data["tianti_hp"]), required_hp=int(next_config.get("need_hp", 0)), roll_success=bool(roll_success))
            if decision.status != "completed":
                return BreakthroughPersistenceResult(decision.status, user_id, old_level, old_level, 0, decision.new_hp, False)
            data["tianti_level"], data["tianti_hp"] = decision.new_level, decision.new_hp
            values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field] for field in fields]
            columns_sql = ", ".join(["user_id", *fields])
            placeholders = ", ".join("?" for _ in range(len(values) + 1))
            updates = ", ".join(f'"{field}"=excluded."{field}"' for field in fields)
            uow.execute(f'INSERT INTO tianti_info ({columns_sql}) VALUES ({placeholders}) ON CONFLICT(user_id) DO UPDATE SET {updates}', (user_id, *values))
            uow.execute("INSERT INTO tianti_breakthrough_operations(operation_id,user_id,cultivation_rank,roll_success,old_level,new_level,hp_cost,new_hp,success) VALUES(?,?,?,?,?,?,?,?,?)", (operation_id, user_id, int(cultivation_rank), int(bool(roll_success)), decision.old_level, decision.new_level, decision.hp_cost, decision.new_hp, int(decision.new_level != decision.old_level)))
            return BreakthroughPersistenceResult("completed", user_id, decision.old_level, decision.new_level, decision.hp_cost, decision.new_hp, decision.new_level != decision.old_level)


class LegacyTiantiTrainingRepository:
    """Lazy adapter for the already transactional Tianti services."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_tianti.transaction_service import (
            MedicineBathService,
            QiaoxueService,
            StoneTrainingService,
            TiantiBreakthroughService,
        )

        return (
            StoneTrainingService(self.game_database, self.player_database),
            MedicineBathService(self.game_database, self.player_database),
            TiantiBreakthroughService(self.player_database),
            QiaoxueService(self.player_database),
        )

    def train(self, operation_id: str, user_id: str, requested_stone: int) -> Any:
        return self._services()[0].train(operation_id, user_id, requested_stone)

    def apply_bath(
        self,
        operation_id: str,
        user_id: str,
        consume_plan: Sequence[Mapping[str, Any]],
        effect: float,
        slot_name: str,
        started_at: datetime,
        duration_minutes: int,
        *,
        sect_fairyland_level: int = 0,
    ) -> Any:
        return self._services()[1].apply(
            operation_id,
            user_id,
            consume_plan,
            effect,
            slot_name,
            started_at,
            duration_minutes,
            sect_fairyland_level=sect_fairyland_level,
        )

    def breakthrough(self, operation_id: str, user_id: str, *, cultivation_rank: int, roll_success: bool) -> Any:
        return self._services()[2].attempt(
            operation_id,
            user_id,
            cultivation_rank=cultivation_rank,
            roll_success=roll_success,
        )

    def open_qiaoxue(self, operation_id: str, user_id: str, roll: int) -> Any:
        return self._services()[3].open(operation_id, user_id, roll)


__all__ = [
    "BreakthroughPersistenceResult",
    "LegacyTiantiTrainingRepository",
    "StoneTrainingPersistenceResult",
    "StoneTrainingSqlRepository",
    "TiantiBreakthroughSqlRepository",
    "TiantiProfileReader",
    "TiantiTrainingRepository",
]
