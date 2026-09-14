from __future__ import annotations

import json
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from ...infrastructure.database import DatabaseUnitOfWork
from .domain import (
    decide_breakthrough,
    decide_medicine_bath_activation,
    decide_stone_training,
    decide_tianti_gain,
    decide_tianti_settlement_window,
)


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

    def qiaoxue_pool(self) -> list[dict[str, Any]]:
        with (self.path.parent / "炼体窍穴.json").open("r", encoding="utf-8") as stream:
            loaded = json.load(stream)
        pool = loaded.get("窍穴", []) if isinstance(loaded, dict) else []
        return [dict(item) for item in pool if isinstance(item, dict) and item.get("name")]

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


@dataclass(frozen=True)
class QiaoxuePersistenceResult:
    status: str
    user_id: str
    qiaoxue: dict[str, Any]
    hp_cost: int
    new_hp: int
    opened_count: int
    unlock_limit: int


@dataclass(frozen=True)
class MedicineBathPersistenceResult:
    status: str
    user_id: str
    consumed: tuple[dict[str, Any], ...] = ()
    effect: float = 0.0
    bath_name: str = ""
    end_time: str = ""
    settlement: dict[str, Any] | None = None
    insufficient: tuple[dict[str, Any], ...] = ()


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def _sect_bonus(level: int) -> float:
    return max(0, min(int(level or 0), 10)) * 0.05


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


class TiantiMedicineBathSqlRepository:
    """Feature-owned cross-database medicine bath transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, *, spirit_vein_multiplier: Callable[[], float] | None = None, profile_reader: TiantiProfileReader | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.profile = profile_reader or TiantiProfileReader(Path(player_database).parent / "xiuxian")
        self.spirit_vein_multiplier = spirit_vein_multiplier or (lambda: 1.0)

    def apply_bath(self, operation_id: str, user_id: str, consume_plan: Sequence[Mapping[str, Any]], effect: float, slot_name: str, started_at: datetime, duration_minutes: int, *, sect_fairyland_level: int = 0) -> Any:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        plan = tuple({"item_id": int(item["item_id"]), "name": str(item["name"]), "amount": int(item["amount"])} for item in consume_plan)
        if not operation_id or not plan or any(item["amount"] <= 0 for item in plan):
            raise ValueError("operation_id and positive consume plan are required")
        fields = tuple(self.profile.default_data().keys())
        now_text = started_at.strftime("%Y-%m-%d %H:%M:%S")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            tables = {str(row["name"]) for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
            player_columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(tianti_info)")}
            if "tianti_medicine_bath_operations" not in tables or "tianti_info" not in {str(row["name"]) for row in uow.query_all("SELECT name FROM player_data.sqlite_master WHERE type='table'")} or not set(fields).issubset(player_columns):
                raise RuntimeError("tianti training schema is not ready; run migrations first")
            previous = uow.query_one("SELECT user_id, result_json FROM tianti_medicine_bath_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["user_id"]) != user_id:
                    return MedicineBathPersistenceResult("state_changed", user_id)
                return MedicineBathPersistenceResult("duplicate", user_id, **json.loads(previous["result_json"]))
            row = uow.query_one("SELECT * FROM player_data.tianti_info WHERE user_id=?", (user_id,))
            if row is None:
                return MedicineBathPersistenceResult("user_missing", user_id)
            data = self.profile.clean(row)
            current_end = _parse_time(data.get("medicine_end_time"))
            if current_end is not None and started_at <= current_end and float(data.get("medicine_effect", 0) or 0) > 1:
                return MedicineBathPersistenceResult("bath_active", user_id)
            insufficient = []
            for item in plan:
                stock = uow.query_one("SELECT COALESCE(goods_num, 0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item["item_id"]))
                have = int(stock["goods_num"]) if stock else 0
                if have < item["amount"]:
                    insufficient.append({**item, "have": have})
            if insufficient:
                return MedicineBathPersistenceResult("item_insufficient", user_id, insufficient=tuple(insufficient))
            old_end = _parse_time(data.get("medicine_end_time"))
            window = decide_tianti_settlement_window(last_settlement=_parse_time(data.get("last_settle_time")), now=started_at)
            settlement: dict[str, Any] = {"status": window.status}
            if window.status == "settle":
                level = self.profile.levels()[str(data["tianti_level"])]
                details = list(data.get("opened_qiaoxue_detail", []) or [])
                base_ratio = sum(float(item.get("effect_value", 0)) for item in details if item.get("effect_type") == "base_per_min_ratio")
                gain_pct = sum(float(item.get("effect_value", 0)) for item in details if item.get("effect_type") == "hp_gain_pct")
                old_bath = float(data.get("medicine_effect", 0) or 0) if old_end and started_at <= old_end else 1.0
                gain = decide_tianti_gain(minutes=window.minutes, base_per_min=int(level.get("hp_gain_per_min", 0)), base_ratio=base_ratio, gain_pct=gain_pct, bath_effect=old_bath if old_bath > 1 else 1.0, sect_bonus=_sect_bonus(sect_fairyland_level), spirit_vein_multiplier=float(self.spirit_vein_multiplier()), old_hp=int(data["tianti_hp"]), hp_cap=self.profile.cap(data))
                data["tianti_hp"] = gain.new_hp
                settlement.update({"mins": window.minutes, "real_gain": gain.real_gain, "new_hp": gain.new_hp})
            data["last_settle_time"] = now_text
            end_time = started_at + timedelta(minutes=int(duration_minutes))
            bath_name = f"{slot_name}药浴（" + "、".join(f"{item['name']}x{item['amount']}" for item in plan) + "）"
            data.update({"medicine_last_time": now_text, "medicine_end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"), "medicine_effect": float(effect), "medicine_name": bath_name})
            for item in plan:
                changed = uow.execute("UPDATE back SET goods_num=goods_num-?, bind_num=MIN(COALESCE(bind_num,0), goods_num-?) WHERE user_id=? AND goods_id=? AND goods_num>=?", (item["amount"], item["amount"], user_id, item["item_id"], item["amount"]))
                if changed.rowcount != 1:
                    return MedicineBathPersistenceResult("item_changed", user_id)
            values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field] for field in fields]
            columns_sql = ", ".join(["user_id", *fields])
            placeholders = ", ".join("?" for _ in range(len(values) + 1))
            updates = ", ".join(f'"{field}"=excluded."{field}"' for field in fields)
            uow.execute(f'INSERT INTO player_data.tianti_info ({columns_sql}) VALUES ({placeholders}) ON CONFLICT(user_id) DO UPDATE SET {updates}', (user_id, *values))
            payload = {"consumed": list(plan), "effect": float(effect), "bath_name": bath_name, "end_time": data["medicine_end_time"], "settlement": settlement}
            uow.execute("INSERT INTO tianti_medicine_bath_operations(operation_id,user_id,request_json,result_json) VALUES(?,?,?,?)", (operation_id, user_id, json.dumps({"plan": list(plan), "effect": effect}, ensure_ascii=False), json.dumps(payload, ensure_ascii=False, default=str)))
            return MedicineBathPersistenceResult("applied", user_id, tuple(plan), float(effect), bath_name, data["medicine_end_time"], settlement)


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


class TiantiQiaoxueSqlRepository:
    """Feature-owned qiaoxue persistence on player.db."""

    def __init__(self, player_database: str | Path, *, profile_reader: TiantiProfileReader | None = None) -> None:
        self.player_database = str(player_database)
        self.profile = profile_reader or TiantiProfileReader(Path(player_database).parent / "xiuxian")

    def open(self, operation_id: str, user_id: str, roll: int) -> Any:
        operation_id, user_id, roll = str(operation_id).strip(), str(user_id), int(roll)
        if not operation_id or roll < 0:
            raise ValueError("operation_id is required and roll must be non-negative")
        fields = tuple(self.profile.default_data().keys())
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            operation_table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='tianti_qiaoxue_operations'")
            player_table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='tianti_info'")
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(tianti_info)")}
            if operation_table is None or player_table is None or not set(fields).issubset(columns):
                raise RuntimeError("tianti training schema is not ready; run migrations first")
            previous = uow.query_one("SELECT user_id, roll, qiaoxue_json, hp_cost, new_hp, opened_count, unlock_limit FROM tianti_qiaoxue_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["user_id"]) != user_id or int(previous["roll"]) != roll:
                    return QiaoxuePersistenceResult("state_changed", user_id, {}, 0, 0, 0, 0)
                return QiaoxuePersistenceResult("duplicate", user_id, json.loads(previous["qiaoxue_json"]), int(previous["hp_cost"]), int(previous["new_hp"]), int(previous["opened_count"]), int(previous["unlock_limit"]))
            row = uow.query_one("SELECT * FROM tianti_info WHERE user_id=?", (user_id,))
            if row is None:
                return QiaoxuePersistenceResult("user_missing", user_id, {}, 0, 0, 0, 0)
            data = self.profile.clean(row)
            opened = list(data.get("opened_qiaoxue", []) or [])
            opened_count = len(opened)
            unlock_limit = min(int(self.profile.levels()[str(data["tianti_level"])]["rank"]) * 3, 108)
            candidates = [item for item in self.profile.qiaoxue_pool() if item["name"] not in set(opened)]
            if opened_count >= unlock_limit or not candidates:
                return QiaoxuePersistenceResult("limit_reached", user_id, {}, 0, int(data["tianti_hp"]), opened_count, unlock_limit)
            old_hp = int(data["tianti_hp"])
            hp_cost = max(1, int(old_hp * 0.1))
            if old_hp < hp_cost:
                return QiaoxuePersistenceResult("hp_insufficient", user_id, {}, 0, old_hp, opened_count, unlock_limit)
            chosen = dict(candidates[roll % len(candidates)])
            detail = list(data.get("opened_qiaoxue_detail", []) or [])
            detail.append({"name": chosen["name"], "group": chosen["group"], "effect_type": chosen["effect_type"], "effect_value": float(chosen["effect_value"])})
            data["tianti_hp"] = old_hp - hp_cost
            data["opened_qiaoxue"] = opened + [chosen["name"]]
            data["opened_qiaoxue_detail"] = detail
            values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (list, dict)) else data[field] for field in fields]
            columns_sql = ", ".join(["user_id", *fields])
            placeholders = ", ".join("?" for _ in range(len(values) + 1))
            updates = ", ".join(f'"{field}"=excluded."{field}"' for field in fields)
            uow.execute(f'INSERT INTO tianti_info ({columns_sql}) VALUES ({placeholders}) ON CONFLICT(user_id) DO UPDATE SET {updates}', (user_id, *values))
            new_count = opened_count + 1
            uow.execute("INSERT INTO tianti_qiaoxue_operations(operation_id,user_id,roll,qiaoxue_json,hp_cost,new_hp,opened_count,unlock_limit) VALUES(?,?,?,?,?,?,?,?)", (operation_id, user_id, roll, json.dumps(chosen, ensure_ascii=False), hp_cost, data["tianti_hp"], new_count, unlock_limit))
            return QiaoxuePersistenceResult("opened", user_id, chosen, hp_cost, int(data["tianti_hp"]), new_count, unlock_limit)

    def open_qiaoxue(self, operation_id: str, user_id: str, roll: int) -> Any:
        return self.open(operation_id, user_id, roll)


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
    "TiantiMedicineBathSqlRepository",
    "TiantiBreakthroughSqlRepository",
    "TiantiProfileReader",
    "TiantiQiaoxueSqlRepository",
    "TiantiTrainingRepository",
]
