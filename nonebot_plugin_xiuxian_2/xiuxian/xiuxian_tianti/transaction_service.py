from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from datetime import datetime
from ..xiuxian_config import XiuConfig
from ..xiuxian_world_events import get_spirit_vein_tianti_multiplier
from .tianti_data import get_next_tianti_level_name, get_tianti_level_data
from ..xiuxian_utils import db_backend
from .tianti_data import TiantiDataManager
from .tianti_data import (
    TiantiDataManager,
    get_next_tianti_level_name,
    get_tianti_level_data,
    get_tianti_level_index,
)
from .tianti_data import TiantiDataManager, get_qiaoxue_pool, get_tianti_level_data
from datetime import datetime, timedelta

def get_tianti_cap(data: dict) -> int:
    """
    上限规则：next_need_hp * closing_exp_upper_limit
    """
    next_name = get_next_tianti_level_name(data["tianti_level"])
    if not next_name:
        return 10**30
    need_hp = int(get_tianti_level_data(next_name)["need_hp"])
    return int(need_hp * XiuConfig().closing_exp_upper_limit)

def calc_qiaoxue_bonus(data: dict):
    """
    统计已开窍穴总加成
    """
    base_ratio = 0.0
    gain_pct = 0.0
    detail_list = data.get("opened_qiaoxue_detail", [])
    for q in detail_list:
        et = q["effect_type"]
        ev = float(q["effect_value"])
        if et == "base_per_min_ratio":
            base_ratio += ev
        elif et == "hp_gain_pct":
            gain_pct += ev
    return base_ratio, gain_pct

def parse_tianti_time(value):
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

def clear_medicine_bath(data: dict):
    data["medicine_last_time"] = None
    data["medicine_end_time"] = None
    data["medicine_effect"] = 0.0
    data["medicine_name"] = ""

def get_active_medicine_bath(data: dict, now_t: datetime):
    end_t = parse_tianti_time(data.get("medicine_end_time"))
    if not end_t or now_t > end_t:
        return None
    try:
        effect = float(data.get("medicine_effect", 0) or 0)
    except Exception:
        effect = 0.0
    if effect <= 1:
        return None
    return {
        "name": data.get("medicine_name") or "未知药材",
        "effect": effect,
        "end_time": end_t,
    }

def get_sect_fairyland_bonus(level: int) -> float:
    try:
        level = int(level or 0)
    except Exception:
        level = 0
    return max(0, min(level, 10)) * 0.05

def _apply_tianti_minutes(data: dict, mins: int, now_t: datetime, sect_fairyland_level: int = 0):
    lvl_data = get_tianti_level_data(data["tianti_level"])
    base_per_min = int(lvl_data["hp_gain_per_min"])
    base_ratio, gain_pct = calc_qiaoxue_bonus(data)
    real_per_min = int(base_per_min * (1 + base_ratio))

    bath = get_active_medicine_bath(data, now_t)
    bath_effect = bath["effect"] if bath else 1.0
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    spirit_vein_multiplier = get_spirit_vein_tianti_multiplier()
    bath_expired = False
    if not bath and data.get("medicine_end_time"):
        clear_medicine_bath(data)
        bath_expired = True

    gain = int(mins * real_per_min * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)
    cap = get_tianti_cap(data)
    old_hp = int(data["tianti_hp"])
    new_hp = min(cap, old_hp + gain)
    real_gain = max(0, new_hp - old_hp)
    data["tianti_hp"] = new_hp

    return {
        "status": "ok",
        "mins": mins,
        "real_gain": real_gain,
        "new_hp": new_hp,
        "cap": cap,
        "bath": bath,
        "bath_expired": bath_expired,
        "sect_bonus": sect_bonus,
        "spirit_vein_bonus": spirit_vein_multiplier - 1,
    }

def calc_tianti_gain_rate(data: dict, now_t: datetime | None = None, sect_fairyland_level: int = 0):
    """
    计算当前炼体每分钟收益，保持与实际结算公式一致。
    """
    now_t = now_t or datetime.now()
    lvl_data = get_tianti_level_data(data["tianti_level"])
    base_per_min = int(lvl_data["hp_gain_per_min"])
    base_ratio, gain_pct = calc_qiaoxue_bonus(data)
    real_per_min = int(base_per_min * (1 + base_ratio))

    bath = get_active_medicine_bath(data, now_t)
    bath_effect = bath["effect"] if bath else 1.0
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    spirit_vein_multiplier = get_spirit_vein_tianti_multiplier()
    per_min = int(real_per_min * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)

    return {
        "base_per_min": base_per_min,
        "base_ratio": base_ratio,
        "gain_pct": gain_pct,
        "bath": bath,
        "bath_effect": bath_effect,
        "sect_bonus": sect_bonus,
        "spirit_vein_bonus": spirit_vein_multiplier - 1,
        "per_min": per_min,
        "efficiency": (per_min / base_per_min) if base_per_min > 0 else 0,
    }

def settle_tianti_gain(data: dict, now_t: datetime, sect_fairyland_level: int = 0):
    last_t = parse_tianti_time(data.get("last_settle_time"))
    if not last_t:
        data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
        return {"status": "init"}

    mins = max(0, int((now_t - last_t).total_seconds() // 60))
    if mins <= 0:
        return {"status": "empty", "mins": mins}

    result = _apply_tianti_minutes(data, mins, now_t, sect_fairyland_level)
    data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
    return result

def grant_tianti_settle_minutes(
    data: dict,
    minutes: int,
    now_t: datetime | None = None,
    sect_fairyland_level: int = 0,
):
    """
    按当前炼体状态发放指定分钟数的炼体气血，不改变正常炼体结算时间。
    """
    now_t = now_t or datetime.now()
    mins = max(0, int(minutes))
    if mins <= 0:
        return {"status": "empty", "mins": mins}
    return _apply_tianti_minutes(data, mins, now_t, sect_fairyland_level)

@dataclass(frozen=True)
class TiantiSettlementResult:
    status: str
    user_id: str
    detail: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}

class TiantiSettlementService:
    """Settle elapsed tianti gain atomically and reuse the first event result."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_settlement_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_level INTEGER NOT NULL, "
            "result_status TEXT NOT NULL, detail_json TEXT NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(tianti_info)").fetchall()}
        for field in fields:
            if field not in columns:
                conn.execute(f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT")

    def get_result(self, operation_id: str) -> TiantiSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn, tuple(self._manager._default().keys()))
            previous = conn.execute(
                "SELECT user_id, detail_json FROM tianti_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return TiantiSettlementResult("duplicate", str(previous[0]), json.loads(previous[1]))

    def settle(self, operation_id, user_id, now_t: datetime,
               *, sect_fairyland_level=0) -> TiantiSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        sect_level = int(sect_fairyland_level)
        if not operation_id:
            raise ValueError("operation_id is required")

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, sect_level, detail_json FROM tianti_settlement_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != sect_level:
                        return TiantiSettlementResult("state_changed", user_id, {})
                    return TiantiSettlementResult("duplicate", user_id, json.loads(previous[2]))

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                detail = settle_tianti_gain(data, now_t, sect_level)
                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_settlement_operations "
                    "(operation_id, user_id, sect_level, result_status, detail_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_level, str(detail["status"]),
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return TiantiSettlementResult("settled", user_id, detail)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class StoneTrainingResult:
    status: str
    user_id: str
    requested_stone: int
    stone_cost: int
    hp_gain: int
    new_hp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"trained", "duplicate"}

class StoneTrainingService:
    """Exchange stones for tianti HP across attached SQLite databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_stone_training_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_stone INTEGER NOT NULL, "
            "stone_cost INTEGER NOT NULL, hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute(
                "PRAGMA player_data.table_info(tianti_info)"
            ).fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    def get_result(self, operation_id: str) -> StoneTrainingResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS tianti_stone_training_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_stone INTEGER NOT NULL, "
                "stone_cost INTEGER NOT NULL, hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT user_id, requested_stone, stone_cost, hp_gain, new_hp "
                "FROM tianti_stone_training_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return StoneTrainingResult(
                "duplicate", str(previous[0]), int(previous[1]), int(previous[2]), int(previous[3]), int(previous[4])
            )

    def train(self, operation_id, user_id, requested_stone) -> StoneTrainingResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        requested_stone = int(requested_stone)
        if not operation_id or requested_stone <= 0:
            raise ValueError("operation_id and requested_stone must be positive")

        def result(status, stone_cost=0, hp_gain=0, new_hp=0):
            return StoneTrainingResult(
                status, user_id, requested_stone, int(stone_cost), int(hp_gain), int(new_hp)
            )

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, requested_stone, stone_cost, hp_gain, new_hp "
                    "FROM tianti_stone_training_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != requested_stone:
                        return result("state_changed")
                    return result("duplicate", previous[2], previous[3], previous[4])

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if int(user[0]) < requested_stone:
                    conn.rollback()
                    return result("stone_insufficient")

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.tianti_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                old_hp = int(data["tianti_hp"])
                cap = get_tianti_cap(data)
                requested_gain = requested_stone // 10
                new_hp = min(cap, old_hp + requested_gain)
                hp_gain = max(0, new_hp - old_hp)
                stone_cost = hp_gain * 10
                if stone_cost <= 0:
                    conn.rollback()
                    return result("at_cap", new_hp=old_hp)

                charged = conn.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-CAST(%s AS REAL) "
                    "WHERE user_id=%s AND CAST(COALESCE(stone,0) AS REAL)>=CAST(%s AS REAL)",
                    (stone_cost, user_id, stone_cost),
                )
                if charged.rowcount != 1:
                    conn.rollback()
                    return result("stone_changed")
                data["tianti_hp"] = new_hp
                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO player_data.tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}",
                    (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_stone_training_operations "
                    "(operation_id, user_id, requested_stone, stone_cost, hp_gain, new_hp) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, requested_stone, stone_cost, hp_gain, new_hp),
                )
                conn.commit()
                return result("trained", stone_cost, hp_gain, new_hp)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class TiantiBreakthroughResult:
    status: str
    user_id: str
    old_level: str
    new_level: str
    hp_cost: int
    new_hp: int
    success: bool

    @property
    def succeeded(self) -> bool:
        return self.status in {"completed", "duplicate"}

class TiantiBreakthroughService:
    """Resolve one breakthrough attempt atomically and idempotently."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_breakthrough_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cultivation_rank INTEGER NOT NULL, "
            "roll_success INTEGER NOT NULL, old_level TEXT NOT NULL, new_level TEXT NOT NULL, "
            "hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, success INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(tianti_info)").fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    @staticmethod
    def _result(status, user_id, row=None):
        if row is None:
            return TiantiBreakthroughResult(status, user_id, "", "", 0, 0, False)
        return TiantiBreakthroughResult(
            status, user_id, str(row[0]), str(row[1]), int(row[2]), int(row[3]), bool(row[4])
        )

    def get_result(self, operation_id: str) -> TiantiBreakthroughResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn, tuple(self._manager._default().keys()))
            previous = conn.execute(
                "SELECT user_id, old_level, new_level, hp_cost, new_hp, success "
                "FROM tianti_breakthrough_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return TiantiBreakthroughResult(
                "duplicate", str(previous[0]), str(previous[1]), str(previous[2]),
                int(previous[3]), int(previous[4]), bool(previous[5]),
            )

    def attempt(self, operation_id, user_id, *, cultivation_rank: int,
                roll_success: bool) -> TiantiBreakthroughResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        cultivation_rank = int(cultivation_rank)
        roll_success = bool(roll_success)
        if not operation_id:
            raise ValueError("operation_id is required")

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, cultivation_rank, roll_success, old_level, new_level, "
                    "hp_cost, new_hp, success FROM tianti_breakthrough_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != cultivation_rank
                    ):
                        return self._result("state_changed", user_id)
                    return self._result("duplicate", user_id, previous[3:])

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                old_level = str(data["tianti_level"])
                next_level = get_next_tianti_level_name(old_level)
                if not next_level:
                    conn.rollback()
                    return self._result("max_level", user_id)
                next_config = get_tianti_level_data(next_level)
                required_rank = get_tianti_level_index(
                    next_config["min_xx_level"], is_xiuxian=True
                )
                if cultivation_rank > required_rank:
                    conn.rollback()
                    return self._result("cultivation_insufficient", user_id)

                required_hp = int(next_config["need_hp"])
                old_hp = int(data["tianti_hp"])
                if old_hp < required_hp:
                    conn.rollback()
                    return self._result("hp_insufficient", user_id)

                hp_cost = max(1, int(old_hp * 0.05))
                new_hp = max(0, old_hp - hp_cost)
                new_level = next_level if roll_success else old_level
                data["tianti_hp"] = new_hp
                data["tianti_level"] = new_level
                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_breakthrough_operations "
                    "(operation_id, user_id, cultivation_rank, roll_success, old_level, new_level, "
                    "hp_cost, new_hp, success) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, cultivation_rank, int(roll_success), old_level,
                     new_level, hp_cost, new_hp, int(roll_success)),
                )
                conn.commit()
                return self._result(
                    "completed", user_id,
                    (old_level, new_level, hp_cost, new_hp, int(roll_success)),
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class QiaoxueResult:
    status: str
    user_id: str
    qiaoxue: dict
    hp_cost: int
    new_hp: int
    opened_count: int
    unlock_limit: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"opened", "duplicate"}

class QiaoxueService:
    """Open one random qiaoxue in an idempotent player database transaction."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_qiaoxue_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, roll INTEGER NOT NULL, "
            "qiaoxue_json TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
            "opened_count INTEGER NOT NULL, unlock_limit INTEGER NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(tianti_info)").fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    @staticmethod
    def _result(status, user_id, qiaoxue=None, hp_cost=0, new_hp=0,
                opened_count=0, unlock_limit=0):
        return QiaoxueResult(
            status, user_id, dict(qiaoxue or {}), int(hp_cost), int(new_hp),
            int(opened_count), int(unlock_limit),
        )

    def get_result(self, operation_id: str) -> QiaoxueResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn, tuple(self._manager._default().keys()))
            previous = conn.execute(
                "SELECT user_id, qiaoxue_json, hp_cost, new_hp, opened_count, unlock_limit "
                "FROM tianti_qiaoxue_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return QiaoxueResult(
                "duplicate", str(previous[0]), json.loads(previous[1]),
                int(previous[2]), int(previous[3]), int(previous[4]), int(previous[5]),
            )

    def open(self, operation_id, user_id, roll) -> QiaoxueResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        roll = int(roll)
        if not operation_id or roll < 0:
            raise ValueError("operation_id is required and roll must be non-negative")

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, roll, qiaoxue_json, hp_cost, new_hp, opened_count, unlock_limit "
                    "FROM tianti_qiaoxue_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id:
                        return self._result("state_changed", user_id)
                    return self._result(
                        "duplicate", user_id, json.loads(previous[2]), *previous[3:]
                    )

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                opened = list(data.get("opened_qiaoxue", []))
                opened_count = len(opened)
                unlock_limit = min(int(get_tianti_level_data(data["tianti_level"])["rank"]) * 3, 108)
                if opened_count >= unlock_limit:
                    conn.rollback()
                    return self._result(
                        "limit_reached", user_id, opened_count=opened_count,
                        unlock_limit=unlock_limit,
                    )

                opened_names = set(opened)
                candidates = [item for item in get_qiaoxue_pool() if item["name"] not in opened_names]
                if not candidates:
                    conn.rollback()
                    return self._result(
                        "limit_reached", user_id, opened_count=opened_count,
                        unlock_limit=unlock_limit,
                    )
                old_hp = int(data["tianti_hp"])
                hp_cost = max(1, int(old_hp * 0.1))
                if old_hp < hp_cost:
                    conn.rollback()
                    return self._result(
                        "hp_insufficient", user_id, opened_count=opened_count,
                        unlock_limit=unlock_limit,
                    )

                chosen = dict(candidates[roll % len(candidates)])
                detail = list(data.get("opened_qiaoxue_detail", []))
                detail.append({
                    "name": chosen["name"], "group": chosen["group"],
                    "effect_type": chosen["effect_type"],
                    "effect_value": float(chosen["effect_value"]),
                })
                new_hp = old_hp - hp_cost
                data["tianti_hp"] = new_hp
                data["opened_qiaoxue"] = opened + [chosen["name"]]
                data["opened_qiaoxue_detail"] = detail
                if not isinstance(data.get("qiaoxue_stage_opened"), dict):
                    data["qiaoxue_stage_opened"] = {}

                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                new_count = opened_count + 1
                conn.execute(
                    "INSERT INTO tianti_qiaoxue_operations "
                    "(operation_id, user_id, roll, qiaoxue_json, hp_cost, new_hp, "
                    "opened_count, unlock_limit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, roll, json.dumps(chosen, ensure_ascii=False),
                     hp_cost, new_hp, new_count, unlock_limit),
                )
                conn.commit()
                return self._result(
                    "opened", user_id, chosen, hp_cost, new_hp, new_count, unlock_limit
                )
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class MedicineBathResult:
    status: str
    user_id: str
    consumed: tuple[dict, ...]
    effect: float
    bath_name: str
    end_time: str
    settlement: dict
    insufficient: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class MedicineBathService:
    """Consume herbs and activate a medicine bath in one attached transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_medicine_bath_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
            "result_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS player_data.tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {
            str(row[1]) for row in conn.execute(
                "PRAGMA player_data.table_info(tianti_info)"
            ).fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    @staticmethod
    def _result_from_payload(status: str, user_id: str, payload: dict) -> MedicineBathResult:
        return MedicineBathResult(
            status=status,
            user_id=user_id,
            consumed=tuple(payload.get("consumed", ())),
            effect=float(payload.get("effect", 0)),
            bath_name=str(payload.get("bath_name", "")),
            end_time=str(payload.get("end_time", "")),
            settlement=dict(payload.get("settlement", {})),
            insufficient=tuple(payload.get("insufficient", ())),
        )

    def get_result(self, operation_id: str) -> MedicineBathResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS tianti_medicine_bath_operations ("
                "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
                "result_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT user_id, result_json FROM tianti_medicine_bath_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return self._result_from_payload("duplicate", str(previous[0]), json.loads(previous[1]))

    def apply(self, operation_id, user_id, consume_plan, effect, slot_name,
              now_t: datetime, duration_minutes: int, *, sect_fairyland_level=0):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        effect = float(effect)
        duration_minutes = int(duration_minutes)
        plan = tuple(
            {"item_id": int(item["item_id"]), "name": str(item["name"]), "amount": int(item["amount"])}
            for item in consume_plan
        )
        if not operation_id or not plan or any(item["amount"] <= 0 for item in plan):
            raise ValueError("operation_id and positive consume plan are required")
        request = {
            "plan": plan,
            "effect": effect,
            "slot_name": str(slot_name),
            "duration_minutes": duration_minutes,
            "sect_fairyland_level": int(sect_fairyland_level),
        }
        now_text = now_t.strftime("%Y-%m-%d %H:%M:%S")
        request_json = json.dumps(request, ensure_ascii=False, sort_keys=True)
        fields = tuple(self._manager._default().keys())

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, request_json, result_json FROM tianti_medicine_bath_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    # Request identity = user_id; plan/effect stored in result_json.
                    if str(previous[0]) != user_id:
                        return self._result_from_payload("state_changed", user_id, {})
                    return self._result_from_payload("duplicate", user_id, json.loads(previous[2]))

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                if get_active_medicine_bath(data, now_t):
                    conn.rollback()
                    return self._result_from_payload("bath_active", user_id, {})

                insufficient = []
                for item in plan:
                    stock = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item["item_id"]),
                    ).fetchone()
                    have = int(stock[0]) if stock else 0
                    if have < item["amount"]:
                        insufficient.append({**item, "have": have})
                if insufficient:
                    conn.rollback()
                    return self._result_from_payload(
                        "item_insufficient", user_id, {"insufficient": insufficient}
                    )

                settlement = settle_tianti_gain(data, now_t, int(sect_fairyland_level))
                if settlement["status"] == "empty":
                    data["last_settle_time"] = now_text
                end_t = now_t + timedelta(minutes=duration_minutes)
                bath_name = f"{slot_name}药浴（" + "、".join(
                    f"{item['name']}x{item['amount']}" for item in plan
                ) + "）"
                data.update({
                    "medicine_last_time": now_text,
                    "medicine_end_time": end_t.strftime("%Y-%m-%d %H:%M:%S"),
                    "medicine_effect": effect,
                    "medicine_name": bath_name,
                })

                for item in plan:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-%s, "
                        "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                        "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                        (item["amount"], item["amount"], user_id, item["item_id"], item["amount"]),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return self._result_from_payload("item_changed", user_id, {})

                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO player_data.tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                payload = {
                    "consumed": plan,
                    "effect": effect,
                    "bath_name": bath_name,
                    "end_time": data["medicine_end_time"],
                    "settlement": settlement,
                }
                conn.execute(
                    "INSERT INTO tianti_medicine_bath_operations "
                    "(operation_id, user_id, request_json, result_json) VALUES (%s, %s, %s, %s)",
                    (operation_id, user_id, request_json,
                     json.dumps(payload, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return self._result_from_payload("applied", user_id, payload)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

@dataclass(frozen=True)
class TiantiItemRewardResult:
    status: str
    user_id: str
    item_id: int
    quantity: int
    minutes: int
    detail: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class TiantiItemRewardService:
    """Consume an item and update tianti state across attached SQLite databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path,
                 lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_item_reward_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, item_id INTEGER NOT NULL, "
            "quantity INTEGER NOT NULL, minutes INTEGER NOT NULL, detail_json TEXT NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.tianti_info (user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1]) for row in conn.execute(
                "PRAGMA player_data.table_info(tianti_info)"
            ).fetchall()
        }
        for field in fields:
            if field not in columns:
                conn.execute(
                    f"ALTER TABLE player_data.tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )

    def apply(self, operation_id, user_id, item_id, quantity, minutes,
              *, sect_fairyland_level=0) -> TiantiItemRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        quantity = int(quantity)
        minutes = int(minutes)
        if not operation_id or quantity <= 0 or minutes <= 0:
            raise ValueError("operation_id, quantity and minutes must be positive")
        total_minutes = quantity * minutes

        def result(status, detail=None, result_quantity=quantity, result_minutes=total_minutes):
            return TiantiItemRewardResult(
                status, user_id, item_id, int(result_quantity), int(result_minutes), detail or {}
            )

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, item_id, quantity, minutes, detail_json "
                    "FROM tianti_item_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id
                        or int(previous[1]) != item_id
                        or int(previous[2]) != quantity
                        or int(previous[3]) != total_minutes
                    ):
                        return result("state_changed")
                    return result("duplicate", json.loads(previous[4]), previous[2], previous[3])

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM player_data.tianti_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                raw = dict(zip(fields, row)) if row else {}
                data = self._manager._clean_user_data(raw)
                detail = grant_tianti_settle_minutes(
                    data, total_minutes, sect_fairyland_level=sect_fairyland_level
                )
                consumed = conn.execute(
                    "UPDATE back SET goods_num=goods_num-%s, "
                    "bind_num=MIN(COALESCE(bind_num, 0), goods_num-%s) "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>=%s",
                    (quantity, quantity, user_id, item_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return result("item_insufficient")

                values = []
                for field in fields:
                    value = data[field]
                    values.append(json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value)
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO player_data.tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}",
                    (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_item_reward_operations "
                    "(operation_id, user_id, item_id, quantity, minutes, detail_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, item_id, quantity, total_minutes,
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return result("applied", detail)
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("DETACH DATABASE player_data")

__all__ = [
    "TiantiSettlementResult",
    "TiantiSettlementService",
    "StoneTrainingResult",
    "StoneTrainingService",
    "TiantiBreakthroughResult",
    "TiantiBreakthroughService",
    "QiaoxueResult",
    "QiaoxueService",
    "MedicineBathResult",
    "MedicineBathService",
    "TiantiItemRewardResult",
    "TiantiItemRewardService",
]
