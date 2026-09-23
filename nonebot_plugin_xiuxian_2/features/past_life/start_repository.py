from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork

PAST_LIFE_FIELDS = (
    "state", "stage", "revision", "alloc", "accumulated", "talent", "birth_scenario",
    "total_score", "score_breakdown", "event_indices", "event_snapshots", "early_death_rolls",
    "history", "last_run_time", "total_runs", "best_ending", "best_score", "endings_log",
    "achievement_points",
)
JSON_FIELDS = {"alloc", "accumulated", "score_breakdown", "event_indices", "event_snapshots", "early_death_rolls", "history", "endings_log"}
INTEGER_FIELDS = {"state", "stage", "revision", "total_score", "total_runs", "best_score", "achievement_points"}
DEFAULT_STATE = {
    "state": 0, "stage": 0, "revision": 0, "alloc": {},
    "accumulated": {"悟性": 0, "机缘": 0, "根骨": 0, "气运": 0, "心性": 0},
    "talent": "", "birth_scenario": "", "total_score": 0, "score_breakdown": {},
    "event_indices": [], "event_snapshots": [], "early_death_rolls": {}, "history": [],
    "last_run_time": None, "total_runs": 0, "best_ending": "", "best_score": 0,
    "endings_log": [], "achievement_points": 0,
}

@dataclass(frozen=True)
class PastLifeStartResult:
    status: str
    message: str = ""
    choices_count: int = 0
    alloc: dict | None = None
    talent: str = ""
    birth_scenario: str = ""
    revision: int = 0
    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class PastLifeStartSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path | None = None):
        self.game_database = str(game_database)
        self.player_database = str(player_database or game_database)

    @staticmethod
    def _canonical(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _normalize(cls, value):
        state = {}
        source = dict(value or {})
        for field in PAST_LIFE_FIELDS:
            raw = source.get(field, DEFAULT_STATE[field])
            if raw is None:
                raw = DEFAULT_STATE[field]
            if field in JSON_FIELDS and isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError):
                    pass
            if field in INTEGER_FIELDS:
                try:
                    raw = int(raw)
                except (TypeError, ValueError):
                    pass
            state[field] = raw
        return state

    @staticmethod
    def _encode(field, value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) if field in JSON_FIELDS else value

    @staticmethod
    def _parse_time(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    def _ensure_schema(self, uow):
        uow.execute("CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)")
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(past_life)")}
        for field in PAST_LIFE_FIELDS:
            if field not in columns:
                data_type = "INTEGER" if field in INTEGER_FIELDS else "TEXT"
                uow.execute(f'ALTER TABLE player_data.past_life ADD COLUMN "{field}" {data_type} DEFAULT NULL')
        uow.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(statistics)")}
        if "前尘往事次数" not in columns:
            uow.execute('ALTER TABLE player_data.statistics ADD COLUMN "前尘往事次数" INTEGER DEFAULT 0')
        uow.execute("CREATE TABLE IF NOT EXISTS past_life_start_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

    def _read_state(self, uow, user_id):
        row = uow.query_one("SELECT * FROM player_data.past_life WHERE user_id=?", (user_id,))
        if row is None:
            return self._normalize(None), False
        return self._normalize(dict(row)), True

    def _write_state(self, uow, user_id, state, exists):
        values = [self._encode(field, state[field]) for field in PAST_LIFE_FIELDS]
        if exists:
            assigns = ",".join(f'"{field}"=?' for field in PAST_LIFE_FIELDS)
            uow.execute(f"UPDATE player_data.past_life SET {assigns} WHERE user_id=?", (*values, user_id))
        else:
            fields = ",".join(["user_id", *[f'"{field}"' for field in PAST_LIFE_FIELDS]])
            marks = ",".join("?" for _ in range(len(PAST_LIFE_FIELDS) + 1))
            uow.execute(f"INSERT INTO player_data.past_life({fields}) VALUES({marks})", (user_id, *values))

    def start(self, operation_id, user_id, *, expected_state, alloc, accumulated, talent, birth_scenario, event_indices, event_snapshots, first_stage_message, choices_count, refresh_slot_start):
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        event_indices = [int(value) for value in event_indices]
        event_snapshots = list(event_snapshots)
        if not operation_id or not user_id or not talent or not birth_scenario or not first_stage_message or int(choices_count) <= 0 or self._parse_time(refresh_slot_start) is None or not event_indices or len(event_indices) != len(event_snapshots):
            raise ValueError("complete past life start plan is required")
        expected = self._normalize(expected_state)
        plan = {"alloc": dict(alloc), "accumulated": dict(accumulated), "talent": str(talent), "birth_scenario": str(birth_scenario), "event_indices": event_indices, "event_snapshots": event_snapshots}
        payload = self._canonical({"user_id": user_id, "plan": plan, "message": str(first_stage_message), "choices_count": int(choices_count), "refresh_slot_start": str(refresh_slot_start)})
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            self._ensure_schema(uow)
            old = uow.query_one("SELECT user_id,result_json FROM past_life_start_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                if str(old["user_id"]) != user_id:
                    return PastLifeStartResult("operation_conflict")
                data = json.loads(old["result_json"]); return PastLifeStartResult("duplicate", **data)
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return PastLifeStartResult("user_missing")
            current, exists = self._read_state(uow, user_id)
            if int(current["state"]) not in {0, 1}:
                return PastLifeStartResult("already_started")
            last_run = self._parse_time(current.get("last_run_time")); slot_start = self._parse_time(refresh_slot_start)
            if last_run is not None and last_run >= slot_start:
                return PastLifeStartResult("cooldown")
            if self._canonical(current) != self._canonical(expected):
                return PastLifeStartResult("state_changed")
            persisted = dict(current); persisted.update(plan); persisted.update({"state": 2, "stage": 0, "revision": int(current.get("revision", 0) or 0) + 1, "total_score": 0, "score_breakdown": {}, "early_death_rolls": {}, "history": []})
            self._write_state(uow, user_id, persisted, exists)
            stat = uow.query_one('SELECT "前尘往事次数" AS count FROM player_data.statistics WHERE user_id=?', (user_id,))
            if stat is None:
                uow.execute('INSERT INTO player_data.statistics(user_id,"前尘往事次数") VALUES(?,1)', (user_id,))
            else:
                uow.execute('UPDATE player_data.statistics SET "前尘往事次数"=COALESCE("前尘往事次数",0)+1 WHERE user_id=?', (user_id,))
            result = {"message": str(first_stage_message), "choices_count": int(choices_count), "alloc": dict(alloc), "talent": str(talent), "birth_scenario": str(birth_scenario), "revision": int(persisted["revision"])}
            uow.execute("INSERT INTO past_life_start_operations(operation_id,user_id,payload,result_json) VALUES(?,?,?,?)", (operation_id, user_id, payload, json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))))
            return PastLifeStartResult("applied", **result)
