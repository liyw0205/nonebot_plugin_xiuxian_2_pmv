from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    canonical,
    encode_field,
    new_default_state,
    normalize_state,
)


STATISTICS_FIELD = "前尘往事次数"
@dataclass(frozen=True)
class PastLifeStartResult:
    status: str
    message: str = ""
    choices_count: int = 0
    alloc: dict = field(default_factory=dict)
    talent: str = ""
    birth_scenario: str = ""
    revision: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PastLifeStartService:
    """Atomically create one frozen past-life run and its derived statistic."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)"
        )
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(past_life)").fetchall()
        }
        for field_name in PAST_LIFE_FIELDS:
            if field_name not in columns:
                data_type = "INTEGER" if field_name in INTEGER_FIELDS else "TEXT"
                conn.execute(
                    "ALTER TABLE player_data.past_life ADD COLUMN "
                    f"{db_backend.quote_ident(field_name)} {data_type} DEFAULT NULL"
                )

        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)"
        )
        statistics_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()
        }
        if STATISTICS_FIELD not in statistics_columns:
            conn.execute(
                "ALTER TABLE player_data.statistics ADD COLUMN "
                f"{db_backend.quote_ident(STATISTICS_FIELD)} INTEGER DEFAULT 0"
            )

        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_start_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _encode_result(result: PastLifeStartResult) -> str:
        return json.dumps(
            asdict(result), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _decode_result(status: str, raw: str) -> PastLifeStartResult:
        value = json.loads(raw)
        value["status"] = status
        return PastLifeStartResult(**value)

    @staticmethod
    def _parse_time(value) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _read_state(conn, user_id: str) -> tuple[dict, bool]:
        cursor = conn.execute(
            "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return new_default_state(), False
        columns = [str(column[0]) for column in cursor.description]
        return normalize_state(dict(zip(columns, row))), True

    @staticmethod
    def _write_state(conn, user_id: str, state: dict, exists: bool) -> None:
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        if exists:
            assignments = ",".join(
                f"{db_backend.quote_ident(name)}=%s" for name in PAST_LIFE_FIELDS
            )
            conn.execute(
                f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
                (*values, user_id),
            )
            return

        fields = ("user_id", *PAST_LIFE_FIELDS)
        conn.execute(
            "INSERT INTO player_data.past_life("
            + ",".join(db_backend.quote_ident(name) for name in fields)
            + ") VALUES("
            + ",".join("%s" for _ in fields)
            + ")",
            (user_id, *values),
        )

    @staticmethod
    def _increment_statistic(conn, user_id: str) -> None:
        field_sql = db_backend.quote_ident(STATISTICS_FIELD)
        changed = conn.execute(
            f"UPDATE player_data.statistics SET {field_sql}=COALESCE({field_sql},0)+1 "
            "WHERE user_id=%s",
            (user_id,),
        )
        if changed.rowcount == 0:
            conn.execute(
                f"INSERT INTO player_data.statistics(user_id,{field_sql}) VALUES(%s,1)",
                (user_id,),
            )

    def get_result(
        self, operation_id, user_id=None
    ) -> PastLifeStartResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS past_life_start_operations("
                "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
                "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.commit()
            row = conn.execute(
                "SELECT user_id,result_json FROM past_life_start_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            if user_id is not None and str(row[0]) != str(user_id):
                return PastLifeStartResult("operation_conflict")
            return self._decode_result("duplicate", str(row[1]))

    def start(
        self,
        operation_id,
        user_id,
        expected_state,
        *,
        alloc,
        accumulated,
        talent,
        birth_scenario,
        event_indices,
        event_snapshots,
        first_stage_message,
        choices_count,
        refresh_slot_start,
    ) -> PastLifeStartResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        alloc = dict(alloc)
        accumulated = dict(accumulated)
        talent = str(talent)
        birth_scenario = str(birth_scenario)
        event_indices = [int(value) for value in event_indices]
        event_snapshots = list(event_snapshots)
        first_stage_message = str(first_stage_message)
        choices_count = int(choices_count)
        slot_start = self._parse_time(refresh_slot_start)
        if (
            not operation_id
            or not user_id
            or not talent
            or not birth_scenario
            or not first_stage_message
            or choices_count <= 0
            or slot_start is None
            or not event_indices
            or len(event_indices) != len(event_snapshots)
        ):
            raise ValueError("complete past life start plan is required")

        expected = normalize_state(expected_state)
        plan = {
            "alloc": alloc,
            "accumulated": accumulated,
            "talent": talent,
            "birth_scenario": birth_scenario,
            "event_indices": event_indices,
            "event_snapshots": event_snapshots,
        }
        payload = canonical(
            {
                "user_id": user_id,
                "plan": plan,
                "message": first_stage_message,
                "choices_count": choices_count,
                "refresh_slot_start": slot_start.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)

                previous = conn.execute(
                    "SELECT user_id,result_json FROM past_life_start_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id:
                        return PastLifeStartResult("operation_conflict")
                    return self._decode_result("duplicate", str(previous[1]))

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return PastLifeStartResult("user_missing")

                current, exists = self._read_state(conn, user_id)
                if int(current["state"]) not in {0, 1}:
                    conn.rollback()
                    return PastLifeStartResult("already_started")

                last_run = self._parse_time(current.get("last_run_time"))
                if last_run is not None and last_run >= slot_start:
                    conn.rollback()
                    return PastLifeStartResult("cooldown")

                if canonical(current) != canonical(expected):
                    conn.rollback()
                    return PastLifeStartResult("state_changed")

                persisted = dict(current)
                persisted.update(plan)
                persisted.update(
                    {
                        "state": 2,
                        "stage": 0,
                        "revision": int(current.get("revision", 0) or 0) + 1,
                        "total_score": 0,
                        "score_breakdown": {},
                        "early_death_rolls": {},
                        "history": [],
                    }
                )
                self._write_state(conn, user_id, persisted, exists)
                self._increment_statistic(conn, user_id)

                result = PastLifeStartResult(
                    "applied",
                    first_stage_message,
                    choices_count,
                    alloc,
                    talent,
                    birth_scenario,
                    int(persisted["revision"]),
                )
                conn.execute(
                    "INSERT INTO past_life_start_operations("
                    "operation_id,user_id,payload,result_json) VALUES(%s,%s,%s,%s)",
                    (operation_id, user_id, payload, self._encode_result(result)),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass
