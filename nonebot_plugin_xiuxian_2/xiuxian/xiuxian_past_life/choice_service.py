from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .past_life_state import (
    INTEGER_FIELDS,
    PAST_LIFE_FIELDS,
    canonical,
    encode_field,
    normalize_state,
)


IMMUTABLE_RUN_FIELDS = (
    "alloc",
    "talent",
    "birth_scenario",
    "event_indices",
    "event_snapshots",
    "last_run_time",
    "total_runs",
    "best_ending",
    "best_score",
    "endings_log",
    "achievement_points",
)


@dataclass(frozen=True)
class PastLifeChoiceResult:
    status: str
    response: dict = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PastLifeChoiceService:
    """Atomically advance a non-terminal past-life stage and journal its reply."""

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
    def ensure_operation_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_choice_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "payload TEXT NOT NULL,response_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def record_result(
        cls,
        conn,
        operation_id: str,
        user_id: str,
        kind: str,
        payload: str,
        response: dict,
    ) -> str:
        cls.ensure_operation_schema(conn)
        previous = conn.execute(
            "SELECT user_id,kind,payload,response_json "
            "FROM past_life_choice_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        response_json = canonical(response)
        if previous is not None:
            if (
                str(previous[0]) == str(user_id)
                and str(previous[1]) == str(kind)
                and str(previous[2]) == str(payload)
                and str(previous[3]) == response_json
            ):
                return "duplicate"
            return "operation_conflict"
        conn.execute(
            "INSERT INTO past_life_choice_operations("
            "operation_id,user_id,kind,payload,response_json) VALUES(%s,%s,%s,%s,%s)",
            (operation_id, str(user_id), str(kind), str(payload), response_json),
        )
        return "applied"

    @classmethod
    def _ensure_schema(cls, conn) -> None:
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
        cls.ensure_operation_schema(conn)

    @staticmethod
    def _read_state(conn, user_id: str) -> dict | None:
        cursor = conn.execute(
            "SELECT * FROM player_data.past_life WHERE user_id=%s", (user_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [str(column[0]) for column in cursor.description]
        return normalize_state(dict(zip(columns, row)))

    @staticmethod
    def _write_state(conn, user_id: str, state: dict) -> None:
        assignments = ",".join(
            f"{db_backend.quote_ident(name)}=%s" for name in PAST_LIFE_FIELDS
        )
        values = [encode_field(name, state[name]) for name in PAST_LIFE_FIELDS]
        changed = conn.execute(
            f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
            (*values, user_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("past life state disappeared during choice settlement")

    def get_result(
        self, operation_id, user_id=None
    ) -> PastLifeChoiceResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self.ensure_operation_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT user_id,response_json FROM past_life_choice_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            if user_id is not None and str(row[0]) != str(user_id):
                return PastLifeChoiceResult("operation_conflict")
            return PastLifeChoiceResult("duplicate", json.loads(str(row[1])))

    def advance(
        self,
        operation_id,
        user_id,
        choice_idx,
        expected_state,
        final_state,
        response,
    ) -> PastLifeChoiceResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        choice_idx = int(choice_idx)
        expected = normalize_state(expected_state)
        final = normalize_state(final_state)
        response = dict(response)
        if (
            not operation_id
            or not user_id
            or choice_idx <= 0
            or not response.get("message")
            or response.get("is_end") is not False
            or int(expected["state"]) != 2
            or int(final["state"]) != 2
            or int(final["stage"]) != int(expected["stage"]) + 1
            or int(final["revision"]) != int(expected["revision"]) + 1
        ):
            raise ValueError("valid non-terminal past life choice is required")
        for field_name in IMMUTABLE_RUN_FIELDS:
            if canonical(final[field_name]) != canonical(expected[field_name]):
                raise ValueError(f"past life choice changed immutable field: {field_name}")

        payload = canonical(
            {
                "user_id": user_id,
                "choice_idx": choice_idx,
                "expected": expected,
                "final": final,
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
                    "SELECT user_id,response_json FROM past_life_choice_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id:
                        return PastLifeChoiceResult("operation_conflict")
                    return PastLifeChoiceResult(
                        "duplicate", json.loads(str(previous[1]))
                    )

                user = conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                current = self._read_state(conn, user_id)
                if user is None or current is None:
                    conn.rollback()
                    return PastLifeChoiceResult("user_missing")
                if canonical(current) != canonical(expected):
                    conn.rollback()
                    return PastLifeChoiceResult("state_changed")

                self._write_state(conn, user_id, final)
                recorded = self.record_result(
                    conn,
                    operation_id,
                    user_id,
                    "advance",
                    payload,
                    response,
                )
                if recorded != "applied":
                    conn.rollback()
                    return PastLifeChoiceResult(recorded)
                conn.commit()
                return PastLifeChoiceResult("applied", response)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass
