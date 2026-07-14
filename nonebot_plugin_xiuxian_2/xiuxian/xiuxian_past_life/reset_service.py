from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
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


@dataclass(frozen=True)
class PastLifeResetResult:
    status: str
    operation_id: str
    mode: str = ""
    clear_history: bool = False
    task_status: str = ""
    user_id: str = ""
    total: int = 0
    processed: int = 0
    applied: int = 0
    conflicted: int = 0
    missing: int = 0
    last_error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "created", "resumed", "duplicate"}

    @property
    def complete(self) -> bool:
        return self.task_status == "completed"


class PastLifeResetService:
    """Reset one player atomically or a frozen all-player set in resumable chunks."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

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
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_reset_operations("
            "operation_id TEXT PRIMARY KEY,mode TEXT NOT NULL,user_id TEXT NOT NULL DEFAULT '',"
            "clear_history INTEGER NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,"
            "total INTEGER NOT NULL DEFAULT 0,processed INTEGER NOT NULL DEFAULT 0,"
            "applied INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
            "missing INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS past_life_reset_targets("
            "operation_id TEXT NOT NULL,ordinal INTEGER NOT NULL,user_id TEXT NOT NULL,"
            "expected_json TEXT NOT NULL,final_json TEXT NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',error_text TEXT NOT NULL DEFAULT '',"
            "updated_at TEXT NOT NULL,PRIMARY KEY(operation_id,user_id))"
        )

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
            changed = conn.execute(
                f"UPDATE player_data.past_life SET {assignments} WHERE user_id=%s",
                (*values, user_id),
            )
            if changed.rowcount != 1:
                raise RuntimeError("past life reset target disappeared")
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
    def _reset_state(current: dict, clear_history: bool) -> dict:
        default = new_default_state()
        final = dict(current)
        for field_name in (
            "state",
            "stage",
            "alloc",
            "accumulated",
            "talent",
            "birth_scenario",
            "total_score",
            "score_breakdown",
            "event_indices",
            "event_snapshots",
            "early_death_rolls",
            "history",
            "last_run_time",
        ):
            final[field_name] = default[field_name]
        final["revision"] = int(current.get("revision", 0) or 0) + 1
        if clear_history:
            for field_name in (
                "total_runs",
                "best_ending",
                "best_score",
                "endings_log",
                "achievement_points",
            ):
                final[field_name] = default[field_name]
        return normalize_state(final)

    @staticmethod
    def _payload(mode: str, clear_history: bool, user_id: str = "") -> str:
        return canonical(
            {
                "mode": str(mode),
                "clear_history": bool(clear_history),
                "user_id": str(user_id),
            }
        )

    @staticmethod
    def _result(conn, operation_id: str, status: str) -> PastLifeResetResult:
        row = conn.execute(
            "SELECT mode,clear_history,status,user_id,total,processed,applied,"
            "conflicted,missing,last_error FROM past_life_reset_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        if row is None:
            return PastLifeResetResult(status, operation_id)
        return PastLifeResetResult(
            status=status,
            operation_id=operation_id,
            mode=str(row[0]),
            clear_history=bool(int(row[1])),
            task_status=str(row[2]),
            user_id=str(row[3] or ""),
            total=int(row[4]),
            processed=int(row[5]),
            applied=int(row[6]),
            conflicted=int(row[7]),
            missing=int(row[8]),
            last_error=str(row[9] or ""),
        )

    def reset_one(
        self,
        operation_id,
        user_id,
        clear_history=False,
        *,
        updated_at=None,
    ) -> PastLifeResetResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        clear_history = bool(clear_history)
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = self._payload("single", clear_history, user_id)

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
                    "SELECT payload FROM past_life_reset_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    result = self._result(
                        conn,
                        operation_id,
                        "duplicate" if str(previous[0]) == payload else "operation_conflict",
                    )
                    conn.rollback()
                    return result

                current, exists = self._read_state(conn, user_id)
                final = self._reset_state(current, clear_history)
                self._write_state(conn, user_id, final, exists)
                conn.execute(
                    "INSERT INTO past_life_reset_operations("
                    "operation_id,mode,user_id,clear_history,payload,status,total,processed,"
                    "applied,created_at,updated_at) VALUES(%s,'single',%s,%s,%s,'completed',1,1,1,%s,%s)",
                    (
                        operation_id,
                        user_id,
                        int(clear_history),
                        payload,
                        updated_at,
                        updated_at,
                    ),
                )
                conn.commit()
                return self._result(conn, operation_id, "applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    def create_all(
        self,
        operation_id,
        clear_history=False,
        *,
        updated_at=None,
    ) -> PastLifeResetResult:
        operation_id = str(operation_id).strip()
        clear_history = bool(clear_history)
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id:
            raise ValueError("operation_id is required")
        payload = self._payload("all", clear_history)

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
                    "SELECT payload,status FROM past_life_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    if str(previous[0]) != payload:
                        status = "operation_conflict"
                    else:
                        status = "duplicate" if str(previous[1]) == "completed" else "resumed"
                    result = self._result(conn, operation_id, status)
                    conn.rollback()
                    return result

                running = conn.execute(
                    "SELECT operation_id,clear_history FROM past_life_reset_operations "
                    "WHERE mode='all' AND status='running' ORDER BY created_at LIMIT 1"
                ).fetchone()
                if running is not None:
                    running_id = str(running[0])
                    status = (
                        "resumed"
                        if bool(int(running[1])) == clear_history
                        else "operation_conflict"
                    )
                    result = self._result(conn, running_id, status)
                    conn.rollback()
                    return result

                cursor = conn.execute(
                    "SELECT * FROM player_data.past_life ORDER BY user_id"
                )
                columns = [str(column[0]) for column in cursor.description]
                users = []
                for row in cursor.fetchall():
                    value = dict(zip(columns, row))
                    user_id = str(value.pop("user_id"))
                    current = normalize_state(value)
                    users.append(
                        (
                            user_id,
                            canonical(current),
                            canonical(self._reset_state(current, clear_history)),
                        )
                    )
                task_status = "completed" if not users else "running"
                conn.execute(
                    "INSERT INTO past_life_reset_operations("
                    "operation_id,mode,clear_history,payload,status,total,created_at,updated_at) "
                    "VALUES(%s,'all',%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        int(clear_history),
                        payload,
                        task_status,
                        len(users),
                        updated_at,
                        updated_at,
                    ),
                )
                conn.executemany(
                    "INSERT INTO past_life_reset_targets("
                    "operation_id,ordinal,user_id,expected_json,final_json,updated_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s)",
                    [
                        (operation_id, ordinal, *user, updated_at)
                        for ordinal, user in enumerate(users)
                    ],
                )
                conn.commit()
                return self._result(conn, operation_id, "created")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    def run_batch(
        self,
        operation_id,
        *,
        batch_size=500,
        updated_at=None,
    ) -> PastLifeResetResult:
        operation_id = str(operation_id).strip()
        batch_size = max(1, int(batch_size))
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id:
            raise ValueError("operation_id is required")

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data", (str(self._player_database),)
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT mode,status FROM past_life_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if operation is None:
                    conn.rollback()
                    return PastLifeResetResult("not_found", operation_id)
                if str(operation[0]) != "all":
                    result = self._result(conn, operation_id, "operation_conflict")
                    conn.rollback()
                    return result
                if str(operation[1]) == "completed":
                    result = self._result(conn, operation_id, "duplicate")
                    conn.rollback()
                    return result

                targets = conn.execute(
                    "SELECT user_id,expected_json,final_json FROM past_life_reset_targets "
                    "WHERE operation_id=%s AND status='pending' ORDER BY ordinal LIMIT %s",
                    (operation_id, batch_size),
                ).fetchall()
                for target in targets:
                    user_id = str(target[0])
                    expected = normalize_state(json.loads(str(target[1])))
                    final = normalize_state(json.loads(str(target[2])))
                    current, exists = self._read_state(conn, user_id)
                    if not exists:
                        target_status, error_text = "missing", "state_missing"
                    elif canonical(current) != canonical(expected):
                        target_status, error_text = "conflict", "state_changed"
                    else:
                        self._write_state(conn, user_id, final, True)
                        target_status, error_text = "applied", ""
                    conn.execute(
                        "UPDATE past_life_reset_targets SET status=%s,error_text=%s,"
                        "updated_at=%s WHERE operation_id=%s AND user_id=%s "
                        "AND status='pending'",
                        (
                            target_status,
                            error_text,
                            updated_at,
                            operation_id,
                            user_id,
                        ),
                    )

                counts = conn.execute(
                    "SELECT COUNT(*),"
                    "COALESCE(SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='applied' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='conflict' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='missing' THEN 1 ELSE 0 END),0),"
                    "COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM past_life_reset_targets WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                task_status = "completed" if int(counts[5]) == 0 else "running"
                conn.execute(
                    "UPDATE past_life_reset_operations SET status=%s,total=%s,processed=%s,"
                    "applied=%s,conflicted=%s,missing=%s,last_error='',updated_at=%s "
                    "WHERE operation_id=%s",
                    (
                        task_status,
                        int(counts[0]),
                        int(counts[1]),
                        int(counts[2]),
                        int(counts[3]),
                        int(counts[4]),
                        updated_at,
                        operation_id,
                    ),
                )
                conn.commit()
                return self._result(conn, operation_id, "applied")
            except Exception as exc:
                conn.rollback()
                self._record_error(operation_id, str(exc), updated_at)
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    def _record_error(self, operation_id: str, error: str, updated_at: str) -> None:
        try:
            with closing(db_backend.connect(self._game_database)) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE past_life_reset_operations SET last_error=%s,updated_at=%s "
                    "WHERE operation_id=%s",
                    (error, updated_at, operation_id),
                )
                conn.commit()
        except Exception:
            pass

    def find_pending_all(self) -> PastLifeResetResult | None:
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                row = conn.execute(
                    "SELECT operation_id FROM past_life_reset_operations "
                    "WHERE mode='all' AND status='running' ORDER BY created_at LIMIT 1"
                ).fetchone()
            except Exception:
                return None
            if row is None:
                return None
            return self._result(conn, str(row[0]), "resumed")


__all__ = ["PastLifeResetResult", "PastLifeResetService"]
