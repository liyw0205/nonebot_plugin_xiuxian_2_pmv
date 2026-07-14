from __future__ import annotations

import json
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


TRAINING_FIELDS = (
    "progress",
    "last_time",
    "points",
    "completed",
    "max_progress",
    "last_event",
    "weekly_purchases",
)


def _as_date(value=None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalize_weekly_purchases(value, today=None) -> tuple[dict[str, int | str], bool]:
    today = _as_date(today)
    changed = False
    if isinstance(value, str):
        try:
            value = json.loads(value) if value else {}
        except (TypeError, ValueError):
            value = {}
            changed = True
    if not isinstance(value, dict):
        value = {}
        changed = True

    try:
        reset = date.fromisoformat(str(value.get("_last_reset", "")))
    except (TypeError, ValueError):
        reset = None
    if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
        return {"_last_reset": today.isoformat()}, True

    weekly: dict[str, int | str] = {"_last_reset": reset.isoformat()}
    for raw_key, raw_amount in value.items():
        key = str(raw_key)
        if key == "_last_reset":
            continue
        try:
            amount = int(raw_amount)
        except (TypeError, ValueError):
            changed = True
            continue
        if amount < 0:
            changed = True
            continue
        weekly[key] = amount
        if key != raw_key or not isinstance(raw_amount, int) or isinstance(raw_amount, bool):
            changed = True
    return weekly, changed


def normalize_weekly_purchases(value, today=None) -> dict[str, int | str]:
    """Return a canonical purchase snapshot for the ISO week containing today."""
    return _normalize_weekly_purchases(value, today)[0]


class TrainingStateService:
    """Initialize and advance a player's training state in one transaction."""

    _COLUMN_DEFINITIONS = {
        "progress": "TEXT DEFAULT '0'",
        "last_time": "TEXT DEFAULT NULL",
        "points": "TEXT DEFAULT '0'",
        "completed": "TEXT DEFAULT '0'",
        "max_progress": "TEXT DEFAULT '0'",
        "last_event": "TEXT DEFAULT ''",
        "weekly_purchases": "TEXT DEFAULT NULL",
    }

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _period_key(today: date) -> str:
        iso = today.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    @staticmethod
    def _integer(value) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _last_time(value) -> datetime | None:
        if value is None or str(value).strip() in {"", "None", "null"}:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    @classmethod
    def _state(cls, row, weekly_purchases) -> dict:
        return {
            "progress": cls._integer(row[0]),
            "last_time": cls._last_time(row[1]),
            "points": cls._integer(row[2]),
            "completed": cls._integer(row[3]),
            "max_progress": cls._integer(row[4]),
            "last_event": str(row[5] or ""),
            "weekly_purchases": weekly_purchases,
        }

    @staticmethod
    def _snapshot(state: dict) -> str:
        snapshot = dict(state)
        if isinstance(snapshot["last_time"], datetime):
            snapshot["last_time"] = snapshot["last_time"].strftime("%Y-%m-%d %H:%M:%S")
        return json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS training ("
            "user_id TEXT PRIMARY KEY,progress TEXT DEFAULT '0',last_time TEXT DEFAULT NULL,"
            "points TEXT DEFAULT '0',completed TEXT DEFAULT '0',max_progress TEXT DEFAULT '0',"
            "last_event TEXT DEFAULT '',weekly_purchases TEXT DEFAULT NULL)"
        )
        columns = {str(column[1]) for column in conn.execute("PRAGMA table_info(training)").fetchall()}
        for field, definition in cls._COLUMN_DEFINITIONS.items():
            if field not in columns:
                conn.execute(f'ALTER TABLE training ADD COLUMN "{field}" {definition}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS training_state_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _insert_operation(conn, operation_id, user_id, kind, period_key, state) -> None:
        conn.execute(
            "INSERT INTO training_state_operations("
            "operation_id,user_id,kind,period_key,snapshot,created_at) "
            "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (operation_id, user_id, kind, period_key, TrainingStateService._snapshot(state)),
        )

    def get(self, user_id, today=None) -> dict:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        today = _as_date(today)
        period_key = self._period_key(today)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT progress,last_time,points,completed,max_progress,last_event,"
                    "weekly_purchases FROM training WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    weekly = {"_last_reset": today.isoformat()}
                    state = self._state((0, None, 0, 0, 0, "", weekly), weekly)
                    conn.execute(
                        "INSERT INTO training("
                        "user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            user_id,
                            state["progress"],
                            None,
                            state["points"],
                            state["completed"],
                            state["max_progress"],
                            state["last_event"],
                            json.dumps(weekly, ensure_ascii=True, sort_keys=True),
                        ),
                    )
                    self._insert_operation(
                        conn,
                        f"training-state-init:{user_id}",
                        user_id,
                        "initialize",
                        period_key,
                        state,
                    )
                    conn.commit()
                    return state

                weekly, weekly_changed = _normalize_weekly_purchases(row[6], today)
                state = self._state(row, weekly)
                if weekly_changed:
                    updated = conn.execute(
                        "UPDATE training SET weekly_purchases=%s WHERE user_id=%s",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    if updated.rowcount != 1:
                        raise db_backend.IntegrityError("training state changed")
                    self._insert_operation(
                        conn,
                        f"training-state-week:{user_id}:{period_key}",
                        user_id,
                        "week",
                        period_key,
                        state,
                    )
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise


__all__ = ["TRAINING_FIELDS", "TrainingStateService", "normalize_weekly_purchases"]
