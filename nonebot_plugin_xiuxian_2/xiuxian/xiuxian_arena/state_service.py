from __future__ import annotations

import json
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


ARENA_FIELDS = (
    "score",
    "total_wins",
    "total_losses",
    "daily_challenges_used",
    "daily_extra_challenges",
    "daily_challenge_buys",
    "last_reset_date",
    "last_buy_date",
    "last_challenge_time",
    "win_streak",
    "max_win_streak",
    "rank",
    "honor_points",
    "total_honor_earned",
    "weekly_purchases",
)

_INTEGER_DEFAULTS = {
    "score": 1000,
    "total_wins": 0,
    "total_losses": 0,
    "daily_challenges_used": 0,
    "daily_extra_challenges": 0,
    "daily_challenge_buys": 0,
    "win_streak": 0,
    "max_win_streak": 0,
    "honor_points": 0,
    "total_honor_earned": 0,
}


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
    return _normalize_weekly_purchases(value, today)[0]


def normalize_daily_purchase_state(bought, extra, last_buy_date, today=None) -> tuple[int, int, str]:
    today = _as_date(today)
    try:
        last_buy = date.fromisoformat(str(last_buy_date))
    except (TypeError, ValueError):
        last_buy = None
    if last_buy != today:
        return 0, 0, today.isoformat()
    try:
        bought = max(0, int(bought or 0))
    except (TypeError, ValueError):
        bought = 0
    try:
        extra = max(0, int(extra or 0))
    except (TypeError, ValueError):
        extra = 0
    return bought, extra, last_buy.isoformat()


class ArenaStateService:
    """Initialize and advance arena read state in one player-database transaction."""

    _COLUMN_DEFINITIONS = {
        "score": "TEXT DEFAULT '1000'",
        "total_wins": "TEXT DEFAULT '0'",
        "total_losses": "TEXT DEFAULT '0'",
        "daily_challenges_used": "TEXT DEFAULT '0'",
        "daily_extra_challenges": "TEXT DEFAULT '0'",
        "daily_challenge_buys": "TEXT DEFAULT '0'",
        "last_reset_date": "TEXT DEFAULT ''",
        "last_buy_date": "TEXT DEFAULT ''",
        "last_challenge_time": "TEXT DEFAULT ''",
        "win_streak": "TEXT DEFAULT '0'",
        "max_win_streak": "TEXT DEFAULT '0'",
        "rank": "TEXT DEFAULT '青铜'",
        "honor_points": "TEXT DEFAULT '0'",
        "total_honor_earned": "TEXT DEFAULT '0'",
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
    def _integer(value, default) -> tuple[int, bool]:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return int(default), True
        return normalized, value is None

    @staticmethod
    def _business_date(value, today) -> tuple[str, bool]:
        try:
            normalized = date.fromisoformat(str(value)).isoformat()
        except (TypeError, ValueError):
            return today.isoformat(), True
        return normalized, False

    @classmethod
    def _normalize_state(cls, row, today, weekly) -> tuple[dict, bool]:
        state = {}
        repair = False
        for index, field in enumerate(ARENA_FIELDS[:-1]):
            value = row[index]
            if field in _INTEGER_DEFAULTS:
                state[field], changed = cls._integer(value, _INTEGER_DEFAULTS[field])
            elif field in {"last_reset_date", "last_buy_date"}:
                state[field], changed = cls._business_date(value, today)
            elif field == "rank":
                state[field], changed = (str(value), False) if value else ("青铜", True)
            else:
                state[field], changed = (str(value or ""), value is None)
            repair = repair or changed
        state["weekly_purchases"] = weekly
        return state, repair

    @staticmethod
    def _snapshot(state) -> str:
        return json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena ("
            "user_id TEXT PRIMARY KEY,score TEXT DEFAULT '1000',total_wins TEXT DEFAULT '0',"
            "total_losses TEXT DEFAULT '0',daily_challenges_used TEXT DEFAULT '0',"
            "daily_extra_challenges TEXT DEFAULT '0',daily_challenge_buys TEXT DEFAULT '0',"
            "last_reset_date TEXT DEFAULT '',last_buy_date TEXT DEFAULT '',"
            "last_challenge_time TEXT DEFAULT '',win_streak TEXT DEFAULT '0',"
            "max_win_streak TEXT DEFAULT '0',rank TEXT DEFAULT '青铜',honor_points TEXT DEFAULT '0',"
            "total_honor_earned TEXT DEFAULT '0',weekly_purchases TEXT DEFAULT NULL)"
        )
        columns = {str(column[1]) for column in conn.execute("PRAGMA table_info(arena)").fetchall()}
        for field, definition in cls._COLUMN_DEFINITIONS.items():
            if field not in columns:
                conn.execute(f'ALTER TABLE arena ADD COLUMN "{field}" {definition}')
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_state_operations ("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
            "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _insert_operation(conn, operation_id, user_id, kind, period_key, state) -> None:
        conn.execute(
            "INSERT INTO arena_state_operations("
            "operation_id,user_id,kind,period_key,snapshot,created_at) "
            "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
            (operation_id, user_id, kind, period_key, ArenaStateService._snapshot(state)),
        )

    @staticmethod
    def _default_state(today) -> dict:
        state = dict(_INTEGER_DEFAULTS)
        state.update(
            {
                "last_reset_date": today.isoformat(),
                "last_buy_date": today.isoformat(),
                "last_challenge_time": "",
                "rank": "青铜",
                "weekly_purchases": {"_last_reset": today.isoformat()},
            }
        )
        return {field: state[field] for field in ARENA_FIELDS}

    @staticmethod
    def _state_values(state) -> tuple:
        return tuple(
            json.dumps(state[field], ensure_ascii=True, sort_keys=True)
            if field == "weekly_purchases"
            else state[field]
            for field in ARENA_FIELDS
        )

    def get(self, user_id, today=None) -> dict:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        today = _as_date(today)
        day_key = today.isoformat()
        period_key = self._period_key(today)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    f"SELECT {','.join(ARENA_FIELDS)} FROM arena WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    state = self._default_state(today)
                    conn.execute(
                        f"INSERT INTO arena(user_id,{','.join(ARENA_FIELDS)}) "
                        f"VALUES(%s,{','.join(['%s'] * len(ARENA_FIELDS))})",
                        (user_id, *self._state_values(state)),
                    )
                    self._insert_operation(
                        conn, f"arena-state-init:{user_id}", user_id,
                        "initialize", day_key, state,
                    )
                    conn.commit()
                    return state

                weekly, weekly_changed = _normalize_weekly_purchases(row[-1], today)
                state, repair = self._normalize_state(row, today, weekly)
                daily_changed = state["last_buy_date"] != day_key
                if daily_changed:
                    state["daily_challenge_buys"] = 0
                    state["daily_extra_challenges"] = 0
                    state["last_buy_date"] = day_key

                if repair:
                    assignments = ",".join(f'"{field}"=%s' for field in ARENA_FIELDS[:-1])
                    conn.execute(
                        f"UPDATE arena SET {assignments} WHERE user_id=%s",
                        (*self._state_values(state)[:-1], user_id),
                    )
                if daily_changed:
                    conn.execute(
                        "UPDATE arena SET daily_challenge_buys=0,daily_extra_challenges=0,"
                        "last_buy_date=%s WHERE user_id=%s",
                        (day_key, user_id),
                    )
                if weekly_changed:
                    conn.execute(
                        "UPDATE arena SET weekly_purchases=%s WHERE user_id=%s",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )

                if repair:
                    self._insert_operation(
                        conn, f"arena-state-normalize:{user_id}", user_id,
                        "normalize", day_key, state,
                    )
                if daily_changed:
                    self._insert_operation(
                        conn, f"arena-state-buy-day:{user_id}:{day_key}", user_id,
                        "day", day_key, state,
                    )
                if weekly_changed:
                    self._insert_operation(
                        conn, f"arena-state-week:{user_id}:{period_key}", user_id,
                        "week", period_key, state,
                    )
                conn.commit()
                return state
            except Exception:
                conn.rollback()
                raise


__all__ = [
    "ARENA_FIELDS",
    "ArenaStateService",
    "normalize_daily_purchase_state",
    "normalize_weekly_purchases",
]
