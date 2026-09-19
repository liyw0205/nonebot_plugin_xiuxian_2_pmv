from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


ARENA_FIELDS = (
    "score", "total_wins", "total_losses", "daily_challenges_used",
    "daily_extra_challenges", "daily_challenge_buys", "last_reset_date",
    "last_buy_date", "last_challenge_time", "win_streak", "max_win_streak",
    "rank", "honor_points", "total_honor_earned", "weekly_purchases",
)


class ArenaStateRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _default(today: date) -> dict[str, Any]:
        return {
            "score": 1000, "total_wins": 0, "total_losses": 0,
            "daily_challenges_used": 0, "daily_extra_challenges": 0,
            "daily_challenge_buys": 0, "last_reset_date": today.isoformat(),
            "last_buy_date": today.isoformat(), "last_challenge_time": "",
            "win_streak": 0, "max_win_streak": 0, "rank": "青铜",
            "honor_points": 0, "total_honor_earned": 0,
            "weekly_purchases": {"_last_reset": today.isoformat()},
        }

    @staticmethod
    def _integer(value: Any, default: int) -> tuple[int, bool]:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return default, True
        return normalized, value is None

    @staticmethod
    def _business_date(value: Any, today: date) -> tuple[str, bool]:
        try:
            normalized = date.fromisoformat(str(value)).isoformat()
        except (TypeError, ValueError):
            return today.isoformat(), True
        return normalized, False

    @staticmethod
    def _weekly(value: Any, today: date) -> tuple[dict[str, int | str], bool]:
        changed = False
        if isinstance(value, str):
            try:
                value = json.loads(value) if value else {}
            except (TypeError, ValueError):
                value, changed = {}, True
        if not isinstance(value, dict):
            value, changed = {}, True
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

    @classmethod
    def _normalize(cls, row: dict[str, Any], today: date, weekly: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        defaults = cls._default(today)
        state: dict[str, Any] = {}
        repaired = False
        for field in ARENA_FIELDS[:-1]:
            value = row[field]
            if field in {
                "score", "total_wins", "total_losses", "daily_challenges_used",
                "daily_extra_challenges", "daily_challenge_buys", "win_streak",
                "max_win_streak", "honor_points", "total_honor_earned",
            }:
                state[field], changed = cls._integer(value, int(defaults[field]))
            elif field in {"last_reset_date", "last_buy_date"}:
                state[field], changed = cls._business_date(value, today)
            elif field == "rank":
                state[field], changed = (str(value), False) if value else ("青铜", True)
            else:
                state[field], changed = str(value or ""), value is None
            repaired = repaired or changed
        state["weekly_purchases"] = weekly
        return state, repaired

    def initialize(self, user_id: str, today: date) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            row = uow.query_one(
                f"SELECT {','.join(ARENA_FIELDS)} FROM arena WHERE user_id=?", (user_id,)
            )
            if row is not None:
                raw = dict(row)
                day_key = today.isoformat()
                weekly, weekly_changed = self._weekly(raw["weekly_purchases"], today)
                result, repaired = self._normalize(raw, today, weekly)
                period = today.isocalendar()
                if repaired:
                    assignments = ",".join(f'"{field}"=?' for field in ARENA_FIELDS[:-1])
                    values = tuple(result[field] for field in ARENA_FIELDS[:-1])
                    uow.execute(
                        f"UPDATE arena SET {assignments} WHERE user_id=?",
                        (*values, user_id),
                    )
                    uow.execute(
                        "INSERT INTO arena_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                        (
                            f"arena-state-normalize:{user_id}", user_id, "normalize", day_key,
                            json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                        ),
                    )
                if weekly_changed:
                    period_key = f"{period.year}-W{period.week:02d}"
                    result["weekly_purchases"] = weekly
                    uow.execute(
                        "UPDATE arena SET weekly_purchases=? WHERE user_id=?",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    uow.execute(
                        "INSERT INTO arena_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                        (
                            f"arena-state-week:{user_id}:{period_key}", user_id, "week", period_key,
                            json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                        ),
                    )
                else:
                    result["weekly_purchases"] = weekly
                if str(result["last_buy_date"] or "") != day_key:
                    result["daily_challenge_buys"] = 0
                    result["daily_extra_challenges"] = 0
                    result["last_buy_date"] = day_key
                    uow.execute(
                        "UPDATE arena SET daily_challenge_buys=0,daily_extra_challenges=0,"
                        "last_buy_date=? WHERE user_id=?",
                        (day_key, user_id),
                    )
                    uow.execute(
                        "INSERT INTO arena_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                        (
                            f"arena-state-buy-day:{user_id}:{day_key}", user_id, "day", day_key,
                            json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                        ),
                    )
                return result
            state = self._default(today)
            values = tuple(
                json.dumps(state[field], ensure_ascii=True, sort_keys=True)
                if field == "weekly_purchases" else state[field]
                for field in ARENA_FIELDS
            )
            placeholders = ",".join("?" for _ in ARENA_FIELDS)
            uow.execute(
                f"INSERT INTO arena(user_id,{','.join(ARENA_FIELDS)}) VALUES(?,{placeholders})",
                (user_id, *values),
            )
            uow.execute(
                "INSERT INTO arena_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                (
                    f"arena-state-init:{user_id}", user_id, "initialize", today.isoformat(),
                    json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                ),
            )
            return state

    def ranking(self, limit: int) -> tuple[tuple[str, int], ...]:
        with DatabaseUnitOfWork(self.player_database) as uow:
            rows = uow.query_all(
                "SELECT user_id,CAST(COALESCE(score,0) AS INTEGER) AS score "
                "FROM arena ORDER BY CAST(COALESCE(score,0) AS INTEGER) DESC LIMIT ?",
                (max(0, int(limit)),),
            )
        return tuple((str(row["user_id"]), int(row["score"])) for row in rows)


__all__ = ["ARENA_FIELDS", "ArenaStateRepository"]
