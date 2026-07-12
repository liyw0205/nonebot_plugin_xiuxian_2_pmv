from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BossRewardResult:
    status: str
    stone: int
    integral: int
    wallet_stone: int
    daily_stone: int
    daily_integral: int
    total_integral: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BossRewardService:
    """Apply wallet and world-boss counters in one cross-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def grant(
        self,
        operation_id,
        user_id,
        expected_daily_stone,
        expected_daily_integral,
        expected_total_integral,
        stone,
        integral,
    ) -> BossRewardResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_daily_stone = int(expected_daily_stone)
        expected_daily_integral = int(expected_daily_integral)
        expected_total_integral = int(expected_total_integral)
        stone = int(stone)
        integral = int(integral)
        if not operation_id or min(expected_daily_stone, expected_daily_integral, expected_total_integral, stone, integral) < 0:
            raise ValueError("valid operation, counters and rewards are required")
        payload = json.dumps(
            [user_id, expected_daily_stone, expected_daily_integral, expected_total_integral, stone, integral],
            ensure_ascii=True,
        )

        def rejected(status: str, wallet_stone: int = 0) -> BossRewardResult:
            return BossRewardResult(
                status, 0, 0, wallet_stone, expected_daily_stone, expected_daily_integral, expected_total_integral
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS boss_reward_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
                    "integral INTEGER NOT NULL, wallet_stone INTEGER NOT NULL, daily_stone INTEGER NOT NULL, "
                    "daily_integral INTEGER NOT NULL, total_integral INTEGER NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, stone, integral, wallet_stone, daily_stone, daily_integral, total_integral "
                    "FROM boss_reward_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return BossRewardResult("duplicate", *(int(value) for value in previous[1:]))

                user = conn.execute(
                    "SELECT COALESCE(stone, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return rejected("user_missing")
                for table, fields in (
                    ("boss", {"boss_stone", "boss_integral"}),
                    ("boss_limit", {"integral"}),
                ):
                    exists = conn.execute(
                        "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s", (table,)
                    ).fetchone()
                    columns = (
                        {str(column[1]) for column in conn.execute(f"PRAGMA player_data.table_info({table})").fetchall()}
                        if exists is not None
                        else set()
                    )
                    if not fields.issubset(columns):
                        conn.rollback()
                        return rejected("state_changed", int(user[0]))

                daily = conn.execute(
                    "SELECT COALESCE(boss_stone, 0), COALESCE(boss_integral, 0) "
                    "FROM player_data.boss WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                total = conn.execute(
                    "SELECT COALESCE(integral, 0) FROM player_data.boss_limit WHERE user_id=%s", (user_id,)
                ).fetchone()
                current = (
                    int(daily[0]) if daily else 0,
                    int(daily[1]) if daily else 0,
                    int(total[0]) if total else 0,
                )
                if current != (expected_daily_stone, expected_daily_integral, expected_total_integral):
                    conn.rollback()
                    return rejected("state_changed", int(user[0]))

                wallet_stone = int(user[0]) + stone
                daily_stone = expected_daily_stone + stone
                daily_integral = expected_daily_integral + integral
                total_integral = expected_total_integral + integral
                if conn.execute(
                    "UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s", (stone, user_id)
                ).rowcount != 1:
                    conn.rollback()
                    return rejected("state_changed", int(user[0]))
                if daily is None:
                    conn.execute(
                        "INSERT INTO player_data.boss (user_id, boss_stone, boss_integral) VALUES (%s, %s, %s)",
                        (user_id, daily_stone, daily_integral),
                    )
                else:
                    conn.execute(
                        "UPDATE player_data.boss SET boss_stone=%s, boss_integral=%s WHERE user_id=%s",
                        (daily_stone, daily_integral, user_id),
                    )
                if total is None:
                    conn.execute(
                        "INSERT INTO player_data.boss_limit (user_id, integral) VALUES (%s, %s)",
                        (user_id, total_integral),
                    )
                else:
                    conn.execute(
                        "UPDATE player_data.boss_limit SET integral=%s WHERE user_id=%s", (total_integral, user_id)
                    )
                conn.execute(
                    "INSERT INTO boss_reward_operations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                    (operation_id, payload, stone, integral, wallet_stone, daily_stone, daily_integral, total_integral),
                )
                conn.commit()
                return BossRewardResult(
                    "applied", stone, integral, wallet_stone, daily_stone, daily_integral, total_integral
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["BossRewardResult", "BossRewardService"]
