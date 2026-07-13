from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaChallengeCostResult:
    status: str
    used: int
    remaining: int
    stamina: int
    challenged_at: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaChallengeCostService:
    """Atomically reserve one arena challenge from a mutable state snapshot."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def consume(
        self, operation_id, user_id, opponent_id, challenge_cap, stamina_cost,
        expected_used, expected_extra, expected_hp, expected_mp, expected_stamina,
        expected_last_challenge_time, challenged_at,
    ) -> ArenaChallengeCostResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        opponent_id = "" if opponent_id is None else str(opponent_id)
        challenge_cap, stamina_cost, expected_used, expected_extra = map(
            int, (challenge_cap, stamina_cost, expected_used, expected_extra)
        )
        expected_hp, expected_mp, expected_stamina = map(int, (expected_hp, expected_mp, expected_stamina))
        expected_last_challenge_time = str(expected_last_challenge_time or "")
        challenged_at = str(challenged_at)
        if not operation_id or not challenged_at or min(
            challenge_cap, stamina_cost, expected_used, expected_extra,
            expected_hp, expected_mp, expected_stamina,
        ) < 0:
            raise ValueError("valid operation and non-negative challenge snapshot are required")
        payload = json.dumps([
            user_id, opponent_id, challenge_cap, stamina_cost, expected_used, expected_extra,
            expected_hp, expected_mp, expected_stamina, expected_last_challenge_time,
        ], ensure_ascii=True)

        def result(status: str, used=expected_used, stamina=expected_stamina, at=expected_last_challenge_time):
            return ArenaChallengeCostResult(status, int(used), max(0, challenge_cap - int(used)), int(stamina), str(at))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_challenge_cost_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, used INTEGER NOT NULL, "
                    "remaining INTEGER NOT NULL, stamina INTEGER NOT NULL, challenged_at TEXT NOT NULL, "
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,used,remaining,stamina,challenged_at FROM arena_challenge_cost_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return ArenaChallengeCostResult("duplicate", *(int(value) for value in previous[1:4]), str(previous[4]))

                user = conn.execute(
                    "SELECT COALESCE(hp,0),COALESCE(mp,0),COALESCE(user_stamina,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()}
                required = {"daily_challenges_used", "daily_extra_challenges", "last_challenge_time"}
                if user is None:
                    conn.rollback(); return result("user_missing")
                if not required.issubset(columns):
                    conn.rollback(); return result("state_changed")
                arena = conn.execute(
                    "SELECT COALESCE(daily_challenges_used,0),COALESCE(daily_extra_challenges,0),"
                    "COALESCE(last_challenge_time,'') FROM player_data.arena WHERE user_id=%s", (user_id,)
                ).fetchone()
                if arena is None or tuple(map(int, user)) != (expected_hp, expected_mp, expected_stamina) or (
                    int(arena[0]), int(arena[1]), str(arena[2])
                ) != (expected_used, expected_extra, expected_last_challenge_time):
                    conn.rollback(); return result("state_changed")
                if expected_used >= challenge_cap:
                    conn.rollback(); return result("limit_reached")
                if expected_stamina < stamina_cost:
                    conn.rollback(); return result("stamina_insufficient")

                used, stamina = expected_used + 1, expected_stamina - stamina_cost
                if conn.execute(
                    "UPDATE player_data.arena SET daily_challenges_used=%s,last_challenge_time=%s "
                    "WHERE user_id=%s AND COALESCE(daily_challenges_used,0)=%s AND COALESCE(daily_extra_challenges,0)=%s "
                    "AND COALESCE(last_challenge_time,'')=%s",
                    (used, challenged_at, user_id, expected_used, expected_extra, expected_last_challenge_time),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                if conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s AND COALESCE(hp,0)=%s "
                    "AND COALESCE(mp,0)=%s AND COALESCE(user_stamina,0)=%s",
                    (stamina, user_id, expected_hp, expected_mp, expected_stamina),
                ).rowcount != 1:
                    conn.rollback(); return result("state_changed")
                conn.execute(
                    "INSERT INTO arena_challenge_cost_operations "
                    "(operation_id,payload,used,remaining,stamina,challenged_at) VALUES (%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, used, challenge_cap - used, stamina, challenged_at),
                )
                conn.commit()
                return result("applied", used, stamina, challenged_at)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["ArenaChallengeCostResult", "ArenaChallengeCostService"]
