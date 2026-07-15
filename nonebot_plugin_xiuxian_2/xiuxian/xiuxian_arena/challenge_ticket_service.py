from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaChallengeTicketResult:
    status: str
    used_tickets: int
    item_remaining: int
    challenges_used: int
    challenges_remaining: int
    challenge_cap: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaChallengeTicketService:
    """Consume challenge tickets and restore arena attempts in one transaction."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def use(
        self,
        operation_id,
        user_id,
        item_id,
        requested_count,
        expected_item_count,
        expected_challenges_used,
        expected_extra_challenges,
        challenge_cap,
    ) -> ArenaChallengeTicketResult:
        operation_id = str(operation_id).strip()
        user_id, item_id = str(user_id), int(item_id)
        requested_count, expected_item_count = map(int, (requested_count, expected_item_count))
        expected_challenges_used, expected_extra_challenges, challenge_cap = map(
            int, (expected_challenges_used, expected_extra_challenges, challenge_cap)
        )
        if not operation_id or requested_count <= 0 or min(
            expected_item_count,
            expected_challenges_used,
            expected_extra_challenges,
            challenge_cap,
        ) < 0:
            raise ValueError("valid operation and non-negative arena snapshot are required")
        # Request identity only — inventory/used counters are concurrency checks.
        payload = json.dumps(
            [
                user_id,
                item_id,
                requested_count,
                challenge_cap,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        def result(status: str) -> ArenaChallengeTicketResult:
            return ArenaChallengeTicketResult(
                status,
                0,
                expected_item_count,
                expected_challenges_used,
                max(0, challenge_cap - expected_challenges_used),
                challenge_cap,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS arena_challenge_ticket_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tickets INTEGER NOT NULL,"
                    "item_remaining INTEGER NOT NULL,challenges_used INTEGER NOT NULL,"
                    "challenges_remaining INTEGER NOT NULL,challenge_cap INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,used_tickets,item_remaining,challenges_used,challenges_remaining,challenge_cap "
                    "FROM arena_challenge_ticket_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("operation_conflict")
                    return ArenaChallengeTicketResult(
                        "duplicate", *(int(value) for value in previous[1:])
                    )

                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(arena)").fetchall()
                }
                if not {"daily_challenges_used", "daily_extra_challenges"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                arena = conn.execute(
                    "SELECT COALESCE(daily_challenges_used,0),COALESCE(daily_extra_challenges,0) "
                    "FROM player_data.arena WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if arena is None or tuple(map(int, arena)) != (
                    expected_challenges_used,
                    expected_extra_challenges,
                ):
                    conn.rollback()
                    return result("state_changed")
                if item is None or int(item[0]) <= 0:
                    conn.rollback()
                    return result("item_missing")
                if int(item[0]) != expected_item_count:
                    conn.rollback()
                    return result("state_changed")
                if expected_challenges_used <= 0:
                    conn.rollback()
                    return result("no_challenges_used")

                used_tickets = min(
                    requested_count, expected_item_count, expected_challenges_used
                )
                item_remaining = expected_item_count - used_tickets
                challenges_used = expected_challenges_used - used_tickets
                challenges_remaining = max(0, challenge_cap - challenges_used)
                bound = int(item[1])
                bind_remaining = min(
                    bound - used_tickets if bound >= used_tickets else bound,
                    item_remaining,
                )
                updated = conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)=%s",
                    (item_remaining, bind_remaining, user_id, item_id, expected_item_count),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                updated = conn.execute(
                    "UPDATE player_data.arena SET daily_challenges_used=%s WHERE user_id=%s "
                    "AND COALESCE(daily_challenges_used,0)=%s "
                    "AND COALESCE(daily_extra_challenges,0)=%s",
                    (
                        challenges_used,
                        user_id,
                        expected_challenges_used,
                        expected_extra_challenges,
                    ),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed")
                conn.execute(
                    "INSERT INTO arena_challenge_ticket_operations("
                    "operation_id,payload,used_tickets,item_remaining,challenges_used,"
                    "challenges_remaining,challenge_cap) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        used_tickets,
                        item_remaining,
                        challenges_used,
                        challenges_remaining,
                        challenge_cap,
                    ),
                )
                conn.commit()
                return ArenaChallengeTicketResult(
                    "applied",
                    used_tickets,
                    item_remaining,
                    challenges_used,
                    challenges_remaining,
                    challenge_cap,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["ArenaChallengeTicketResult", "ArenaChallengeTicketService"]
