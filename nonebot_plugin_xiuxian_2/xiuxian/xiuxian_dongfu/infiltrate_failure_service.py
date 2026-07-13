from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class InfiltrateFailureResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class InfiltrateFailureService:
    """Atomically settle a detected and failed dongfu infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def settle(
        self, operation_id, visitor_id, target_id, day, mode_field,
        mode_limit, target_limit, loss, consume_guard,
    ) -> InfiltrateFailureResult:
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(str, (visitor_id, target_id, day, mode_field))
        mode_limit, target_limit, loss = map(int, (mode_limit, target_limit, loss))
        consume_guard = int(bool(consume_guard))
        if (
            not operation_id
            or visitor_id == target_id
            or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}
            or mode_limit < 1
            or target_limit < 1
            or loss < 0
        ):
            raise ValueError("valid infiltration failure operation is required")
        payload = "|".join(map(str, (visitor_id, target_id, day, mode_field, mode_limit, target_limit, loss, consume_guard)))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS dongfu_infiltrate_failure_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "infiltrate_left INTEGER NOT NULL,intrude_left INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,infiltrate_left,intrude_left "
                    "FROM dongfu_infiltrate_failure_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return InfiltrateFailureResult("state_changed")
                    return InfiltrateFailureResult("duplicate", int(old[1]), int(old[2]))

                user = conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (visitor_id,)).fetchone()
                visitor = conn.execute(
                    f'SELECT built,infiltrate_date,{mode_field} '
                    'FROM player_data."dongfu_status" WHERE user_id=%s',
                    (visitor_id,),
                ).fetchone()
                target = conn.execute(
                    'SELECT built,intrude_date,intrude_count,patrol_guard '
                    'FROM player_data."dongfu_status" WHERE user_id=%s',
                    (target_id,),
                ).fetchone()
                if (
                    user is None or visitor is None or target is None
                    or int(visitor[0] or 0) != 1 or int(target[0] or 0) != 1
                ):
                    conn.rollback()
                    return InfiltrateFailureResult("state_changed")

                mode_count = int(visitor[2] or 0) if str(visitor[1] or "") == day else 0
                intrude_count = int(target[2] or 0) if str(target[1] or "") == day else 0
                if mode_count >= mode_limit or intrude_count >= target_limit:
                    conn.rollback()
                    return InfiltrateFailureResult(
                        "daily_limit", max(0, mode_limit - mode_count), max(0, target_limit - intrude_count)
                    )

                mode_count += 1
                intrude_count += 1
                visitor_update = conn.execute(
                    f'UPDATE player_data."dongfu_status" SET infiltrate_date=%s,{mode_field}=%s WHERE user_id=%s',
                    (day, mode_count, visitor_id),
                )
                target_update = conn.execute(
                    'UPDATE player_data."dongfu_status" '
                    'SET intrude_date=%s,intrude_count=%s,patrol_guard=MAX(patrol_guard-%s,0) '
                    'WHERE user_id=%s',
                    (day, intrude_count, consume_guard, target_id),
                )
                stone_update = conn.execute(
                    "UPDATE user_xiuxian SET stone=MAX(stone-%s,0) WHERE user_id=%s",
                    (loss, visitor_id),
                )
                if visitor_update.rowcount != 1 or target_update.rowcount != 1 or stone_update.rowcount != 1:
                    conn.rollback()
                    return InfiltrateFailureResult("state_changed")

                left = max(0, mode_limit - mode_count), max(0, target_limit - intrude_count)
                conn.execute(
                    "INSERT INTO dongfu_infiltrate_failure_operations "
                    "(operation_id,payload,infiltrate_left,intrude_left) VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, *left),
                )
                conn.commit()
                return InfiltrateFailureResult("settled", *left)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["InfiltrateFailureResult", "InfiltrateFailureService"]
