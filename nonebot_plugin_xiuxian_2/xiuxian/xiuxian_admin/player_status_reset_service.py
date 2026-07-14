from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class AdminPlayerStatusResetResult:
    status: str
    previous_state: tuple[int, int, int, int, int] = ()
    final_state: tuple[int, int, int, int, int] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"reset", "duplicate"}


class AdminPlayerStatusResetService:
    """Atomically recompute one player's combat state and stamina."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_player_status_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "previous_state TEXT NOT NULL,final_state TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _state(row) -> tuple[int, int, int, int, int]:
        return tuple(int(value or 0) for value in row)

    def snapshot(self, user_id) -> tuple[int, int, int, int, int] | None:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            row = conn.execute(
                "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                "WHERE user_id=%s",
                (user_id,),
            ).fetchone()
            return self._state(row) if row else None

    def reset(
        self,
        operation_id,
        operator_id,
        user_id,
        expected_state,
        max_stamina,
        *,
        target_name="",
    ) -> AdminPlayerStatusResetResult:
        operation_id = str(operation_id).strip()
        operator_id = str(operator_id).strip()
        user_id = str(user_id).strip()
        max_stamina = int(max_stamina)
        target_name = str(target_name)
        if not operation_id or not operator_id or not user_id:
            raise ValueError("operation, operator and user are required")
        if max_stamina <= 0:
            raise ValueError("stamina limit must be positive")
        if expected_state is None:
            normalized_expected = None
        else:
            normalized_expected = tuple(int(value or 0) for value in expected_state)
            if len(normalized_expected) != 5:
                raise ValueError("complete status snapshot is required")

        payload = json.dumps(
            [operator_id, user_id, max_stamina],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,previous_state,final_state "
                    "FROM admin_player_status_reset_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return AdminPlayerStatusResetResult("operation_conflict")
                    return AdminPlayerStatusResetResult(
                        "duplicate",
                        tuple(json.loads(str(previous[1]))),
                        tuple(json.loads(str(previous[2]))),
                    )

                row = conn.execute(
                    "SELECT exp,hp,mp,atk,user_stamina FROM user_xiuxian "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return AdminPlayerStatusResetResult("user_missing")
                actual_state = self._state(row)
                if actual_state != normalized_expected:
                    conn.rollback()
                    return AdminPlayerStatusResetResult(
                        "state_changed", actual_state, actual_state
                    )

                exp = actual_state[0]
                final_state = (
                    exp,
                    exp // 2,
                    exp,
                    exp // 10,
                    max_stamina,
                )
                changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,atk=%s,user_stamina=%s "
                    "WHERE user_id=%s AND COALESCE(exp,0)=%s AND COALESCE(hp,0)=%s "
                    "AND COALESCE(mp,0)=%s AND COALESCE(atk,0)=%s "
                    "AND COALESCE(user_stamina,0)=%s",
                    (
                        final_state[1],
                        final_state[2],
                        final_state[3],
                        final_state[4],
                        user_id,
                        *actual_state,
                    ),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return AdminPlayerStatusResetResult("state_changed")

                conn.execute(
                    "INSERT INTO admin_player_status_reset_operations("
                    "operation_id,payload,previous_state,final_state) "
                    "VALUES(%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        json.dumps(actual_state, separators=(",", ":")),
                        json.dumps(final_state, separators=(",", ":")),
                    ),
                )
                conn.commit()
                return AdminPlayerStatusResetResult(
                    "reset", actual_state, final_state
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["AdminPlayerStatusResetResult", "AdminPlayerStatusResetService"]
