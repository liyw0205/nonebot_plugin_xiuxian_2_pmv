from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .player_status_reset_service import AdminPlayerStatusResetService


@dataclass(frozen=True)
class AdminPlayerStatusBatchResetResult:
    status: str
    total: int = 0
    completed: int = 0
    reset_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminPlayerStatusBatchResetService:
    """Persist and resume full-server player status reset plans."""

    def __init__(
        self,
        database: str | Path,
        reset_service: AdminPlayerStatusResetService | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._database = Path(database)
        self._reset_service = reset_service or AdminPlayerStatusResetService(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_player_status_batch_reset_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "reset_applied INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _request(operator_id, max_stamina) -> dict:
        return {
            "operator_id": str(operator_id),
            "max_stamina": int(max_stamina),
        }

    @staticmethod
    def _payload(request: dict, user_ids: tuple[str, ...]) -> str:
        return json.dumps(
            {"request": request, "users": list(user_ids)},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(
        conn, operation_id: str, status: str
    ) -> AdminPlayerStatusBatchResetResult:
        operation = conn.execute(
            "SELECT total FROM admin_player_status_batch_reset_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        counts = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(reset_applied),0) "
            "FROM admin_player_status_batch_reset_progress WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        completed = int(counts[0])
        reset_users = int(counts[1])
        return AdminPlayerStatusBatchResetResult(
            status,
            int(operation[0]),
            completed,
            reset_users,
            completed - reset_users,
        )

    def find_running(self, operator_id, max_stamina) -> str | None:
        request = self._request(operator_id, max_stamina)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            if not conn.table_exists("admin_player_status_batch_reset_operations"):
                return None
            rows = conn.execute(
                "SELECT operation_id,payload "
                "FROM admin_player_status_batch_reset_operations "
                "WHERE status='running' ORDER BY created_at DESC,rowid DESC"
            ).fetchall()
            for row in rows:
                try:
                    if json.loads(str(row[1])).get("request") == request:
                        return str(row[0])
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
        return None

    def _begin(
        self,
        operation_id: str,
        request: dict,
        user_ids: tuple[str, ...],
        chunk_size: int,
    ) -> tuple[AdminPlayerStatusBatchResetResult | None, tuple[str, ...]]:
        payload = self._payload(request, user_ids)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,status "
                    "FROM admin_player_status_batch_reset_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    if not user_ids:
                        conn.rollback()
                        raise ValueError("new player status reset plan requires users")
                    conn.execute(
                        "INSERT INTO admin_player_status_batch_reset_operations("
                        "operation_id,payload,total) VALUES(%s,%s,%s)",
                        (operation_id, payload, len(user_ids)),
                    )
                    frozen_users = user_ids
                else:
                    previous_payload = json.loads(str(previous[0]))
                    if previous_payload.get("request") != request:
                        result = self._result(conn, operation_id, "operation_conflict")
                        conn.rollback()
                        return result, ()
                    frozen_users = tuple(
                        str(user_id) for user_id in previous_payload.get("users", [])
                    )
                    if str(previous[1]) == "completed":
                        result = self._result(conn, operation_id, "duplicate")
                        conn.rollback()
                        return result, ()

                completed_users = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT user_id FROM admin_player_status_batch_reset_progress "
                        "WHERE operation_id=%s",
                        (operation_id,),
                    ).fetchall()
                }
                pending = tuple(
                    user_id
                    for user_id in frozen_users
                    if user_id not in completed_users
                )[:chunk_size]
                conn.commit()
                return None, pending
            except Exception:
                conn.rollback()
                raise

    def _record(self, operation_id: str, user_id: str, result) -> None:
        result_json = json.dumps(
            {
                "status": result.status,
                "previous_state": result.previous_state,
                "final_state": result.final_state,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute(
                    "INSERT INTO admin_player_status_batch_reset_progress("
                    "operation_id,user_id,status,reset_applied,result_json) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(operation_id,user_id) "
                    "DO NOTHING",
                    (
                        operation_id,
                        user_id,
                        result.status,
                        int(result.succeeded),
                        result_json,
                    ),
                )
                counts = conn.execute(
                    "SELECT o.total,COUNT(p.user_id) "
                    "FROM admin_player_status_batch_reset_operations o "
                    "LEFT JOIN admin_player_status_batch_reset_progress p "
                    "ON p.operation_id=o.operation_id WHERE o.operation_id=%s "
                    "GROUP BY o.total",
                    (operation_id,),
                ).fetchone()
                status = "completed" if int(counts[1]) >= int(counts[0]) else "running"
                conn.execute(
                    "UPDATE admin_player_status_batch_reset_operations "
                    "SET status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (status, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def reset(
        self,
        operation_id,
        operator_id,
        user_ids,
        max_stamina,
        *,
        chunk_size=100,
    ) -> AdminPlayerStatusBatchResetResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, max_stamina)
        normalized_users = tuple(
            sorted({str(user_id).strip() for user_id in user_ids if str(user_id).strip()})
        )
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"].strip()
            or request["max_stamina"] <= 0
        ):
            raise ValueError("invalid player status batch reset arguments")

        previous, pending = self._begin(
            operation_id, request, normalized_users, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                expected_state = self._reset_service.snapshot(user_id)
                result = self._reset_service.reset(
                    f"admin-player-status-reset-batch:{operation_id}:{user_id}",
                    request["operator_id"],
                    user_id,
                    expected_state,
                    request["max_stamina"],
                    target_name="all",
                )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(
                    f"player status batch child operation conflict: {user_id}"
                )
            if result.status == "state_changed":
                raise RuntimeError(
                    f"player status remained unstable during batch reset: {user_id}"
                )
            self._record(operation_id, user_id, result)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            return self._result(conn, operation_id, "applied")


__all__ = [
    "AdminPlayerStatusBatchResetResult",
    "AdminPlayerStatusBatchResetService",
]
