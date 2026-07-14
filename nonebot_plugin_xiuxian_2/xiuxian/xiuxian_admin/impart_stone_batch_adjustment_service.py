from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .impart_stone_adjustment_service import AdminImpartStoneAdjustmentService


@dataclass(frozen=True)
class AdminImpartStoneBatchAdjustmentResult:
    status: str
    total: int = 0
    completed: int = 0
    applied_delta: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminImpartStoneBatchAdjustmentService:
    """Persist and resume full-server impart stone adjustment plans."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        adjustment_service: AdminImpartStoneAdjustmentService | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._adjustment_service = adjustment_service or AdminImpartStoneAdjustmentService(
            game_database, impart_database
        )
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "applied_delta INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _request(operator_id, requested_delta) -> dict:
        return {
            "operator_id": str(operator_id),
            "requested_delta": int(requested_delta),
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
    ) -> AdminImpartStoneBatchAdjustmentResult:
        operation = conn.execute(
            "SELECT total FROM admin_impart_stone_batch_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        counts = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(applied_delta),0),"
            "COALESCE(SUM(CASE WHEN applied_delta!=0 THEN 1 ELSE 0 END),0) "
            "FROM admin_impart_stone_batch_progress WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        completed = int(counts[0])
        affected_users = int(counts[2])
        return AdminImpartStoneBatchAdjustmentResult(
            status,
            int(operation[0]),
            completed,
            int(counts[1]),
            affected_users,
            completed - affected_users,
        )

    def find_running(self, operator_id, requested_delta) -> str | None:
        request = self._request(operator_id, requested_delta)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("admin_impart_stone_batch_operations"):
                return None
            rows = conn.execute(
                "SELECT operation_id,payload FROM admin_impart_stone_batch_operations "
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
    ) -> tuple[AdminImpartStoneBatchAdjustmentResult | None, tuple[str, ...]]:
        payload = self._payload(request, user_ids)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,status FROM admin_impart_stone_batch_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    conn.execute(
                        "INSERT INTO admin_impart_stone_batch_operations("
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
                        "SELECT user_id FROM admin_impart_stone_batch_progress "
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
                "previous_stone": result.previous_stone,
                "final_stone": result.final_stone,
                "applied_delta": result.applied_delta,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute(
                    "INSERT INTO admin_impart_stone_batch_progress("
                    "operation_id,user_id,status,applied_delta,result_json) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(operation_id,user_id) "
                    "DO UPDATE SET status=EXCLUDED.status,"
                    "applied_delta=EXCLUDED.applied_delta,"
                    "result_json=EXCLUDED.result_json "
                    "WHERE ABS(EXCLUDED.applied_delta)>"
                    "ABS(admin_impart_stone_batch_progress.applied_delta)",
                    (
                        operation_id,
                        user_id,
                        result.status,
                        result.applied_delta,
                        result_json,
                    ),
                )
                counts = conn.execute(
                    "SELECT o.total,COUNT(p.user_id) "
                    "FROM admin_impart_stone_batch_operations o "
                    "LEFT JOIN admin_impart_stone_batch_progress p "
                    "ON p.operation_id=o.operation_id WHERE o.operation_id=%s "
                    "GROUP BY o.total",
                    (operation_id,),
                ).fetchone()
                status = "completed" if int(counts[1]) >= int(counts[0]) else "running"
                conn.execute(
                    "UPDATE admin_impart_stone_batch_operations "
                    "SET status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (status, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def adjust(
        self,
        operation_id,
        operator_id,
        user_ids,
        requested_delta,
        *,
        chunk_size=100,
    ) -> AdminImpartStoneBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        request = self._request(operator_id, requested_delta)
        normalized_users = tuple(
            sorted({str(user_id).strip() for user_id in user_ids if str(user_id).strip()})
        )
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"].strip()
            or not normalized_users
            or request["requested_delta"] == 0
        ):
            raise ValueError("invalid impart stone batch arguments")

        previous, pending = self._begin(
            operation_id, request, normalized_users, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                expected_stone = self._adjustment_service.snapshot(user_id)
                result = self._adjustment_service.adjust(
                    f"admin-impart-stone-batch:{operation_id}:{user_id}",
                    request["operator_id"],
                    user_id,
                    expected_stone,
                    request["requested_delta"],
                    target_name="all",
                )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(
                    f"impart stone batch child operation conflict: {user_id}"
                )
            self._record(operation_id, user_id, result)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            return self._result(conn, operation_id, "applied")


__all__ = [
    "AdminImpartStoneBatchAdjustmentResult",
    "AdminImpartStoneBatchAdjustmentService",
]
