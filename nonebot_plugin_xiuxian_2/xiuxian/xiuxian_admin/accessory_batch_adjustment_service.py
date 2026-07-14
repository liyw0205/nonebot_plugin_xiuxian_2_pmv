from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend
from .accessory_adjustment_service import AdminAccessoryAdjustmentService


@dataclass(frozen=True)
class AdminAccessoryBatchAdjustmentResult:
    status: str
    action: str
    total: int = 0
    completed: int = 0
    affected_quantity: int = 0
    affected_users: int = 0
    skipped_users: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class AdminAccessoryBatchAdjustmentService:
    """Persist and resume full-server accessory adjustment plans."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        adjustment_service: AdminAccessoryAdjustmentService | None = None,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._adjustment_service = adjustment_service or AdminAccessoryAdjustmentService(
            game_database, player_database
        )
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_accessory_batch_operations("
            "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
            "total INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'running',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS admin_accessory_batch_progress("
            "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "affected_quantity INTEGER NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY(operation_id,user_id))"
        )

    @staticmethod
    def _request(
        action,
        operator_id,
        item_id,
        item_name,
        quality,
        quantity,
        max_accessories,
    ) -> dict:
        return {
            "action": str(action),
            "operator_id": str(operator_id),
            "item_id": int(item_id),
            "item_name": str(item_name),
            "quality": int(quality),
            "quantity": int(quantity),
            "max_accessories": int(max_accessories),
        }

    @staticmethod
    def _payload(request: dict, user_ids: tuple[str, ...]) -> str:
        return json.dumps(
            {"request": request, "users": list(user_ids)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _result(
        conn, operation_id: str, status: str
    ) -> AdminAccessoryBatchAdjustmentResult:
        operation = conn.execute(
            "SELECT action,total FROM admin_accessory_batch_operations "
            "WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        counts = conn.execute(
            "SELECT COUNT(*),COALESCE(SUM(affected_quantity),0),"
            "COALESCE(SUM(CASE WHEN affected_quantity>0 THEN 1 ELSE 0 END),0) "
            "FROM admin_accessory_batch_progress WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()
        completed = int(counts[0])
        affected_users = int(counts[2])
        return AdminAccessoryBatchAdjustmentResult(
            status,
            str(operation[0]),
            int(operation[1]),
            completed,
            int(counts[1]),
            affected_users,
            completed - affected_users,
        )

    def find_running(
        self,
        action,
        operator_id,
        item_id,
        item_name,
        quality,
        quantity,
        max_accessories,
    ) -> str | None:
        request = self._request(
            action,
            operator_id,
            item_id,
            item_name,
            quality,
            quantity,
            max_accessories,
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("admin_accessory_batch_operations"):
                return None
            rows = conn.execute(
                "SELECT operation_id,payload FROM admin_accessory_batch_operations "
                "WHERE action=%s AND status='running' ORDER BY created_at DESC,rowid DESC",
                (str(action),),
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
    ) -> tuple[AdminAccessoryBatchAdjustmentResult | None, tuple[str, ...]]:
        payload = self._payload(request, user_ids)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,status FROM admin_accessory_batch_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is None:
                    conn.execute(
                        "INSERT INTO admin_accessory_batch_operations("
                        "operation_id,action,payload,total) VALUES(%s,%s,%s,%s)",
                        (operation_id, request["action"], payload, len(user_ids)),
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
                        "SELECT user_id FROM admin_accessory_batch_progress "
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
                "requested_quantity": result.requested_quantity,
                "affected_quantity": result.affected_quantity,
                "accessories": result.accessories,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute(
                    "INSERT INTO admin_accessory_batch_progress("
                    "operation_id,user_id,status,affected_quantity,result_json) "
                    "VALUES(%s,%s,%s,%s,%s) ON CONFLICT(operation_id,user_id) "
                    "DO UPDATE SET status=EXCLUDED.status,"
                    "affected_quantity=EXCLUDED.affected_quantity,"
                    "result_json=EXCLUDED.result_json "
                    "WHERE EXCLUDED.affected_quantity>"
                    "admin_accessory_batch_progress.affected_quantity",
                    (
                        operation_id,
                        user_id,
                        result.status,
                        result.affected_quantity,
                        result_json,
                    ),
                )
                counts = conn.execute(
                    "SELECT o.total,COUNT(p.user_id) "
                    "FROM admin_accessory_batch_operations o "
                    "LEFT JOIN admin_accessory_batch_progress p "
                    "ON p.operation_id=o.operation_id WHERE o.operation_id=%s "
                    "GROUP BY o.total",
                    (operation_id,),
                ).fetchone()
                status = "completed" if int(counts[1]) >= int(counts[0]) else "running"
                conn.execute(
                    "UPDATE admin_accessory_batch_operations "
                    "SET status=%s,updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (status, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _advance(
        self,
        operation_id,
        request: dict,
        user_ids,
        *,
        chunk_size,
        create_accessory: Callable[[str], dict] | None = None,
    ) -> AdminAccessoryBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        normalized_users = tuple(
            sorted({str(user_id).strip() for user_id in user_ids if str(user_id).strip()})
        )
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"].strip()
            or not normalized_users
            or request["action"] not in {"grant", "destroy"}
            or request["item_id"] <= 0
            or request["quantity"] <= 0
        ):
            raise ValueError("invalid accessory batch arguments")
        if request["action"] == "grant" and (
            request["quality"] not in {1, 2, 3, 4, 5}
            or request["max_accessories"] <= 0
            or create_accessory is None
        ):
            raise ValueError("grant requires quality, capacity and instance factory")

        previous, pending = self._begin(
            operation_id, request, normalized_users, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                equipped, bag = self._adjustment_service.snapshot(user_id)
                child_operation = (
                    f"admin-accessory-batch:{operation_id}:{request['action']}:{user_id}"
                )
                if request["action"] == "grant":
                    result = self._adjustment_service.grant(
                        child_operation,
                        request["operator_id"],
                        user_id,
                        request["item_id"],
                        request["item_name"],
                        request["quality"],
                        request["quantity"],
                        equipped,
                        bag,
                        request["max_accessories"],
                        lambda user_id=user_id: create_accessory(user_id),
                        target_name="all",
                    )
                else:
                    result = self._adjustment_service.destroy(
                        child_operation,
                        request["operator_id"],
                        user_id,
                        request["item_id"],
                        request["item_name"],
                        request["quantity"],
                        equipped,
                        bag,
                        target_name="all",
                    )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(
                    f"accessory batch child operation conflict: {user_id}"
                )
            self._record(operation_id, user_id, result)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            return self._result(conn, operation_id, "applied")

    def grant(
        self,
        operation_id,
        operator_id,
        user_ids,
        item_id,
        item_name,
        quality,
        quantity,
        max_accessories,
        create_accessory: Callable[[str], dict],
        *,
        chunk_size=100,
    ) -> AdminAccessoryBatchAdjustmentResult:
        request = self._request(
            "grant",
            operator_id,
            item_id,
            item_name,
            quality,
            quantity,
            max_accessories,
        )
        return self._advance(
            operation_id,
            request,
            user_ids,
            chunk_size=chunk_size,
            create_accessory=create_accessory,
        )

    def destroy(
        self,
        operation_id,
        operator_id,
        user_ids,
        item_id,
        item_name,
        quantity,
        *,
        chunk_size=100,
    ) -> AdminAccessoryBatchAdjustmentResult:
        request = self._request(
            "destroy", operator_id, item_id, item_name, 0, quantity, 0
        )
        return self._advance(
            operation_id, request, user_ids, chunk_size=chunk_size
        )


__all__ = [
    "AdminAccessoryBatchAdjustmentResult",
    "AdminAccessoryBatchAdjustmentService",
]
