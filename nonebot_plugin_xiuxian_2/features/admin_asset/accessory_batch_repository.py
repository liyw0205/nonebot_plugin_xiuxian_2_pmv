from __future__ import annotations

import json
import shutil
from collections.abc import Callable, Sized
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .accessory_repository import AdminAccessorySqlRepository


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


class AdminAccessoryBatchSqlRepository:
    """Persist resumable accessory batch plans and process targets in bounded chunks."""

    reserve_bytes = 8 * 1024 * 1024
    bytes_per_target = 2 * 1024
    bytes_per_accessory = 4 * 1024
    target_insert_chunk_size = 500

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        accessory_repository: AdminAccessorySqlRepository | None = None,
    ) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.accessory_repository = accessory_repository or AdminAccessorySqlRepository(
            game_database, player_database
        )

    @staticmethod
    def _request(
        action: str,
        operator_id: str,
        item_id: int,
        item_name: str,
        quality: int,
        quantity: int,
        max_accessories: int,
    ) -> dict[str, Any]:
        return {
            "action": str(action),
            "operator_id": str(operator_id).strip(),
            "item_id": int(item_id),
            "item_name": str(item_name),
            "quality": int(quality),
            "quantity": int(quantity),
            "max_accessories": int(max_accessories),
        }

    @staticmethod
    def _payload(request: dict[str, Any]) -> str:
        return json.dumps(
            {"request": request},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        expected = {
            "user_xiuxian": {"user_id"},
            "admin_accessory_batch_operations": {
                "operation_id", "action", "payload", "total", "status",
            },
            "admin_accessory_batch_progress": {
                "operation_id", "user_id", "status", "affected_quantity", "result_json",
            },
            "admin_accessory_batch_targets": {
                "operation_id", "user_id", "status", "affected_quantity", "result_json",
            },
        }
        for table, required in expected.items():
            columns = {
                str(row["name"]).casefold()
                for row in uow.query_all(f'PRAGMA table_info("{table}")')
            }
            if not required.issubset(columns):
                return False
        return True

    @staticmethod
    def _decode_payload(payload: str) -> dict[str, Any] | None:
        try:
            value = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    @classmethod
    def _matches_request(cls, payload: str, request: dict[str, Any]) -> bool:
        decoded = cls._decode_payload(payload)
        return decoded is not None and decoded.get("request") == request

    @classmethod
    def _result(
        cls,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        status: str,
        action: str = "",
    ) -> AdminAccessoryBatchAdjustmentResult:
        operation = uow.query_one(
            "SELECT action,total FROM admin_accessory_batch_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None:
            return AdminAccessoryBatchAdjustmentResult(status, action)
        counts = uow.query_one(
            "SELECT SUM(CASE WHEN status!='pending' THEN 1 ELSE 0 END) AS completed,"
            "COALESCE(SUM(affected_quantity),0) AS affected_quantity,"
            "SUM(CASE WHEN status!='pending' AND affected_quantity>0 THEN 1 ELSE 0 END) "
            "AS affected_users FROM admin_accessory_batch_targets WHERE operation_id=?",
            (operation_id,),
        )
        target_count = uow.query_one(
            "SELECT COUNT(*) AS total FROM admin_accessory_batch_targets WHERE operation_id=?",
            (operation_id,),
        )
        if int(target_count["total"]) == 0:
            legacy = uow.query_one(
                "SELECT payload FROM admin_accessory_batch_operations WHERE operation_id=?",
                (operation_id,),
            )
            decoded = cls._decode_payload(str(legacy["payload"])) if legacy else None
            if decoded is not None and isinstance(decoded.get("users"), list):
                counts = uow.query_one(
                    "SELECT COUNT(*) AS completed,"
                    "COALESCE(SUM(affected_quantity),0) AS affected_quantity,"
                    "SUM(CASE WHEN affected_quantity>0 THEN 1 ELSE 0 END) AS affected_users "
                    "FROM admin_accessory_batch_progress WHERE operation_id=?",
                    (operation_id,),
                )
        completed = int(counts["completed"] or 0)
        affected_users = int(counts["affected_users"] or 0)
        return AdminAccessoryBatchAdjustmentResult(
            status,
            str(operation["action"]),
            int(operation["total"]),
            completed,
            int(counts["affected_quantity"] or 0),
            affected_users,
            completed - affected_users,
        )

    @staticmethod
    def _schema_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ) is not None

    def find_running(
        self,
        action: str,
        operator_id: str,
        item_id: int,
        item_name: str,
        quality: int,
        quantity: int,
        max_accessories: int,
    ) -> str | None:
        if not self.game_database.is_file():
            return None
        request = self._request(
            action, operator_id, item_id, item_name, quality, quantity, max_accessories
        )
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if not self._schema_exists(uow, "admin_accessory_batch_operations"):
                    return None
                rows = uow.query_all(
                    "SELECT operation_id,payload FROM admin_accessory_batch_operations "
                    "WHERE action=? AND status='running' ORDER BY created_at DESC,rowid DESC",
                    (str(action),),
                )
                for row in rows:
                    if self._matches_request(str(row["payload"]), request):
                        return str(row["operation_id"])
        except (OSError, ValueError):
            return None
        return None

    def _required_bytes(self, target_count: int, request: dict[str, Any]) -> int:
        per_target = self.bytes_per_target + (
            int(request["quantity"]) * self.bytes_per_accessory
        )
        return self.reserve_bytes + max(0, target_count) * per_target

    def _has_space(self, required: int) -> bool:
        try:
            game_free = shutil.disk_usage(self.game_database.parent).free
            player_free = shutil.disk_usage(self.player_database.parent).free
        except OSError:
            return False
        return min(game_free, player_free) >= required

    @classmethod
    def _seed_legacy_targets(
        cls,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        payload: dict[str, Any],
    ) -> None:
        users = payload.get("users")
        if not isinstance(users, list):
            return
        cls._insert_targets(uow, operation_id, users, cls.target_insert_chunk_size)
        uow.execute(
            "UPDATE admin_accessory_batch_targets AS t SET "
            "status=(SELECT p.status FROM admin_accessory_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id),"
            "affected_quantity=(SELECT p.affected_quantity FROM admin_accessory_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id),"
            "result_json=(SELECT p.result_json FROM admin_accessory_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id) "
            "WHERE t.operation_id=? AND EXISTS (SELECT 1 FROM admin_accessory_batch_progress p "
            "WHERE p.operation_id=t.operation_id AND p.user_id=t.user_id)",
            (operation_id,),
        )

    @staticmethod
    def _insert_targets(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        user_ids: Sized,
        chunk_size: int,
    ) -> int:
        inserted = []
        for raw_user_id in user_ids:
            user_id = str(raw_user_id).strip()
            if not user_id:
                continue
            inserted.append((operation_id, user_id))
            if len(inserted) >= chunk_size:
                uow.executemany(
                    "INSERT OR IGNORE INTO admin_accessory_batch_targets"
                    "(operation_id,user_id,status,affected_quantity,result_json) "
                    "VALUES(?,?,'pending',0,'{}')",
                    inserted,
                )
                inserted.clear()
        if inserted:
            uow.executemany(
                "INSERT OR IGNORE INTO admin_accessory_batch_targets"
                "(operation_id,user_id,status,affected_quantity,result_json) "
                "VALUES(?,?,'pending',0,'{}')",
                inserted,
            )
        row = uow.query_one(
            "SELECT COUNT(*) AS total FROM admin_accessory_batch_targets WHERE operation_id=?",
            (operation_id,),
        )
        return int(row["total"])

    def _begin(
        self,
        operation_id: str,
        request: dict[str, Any],
        user_ids: Sized,
        chunk_size: int,
    ) -> tuple[AdminAccessoryBatchAdjustmentResult | None, tuple[str, ...]]:
        payload_json = self._payload(request)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminAccessoryBatchAdjustmentResult("not_ready", request["action"]), ()

            previous = uow.query_one(
                "SELECT payload,status,total FROM admin_accessory_batch_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is None:
                running = uow.query_all(
                    "SELECT operation_id,payload FROM admin_accessory_batch_operations "
                    "WHERE action=? AND status='running' ORDER BY created_at DESC,rowid DESC",
                    (request["action"],),
                )
                for row in running:
                    if self._matches_request(str(row["payload"]), request):
                        return self._result(
                            uow, str(row["operation_id"]), "in_progress", request["action"]
                        ), ()

                required = self._required_bytes(len(user_ids), request)
                if not self._has_space(required):
                    return AdminAccessoryBatchAdjustmentResult(
                        "insufficient_space", request["action"], len(user_ids)
                    ), ()
                uow.execute(
                    "INSERT INTO admin_accessory_batch_operations"
                    "(operation_id,action,payload,total,status) VALUES(?,?,?,0,'running')",
                    (operation_id, request["action"], payload_json),
                )
                total = self._insert_targets(
                    uow, operation_id, user_ids, self.target_insert_chunk_size
                )
                if total <= 0:
                    raise ValueError("accessory batch requires at least one target")
                uow.execute(
                    "UPDATE admin_accessory_batch_operations SET total=? WHERE operation_id=?",
                    (total, operation_id),
                )
                previous = {"payload": payload_json, "status": "running", "total": total}
            elif not self._matches_request(str(previous["payload"]), request):
                return self._result(
                    uow, operation_id, "operation_conflict", request["action"]
                ), ()
            elif str(previous["status"]) == "completed":
                return self._result(uow, operation_id, "duplicate", request["action"]), ()

            decoded_payload = self._decode_payload(str(previous["payload"]))
            if decoded_payload is not None and "users" in decoded_payload:
                target_count = uow.query_one(
                    "SELECT COUNT(*) AS total FROM admin_accessory_batch_targets "
                    "WHERE operation_id=?",
                    (operation_id,),
                )
                if int(target_count["total"]) == 0 and int(previous["total"]) > 0:
                    if not self._has_space(
                        self._required_bytes(int(previous["total"]), request)
                    ):
                        return self._result(
                            uow, operation_id, "insufficient_space", request["action"]
                        ), ()
                    self._seed_legacy_targets(uow, operation_id, decoded_payload)

            pending = uow.query_all(
                "SELECT user_id FROM admin_accessory_batch_targets "
                "WHERE operation_id=? AND status='pending' ORDER BY user_id LIMIT ?",
                (operation_id, chunk_size),
            )
            if not pending:
                counts = uow.query_one(
                    "SELECT total,(SELECT COUNT(*) FROM admin_accessory_batch_targets "
                    "WHERE operation_id=? AND status!='pending') AS completed "
                    "FROM admin_accessory_batch_operations WHERE operation_id=?",
                    (operation_id, operation_id),
                )
                if counts is not None and int(counts["completed"]) < int(counts["total"]):
                    return self._result(
                        uow, operation_id, "in_progress", request["action"]
                    ), ()
            return None, tuple(str(row["user_id"]) for row in pending)

    def _record(self, operation_id: str, user_id: str, result: Any) -> None:
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
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.execute(
                "UPDATE admin_accessory_batch_targets SET status=?,affected_quantity=?,"
                "result_json=? WHERE operation_id=? AND user_id=? AND status='pending'",
                (
                    result.status,
                    result.affected_quantity,
                    result_json,
                    operation_id,
                    user_id,
                ),
            )
            counts = uow.query_one(
                "SELECT total,"
                "(SELECT COUNT(*) FROM admin_accessory_batch_targets t "
                "WHERE t.operation_id=o.operation_id AND t.status!='pending') AS completed "
                "FROM admin_accessory_batch_operations o WHERE operation_id=?",
                (operation_id,),
            )
            if counts is not None:
                status = "completed" if int(counts["completed"]) >= int(counts["total"]) else "running"
                uow.execute(
                    "UPDATE admin_accessory_batch_operations SET status=?,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=?",
                    (status, operation_id),
                )

    def _advance(
        self,
        operation_id: str,
        request: dict[str, Any],
        user_ids: Sized,
        *,
        chunk_size: int,
        create_accessory: Callable[[str], dict[str, Any]] | None = None,
    ) -> AdminAccessoryBatchAdjustmentResult:
        operation_id = str(operation_id).strip()
        try:
            user_count = len(user_ids)
        except TypeError as exc:
            raise ValueError("accessory batch targets must be a sized collection") from exc
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not request["operator_id"]
            or user_count <= 0
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
        if not self.game_database.is_file() or not self.player_database.is_file():
            return AdminAccessoryBatchAdjustmentResult("not_ready", request["action"])

        previous, pending = self._begin(
            operation_id, request, user_ids, chunk_size
        )
        if previous is not None:
            return previous

        for user_id in pending:
            result = None
            for _ in range(3):
                snapshot = self.accessory_repository.snapshot(user_id)
                if snapshot.status != "ok":
                    if snapshot.status == "schema_missing":
                        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                            return self._result(
                                uow, operation_id, "not_ready", request["action"]
                            )
                    from .accessory_repository import AdminAccessoryAdjustmentResult

                    result = AdminAccessoryAdjustmentResult(
                        snapshot.status, request["action"], user_id, request["quantity"]
                    )
                    break
                child_operation = (
                    f"admin-accessory-batch:{operation_id}:{request['action']}:{user_id}"
                )
                if request["action"] == "grant":
                    result = self.accessory_repository.grant(
                        child_operation,
                        request["operator_id"],
                        user_id,
                        request["item_id"],
                        request["item_name"],
                        request["quality"],
                        request["quantity"],
                        snapshot.equipped,
                        snapshot.bag,
                        request["max_accessories"],
                        lambda user_id=user_id: create_accessory(user_id),
                        target_name="all",
                    )
                else:
                    result = self.accessory_repository.destroy(
                        child_operation,
                        request["operator_id"],
                        user_id,
                        request["item_id"],
                        request["item_name"],
                        request["quantity"],
                        snapshot.equipped,
                        snapshot.bag,
                        target_name="all",
                    )
                if result.status != "state_changed":
                    break
            if result.status == "operation_conflict":
                raise RuntimeError(f"accessory batch child operation conflict: {user_id}")
            self._record(operation_id, user_id, result)

        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            return self._result(uow, operation_id, "applied", request["action"])

    def grant(
        self,
        operation_id: str,
        operator_id: str,
        user_ids: Sized,
        item_id: int,
        item_name: str,
        quality: int,
        quantity: int,
        max_accessories: int,
        create_accessory: Callable[[str], dict[str, Any]],
        *,
        chunk_size: int = 100,
    ) -> AdminAccessoryBatchAdjustmentResult:
        request = self._request(
            "grant", operator_id, item_id, item_name, quality, quantity, max_accessories
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
        operation_id: str,
        operator_id: str,
        user_ids: Sized,
        item_id: int,
        item_name: str,
        quantity: int,
        *,
        chunk_size: int = 100,
    ) -> AdminAccessoryBatchAdjustmentResult:
        request = self._request("destroy", operator_id, item_id, item_name, 0, quantity, 0)
        return self._advance(operation_id, request, user_ids, chunk_size=chunk_size)


__all__ = ["AdminAccessoryBatchAdjustmentResult", "AdminAccessoryBatchSqlRepository"]
