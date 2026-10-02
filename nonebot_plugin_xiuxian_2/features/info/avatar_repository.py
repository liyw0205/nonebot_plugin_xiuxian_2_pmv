"""Player-owned persistence for avatar identity routing."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...core.errors import OperationConflictError
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, request_hash


@dataclass(frozen=True)
class AvatarStateResult:
    status: str
    active_id: str | None = None
    role: str | None = None
    replayed: bool = False
    avatar_id: str | None = None
    create_time: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "applied"


@dataclass(frozen=True)
class AvatarInitializationPlan:
    user_id: str
    avatar_id: str
    create_time: str
    request_hash: str


class AvatarStateSqlRepository:
    REQUIRED_AVATAR_COLUMNS = {"user_id", "main_id", "avatar_id", "active_id", "create_time"}
    REQUIRED_RECEIPT_COLUMNS = {"operation_id", "action", "request_hash", "result_json", "created_at"}
    REQUIRED_PLAN_COLUMNS = {
        "operation_id",
        "user_id",
        "avatar_id",
        "create_time",
        "request_hash",
        "created_at",
    }

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _table_columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        tables = {
            str(row["name"]).casefold()
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }
        return (
            "avatar" in tables
            and "avatar_operation_receipts" in tables
            and "avatar_initialization_plans" in tables
            and cls.REQUIRED_AVATAR_COLUMNS.issubset(cls._table_columns(uow, "avatar"))
            and cls.REQUIRED_RECEIPT_COLUMNS.issubset(
                cls._table_columns(uow, "avatar_operation_receipts")
            )
            and cls.REQUIRED_PLAN_COLUMNS.issubset(
                cls._table_columns(uow, "avatar_initialization_plans")
            )
        )

    def get_avatar_info(self, user_id: str) -> dict[str, Any]:
        if not self.database.is_file():
            return {}
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._schema_ready(uow):
                    return {}
                row = uow.query_one("SELECT * FROM avatar WHERE user_id=? LIMIT 1", (str(user_id),))
        except sqlite3.DatabaseError:
            return {}
        if row is None:
            return {}
        result: dict[str, Any] = {}
        for name, value in row.items():
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except json.JSONDecodeError:
                    pass
            result[str(name)] = value
        return result

    def get_active_id(self, user_id: str) -> str | None:
        if not self.database.is_file():
            return None
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._schema_ready(uow):
                    return None
                row = uow.query_one(
                    "SELECT active_id, main_id FROM avatar WHERE user_id=? LIMIT 1",
                    (str(user_id),),
                )
        except sqlite3.DatabaseError:
            return None
        if row is None:
            return None
        active_id = row.get("active_id") or row.get("main_id")
        return str(active_id) if active_id else None

    def toggle(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_active_id: str,
    ) -> AvatarStateResult:
        return self._apply(
            operation_id=operation_id,
            action="avatar_toggle",
            user_id=user_id,
            expected_active_id=expected_active_id,
        )

    def restore(self, *, operation_id: str, user_id: str) -> AvatarStateResult:
        return self._apply(
            operation_id=operation_id,
            action="avatar_restore",
            user_id=user_id,
            expected_active_id=None,
        )

    def initialize(
        self,
        *,
        operation_id: str,
        user_id: str,
        proposed_avatar_id: str,
        proposed_create_time: str,
    ) -> AvatarStateResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id:
            raise ValueError("operation_id is required")
        digest = request_hash({"action": "avatar_init", "user_id": user_id})
        plan = self._reserve_initialization(
            operation_id=operation_id,
            user_id=user_id,
            proposed_avatar_id=str(proposed_avatar_id),
            proposed_create_time=str(proposed_create_time),
            digest=digest,
        )
        if isinstance(plan, AvatarStateResult):
            return plan
        return self._complete_initialization(operation_id, plan, digest)

    def _reserve_initialization(
        self,
        *,
        operation_id: str,
        user_id: str,
        proposed_avatar_id: str,
        proposed_create_time: str,
        digest: str,
    ) -> AvatarInitializationPlan | AvatarStateResult:
        if not self.database.is_file():
            return AvatarStateResult("schema_missing")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                ready = self._schema_ready(uow)
        except sqlite3.DatabaseError:
            return AvatarStateResult("schema_missing")
        if not ready:
            return AvatarStateResult("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AvatarStateResult("schema_missing")
            receipt = uow.query_one(
                "SELECT request_hash, result_json FROM avatar_operation_receipts "
                "WHERE operation_id=? AND action='avatar_init'",
                (operation_id,),
            )
            if receipt is not None:
                if receipt["request_hash"] != digest:
                    raise OperationConflictError(operation_id, "avatar_init")
                return self._receipt_result(receipt, replayed=True)

            row = uow.query_one(
                "SELECT user_id, avatar_id, create_time FROM avatar_initialization_plans "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is not None:
                if str(row["user_id"]) != user_id:
                    raise OperationConflictError(operation_id, "avatar_init")
                return AvatarInitializationPlan(
                    user_id=user_id,
                    avatar_id=str(row["avatar_id"]),
                    create_time=str(row["create_time"]),
                    request_hash=digest,
                )

            existing_avatar = uow.query_one(
                "SELECT 1 FROM avatar WHERE avatar_id=? LIMIT 1", (proposed_avatar_id,)
            )
            existing_plan = uow.query_one(
                "SELECT 1 FROM avatar_initialization_plans WHERE avatar_id=? LIMIT 1",
                (proposed_avatar_id,),
            )
            if existing_avatar is not None or existing_plan is not None:
                return AvatarStateResult("avatar_id_conflict")
            created_at = self.clock.now().isoformat()
            uow.execute(
                "INSERT INTO avatar_initialization_plans "
                "(operation_id,user_id,avatar_id,create_time,request_hash,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    proposed_avatar_id,
                    proposed_create_time,
                    digest,
                    created_at,
                ),
            )
            return AvatarInitializationPlan(
                user_id=user_id,
                avatar_id=proposed_avatar_id,
                create_time=proposed_create_time,
                request_hash=digest,
            )

    def _complete_initialization(
        self,
        operation_id: str,
        plan: AvatarInitializationPlan,
        digest: str,
    ) -> AvatarStateResult:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AvatarStateResult("schema_missing")
            receipt = uow.query_one(
                "SELECT request_hash, result_json FROM avatar_operation_receipts "
                "WHERE operation_id=? AND action='avatar_init'",
                (operation_id,),
            )
            if receipt is not None:
                if receipt["request_hash"] != digest:
                    raise OperationConflictError(operation_id, "avatar_init")
                return self._receipt_result(receipt, replayed=True)

            stored_plan = uow.query_one(
                "SELECT user_id, avatar_id, create_time, request_hash "
                "FROM avatar_initialization_plans WHERE operation_id=?",
                (operation_id,),
            )
            if stored_plan is not None:
                if str(stored_plan["user_id"]) != plan.user_id:
                    raise OperationConflictError(operation_id, "avatar_init")
                if stored_plan["request_hash"] != digest:
                    raise OperationConflictError(operation_id, "avatar_init")
                frozen_avatar_id = str(stored_plan["avatar_id"])
                frozen_create_time = str(stored_plan["create_time"])
            else:
                frozen_avatar_id = plan.avatar_id
                frozen_create_time = plan.create_time
            avatar = uow.query_one(
                "SELECT main_id, avatar_id, active_id, create_time FROM avatar "
                "WHERE user_id=? LIMIT 1",
                (plan.user_id,),
            )
            if stored_plan is None and (avatar is None or not avatar.get("avatar_id")):
                return AvatarStateResult("avatar_init_plan_missing")
            if avatar is not None and avatar.get("avatar_id"):
                main_id = str(avatar.get("main_id") or plan.user_id)
                avatar_id = str(avatar["avatar_id"])
                create_time = str(avatar.get("create_time") or frozen_create_time)
                active_id = str(avatar.get("active_id") or main_id)
            else:
                main_id = str(avatar.get("main_id") or plan.user_id) if avatar else plan.user_id
                avatar_id = frozen_avatar_id
                create_time = str(avatar.get("create_time") or frozen_create_time) if avatar else frozen_create_time
                active_id = str(avatar.get("active_id") or main_id) if avatar else main_id
                uow.execute(
                    "INSERT INTO avatar(user_id,main_id,avatar_id,active_id,create_time) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
                    "main_id=excluded.main_id, avatar_id=excluded.avatar_id, "
                    "active_id=excluded.active_id, create_time=excluded.create_time",
                    (plan.user_id, main_id, avatar_id, active_id, create_time),
                )
            role = "avatar" if active_id != main_id else "main"
            result = {
                "status": "applied",
                "active_id": active_id,
                "role": role,
                "avatar_id": avatar_id,
                "create_time": create_time,
            }
            self._record_receipt(uow, operation_id, "avatar_init", digest, result)
            uow.execute(
                "DELETE FROM avatar_initialization_plans WHERE user_id=?",
                (plan.user_id,),
            )
            return AvatarStateResult(
                status="applied",
                active_id=active_id,
                role=role,
                avatar_id=avatar_id,
                create_time=create_time,
            )

    def _apply(
        self,
        *,
        operation_id: str,
        action: str,
        user_id: str,
        expected_active_id: str | None,
    ) -> AvatarStateResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id:
            raise ValueError("operation_id is required")
        if not self.database.is_file():
            return AvatarStateResult("schema_missing")

        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                ready = self._schema_ready(uow)
        except sqlite3.DatabaseError:
            return AvatarStateResult("schema_missing")
        if not ready:
            return AvatarStateResult("schema_missing")

        payload = {
            "action": action,
            "user_id": user_id,
        }
        digest = request_hash(payload)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AvatarStateResult("schema_missing")
            receipt = uow.query_one(
                "SELECT request_hash, result_json FROM avatar_operation_receipts "
                "WHERE operation_id=? AND action=?",
                (operation_id, action),
            )
            if receipt is not None:
                if receipt["request_hash"] != digest:
                    raise OperationConflictError(operation_id, action)
                result = json.loads(str(receipt["result_json"]))
                return AvatarStateResult(
                    status=str(result["status"]),
                    active_id=str(result["active_id"]),
                    role=str(result["role"]),
                    replayed=True,
                    avatar_id=str(result["avatar_id"]) if result.get("avatar_id") else None,
                    create_time=str(result["create_time"]) if result.get("create_time") else None,
                )

            avatar = uow.query_one(
                "SELECT main_id, avatar_id, active_id FROM avatar WHERE user_id=? LIMIT 1",
                (user_id,),
            )
            if avatar is None:
                return AvatarStateResult("avatar_missing")
            main_id = str(avatar.get("main_id") or user_id)
            current_active_id = str(avatar.get("active_id") or main_id)
            if expected_active_id is not None and current_active_id != str(expected_active_id):
                return AvatarStateResult(
                    "stale_snapshot", active_id=current_active_id,
                    role="avatar" if current_active_id != main_id else "main",
                )

            if action == "avatar_toggle":
                avatar_id = str(avatar.get("avatar_id") or "")
                if not avatar_id:
                    return AvatarStateResult("avatar_missing", active_id=current_active_id)
                active_id = main_id if current_active_id != main_id else avatar_id
            else:
                active_id = main_id
            role = "avatar" if active_id != main_id else "main"
            result = {"status": "applied", "active_id": active_id, "role": role}
            self._write_active_id(uow, user_id, active_id)
            self._record_receipt(uow, operation_id, action, digest, result)
            return AvatarStateResult(status="applied", active_id=active_id, role=role)

    @staticmethod
    def _write_active_id(uow: DatabaseUnitOfWork, user_id: str, active_id: str) -> None:
        cursor = uow.execute(
            "UPDATE avatar SET active_id=? WHERE user_id=?",
            (str(active_id), str(user_id)),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("avatar_missing")

    def _record_receipt(
        self,
        uow: DatabaseUnitOfWork,
        operation_id: str,
        action: str,
        digest: str,
        result: dict[str, str],
    ) -> None:
        uow.execute(
            "INSERT INTO avatar_operation_receipts "
            "(operation_id, action, request_hash, result_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                operation_id,
                action,
                digest,
                json.dumps(result, ensure_ascii=False, sort_keys=True),
                self.clock.now().isoformat(),
            ),
        )

    @staticmethod
    def _receipt_result(receipt: dict[str, Any], *, replayed: bool) -> AvatarStateResult:
        result = json.loads(str(receipt["result_json"]))
        return AvatarStateResult(
            status=str(result["status"]),
            active_id=str(result["active_id"]) if result.get("active_id") else None,
            role=str(result["role"]) if result.get("role") else None,
            replayed=replayed,
            avatar_id=str(result["avatar_id"]) if result.get("avatar_id") else None,
            create_time=str(result["create_time"]) if result.get("create_time") else None,
        )


__all__ = ["AvatarInitializationPlan", "AvatarStateResult", "AvatarStateSqlRepository"]
