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

    @property
    def ok(self) -> bool:
        return self.status == "applied"


class AvatarStateSqlRepository:
    REQUIRED_AVATAR_COLUMNS = {"user_id", "main_id", "avatar_id", "active_id", "create_time"}
    REQUIRED_RECEIPT_COLUMNS = {"operation_id", "action", "request_hash", "result_json", "created_at"}

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
            and cls.REQUIRED_AVATAR_COLUMNS.issubset(cls._table_columns(uow, "avatar"))
            and cls.REQUIRED_RECEIPT_COLUMNS.issubset(
                cls._table_columns(uow, "avatar_operation_receipts")
            )
        )

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


__all__ = ["AvatarStateResult", "AvatarStateSqlRepository"]
