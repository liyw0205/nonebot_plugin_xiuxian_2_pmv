from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AdminBlackhouseStatusResult:
    status: str
    action: str = ""
    previous_banned: bool = False
    final_banned: bool = False
    changed: bool = False

    @property
    def succeeded(self) -> bool:
        return self.status in {"changed", "unchanged", "duplicate"}


class AdminBlackhouseSqlRepository:
    """Own the blacklist, registered-player projection and mutation receipts."""

    _REQUIRED_COLUMNS = {
        "user_xiuxian": {"user_id", "is_ban"},
        "admin_blackhouse_users": {"user_id", "name", "reason", "updated_at"},
        "admin_blackhouse_status_operations": {
            "operation_id", "payload", "result_json", "created_at",
        },
        "admin_blackhouse_imports": {"import_key", "imported_at"},
    }

    def __init__(self, database: str | Path, *, lock: RLock | None = None) -> None:
        self.database = Path(database)
        self.lock = lock or RLock()

    @staticmethod
    def _identifier(value: Any, label: str) -> str:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError(f"{label} is required")
        normalized = str(value).strip()
        if not normalized or "\x00" in normalized:
            raise ValueError(f"{label} is required")
        return normalized

    @staticmethod
    def _boolean(value: Any, label: str) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        raise ValueError(f"{label} must be boolean")

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        for table, required in cls._REQUIRED_COLUMNS.items():
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ) is None:
                return False
            definitions = uow.query_all(f'PRAGMA table_info("{table}")')
            columns = {str(row["name"]) for row in definitions}
            if not required.issubset(columns):
                return False
            if table != "user_xiuxian":
                primary_key = {str(row["name"]) for row in definitions if row["pk"]}
                expected_key = {
                    "admin_blackhouse_users": "user_id",
                    "admin_blackhouse_status_operations": "operation_id",
                    "admin_blackhouse_imports": "import_key",
                }[table]
                if primary_key != {expected_key}:
                    return False
        return uow.query_one(
            "SELECT 1 AS present FROM admin_blackhouse_imports WHERE import_key=?",
            ("legacy_json_and_is_ban_v1",),
        ) is not None

    def snapshot(self, user_id: Any) -> bool:
        user_id = self._identifier(user_id, "user id")
        if not self.database.is_file():
            raise RuntimeError("schema_missing")
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("schema_missing")
            return uow.query_one(
                "SELECT 1 AS present FROM admin_blackhouse_users WHERE user_id=?",
                (user_id,),
            ) is not None

    def is_banned(self, user_id: Any) -> bool:
        return self.snapshot(user_id)

    def list_banned(self) -> list[dict[str, str]]:
        if not self.database.is_file():
            raise RuntimeError("schema_missing")
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise RuntimeError("schema_missing")
            return [
                {
                    "user_id": str(row["user_id"]),
                    "name": str(row["name"] or row["user_id"]),
                    "reason": str(row["reason"] or ""),
                }
                for row in uow.query_all(
                    "SELECT user_id,name,reason FROM admin_blackhouse_users ORDER BY user_id"
                )
            ]

    def set_banned(
        self,
        operation_id: Any,
        operator_id: Any,
        user_id: Any,
        expected_banned: Any,
        banned: Any,
        *,
        name: str = "",
        reason: str = "",
    ) -> AdminBlackhouseStatusResult:
        operation_id = self._identifier(operation_id, "operation id")
        operator_id = self._identifier(operator_id, "operator id")
        user_id = self._identifier(user_id, "user id")
        banned = self._boolean(banned, "banned")
        normalized_expected = (
            None if expected_banned is None
            else self._boolean(expected_banned, "expected banned")
        )
        if not isinstance(name, str) or not isinstance(reason, str):
            raise ValueError("name and reason must be strings")
        action = "ban" if banned else "unban"
        if not self.database.is_file():
            return AdminBlackhouseStatusResult("schema_missing", action)

        # Descriptive metadata is not operation identity, preserving legacy receipts.
        payload = json.dumps(
            [operator_id, user_id, action], ensure_ascii=True, separators=(",", ":")
        )
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return AdminBlackhouseStatusResult("schema_missing", action)
            previous = uow.query_one(
                "SELECT payload,result_json FROM admin_blackhouse_status_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return AdminBlackhouseStatusResult("operation_conflict", action)
                saved = json.loads(str(previous["result_json"]))
                return AdminBlackhouseStatusResult(
                    "duplicate", str(saved["action"]),
                    bool(saved["previous_banned"]), bool(saved["final_banned"]),
                    bool(saved["changed"]),
                )

            actual_banned = uow.query_one(
                "SELECT 1 AS present FROM admin_blackhouse_users WHERE user_id=?",
                (user_id,),
            ) is not None
            if normalized_expected != actual_banned:
                return AdminBlackhouseStatusResult(
                    "state_changed", action, actual_banned, actual_banned,
                )

            changed = actual_banned != banned
            if banned:
                if not actual_banned:
                    uow.execute(
                        "INSERT INTO admin_blackhouse_users(user_id,name,reason,updated_at) "
                        "VALUES(?,?,?,CURRENT_TIMESTAMP)",
                        (user_id, name, reason),
                    )
                elif name:
                    uow.execute(
                        "UPDATE admin_blackhouse_users SET name=?,updated_at=CURRENT_TIMESTAMP "
                        "WHERE user_id=? AND name=''",
                        (name, user_id),
                    )
            else:
                uow.execute("DELETE FROM admin_blackhouse_users WHERE user_id=?", (user_id,))
            uow.execute(
                "UPDATE user_xiuxian SET is_ban=? WHERE user_id=? AND "
                "(is_ban IS NULL OR is_ban<>?)",
                (int(banned), user_id, int(banned)),
            )
            membership = uow.query_one(
                "SELECT 1 AS present FROM admin_blackhouse_users WHERE user_id=?",
                (user_id,),
            ) is not None
            stale_projection = uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=? "
                "AND (is_ban IS NULL OR is_ban<>?) LIMIT 1",
                (user_id, int(banned)),
            )
            if membership != banned or stale_projection is not None:
                raise RuntimeError("blackhouse state update was not applied")
            result_json = json.dumps(
                {
                    "action": action,
                    "previous_banned": actual_banned,
                    "final_banned": banned,
                    "changed": changed,
                },
                ensure_ascii=True, sort_keys=True, separators=(",", ":"),
            )
            receipt = uow.execute(
                "INSERT INTO admin_blackhouse_status_operations(operation_id,payload,result_json) "
                "VALUES(?,?,?)",
                (operation_id, payload, result_json),
            )
            if receipt.rowcount != 1:
                raise RuntimeError("blackhouse receipt was not saved")
            return AdminBlackhouseStatusResult(
                "changed" if changed else "unchanged", action,
                actual_banned, banned, changed,
            )


__all__ = ["AdminBlackhouseStatusResult", "AdminBlackhouseSqlRepository"]
