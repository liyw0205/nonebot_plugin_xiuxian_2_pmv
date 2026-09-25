from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class RiftTerminationSqlResult:
    status: str
    rift_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class RiftTerminationSqlRepository:
    operation_table = "rift_termination_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _tables(uow: DatabaseUnitOfWork) -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
        }

    @staticmethod
    def _payload_matches(stored: Any, expected: str) -> bool:
        def decode(value: Any) -> Any:
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8", errors="replace")
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (TypeError, ValueError):
                    return value.strip()
            return value

        return decode(stored) == decode(expected)

    @staticmethod
    def _payload(user_id: str, rift_data: dict[str, Any]) -> tuple[str, str]:
        snapshot = json.dumps(rift_data, ensure_ascii=False, sort_keys=True)
        payload = json.dumps([user_id, snapshot], ensure_ascii=True)
        return snapshot, payload

    def replay(self, operation_id: str, user_id: str) -> RiftTerminationSqlResult | None:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id or not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if self.operation_table not in self._tables(uow):
                return None
            row = uow.query_one(
                f"SELECT payload FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if row is None:
                return None
            stored_user_id, snapshot = json.loads(str(row["payload"]))
            if str(stored_user_id) != user_id:
                return RiftTerminationSqlResult("state_changed")
            rift_data = json.loads(str(snapshot))
            return RiftTerminationSqlResult(
                "duplicate", str(rift_data.get("name", ""))
            )

    def terminate(
        self, operation_id: str, user_id: str, rift_data: dict[str, Any]
    ) -> RiftTerminationSqlResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        snapshot, payload = self._payload(user_id, rift_data)
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        if not self.database.is_file():
            return RiftTerminationSqlResult("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            required = {self.operation_table, "rift_entries", "user_cd"}
            if not required.issubset(self._tables(uow)):
                return RiftTerminationSqlResult("schema_missing")
            previous = uow.query_one(
                f"SELECT payload FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                status = (
                    "duplicate"
                    if self._payload_matches(previous["payload"], payload)
                    else "state_changed"
                )
                return RiftTerminationSqlResult(
                    status, str(rift_data.get("name", ""))
                )

            entry = uow.query_one(
                "SELECT rift_data,status FROM rift_entries WHERE user_id=?",
                (user_id,),
            )
            if entry is None or str(entry["status"]) != "active":
                return RiftTerminationSqlResult("not_active")
            if json.loads(str(entry["rift_data"])) != json.loads(snapshot):
                return RiftTerminationSqlResult("state_changed")
            cooldown = uow.query_one(
                "SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?",
                (user_id,),
            )
            if cooldown is None or int(cooldown["type"]) != 3:
                return RiftTerminationSqlResult("state_changed")

            uow.execute(
                "UPDATE rift_entries SET status='terminated' "
                "WHERE user_id=? AND status='active'",
                (user_id,),
            )
            uow.execute(
                "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                "WHERE user_id=? AND type=3",
                (user_id,),
            )
            uow.execute(
                f"INSERT INTO {self.operation_table}(operation_id,payload) VALUES(?,?)",
                (operation_id, payload),
            )
            return RiftTerminationSqlResult(
                "applied", str(rift_data.get("name", ""))
            )


__all__ = ["RiftTerminationSqlRepository", "RiftTerminationSqlResult"]
