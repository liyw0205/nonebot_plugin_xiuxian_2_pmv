from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class RiftGenerationSqlResult:
    status: str
    state: dict[str, Any] | None = None


class RiftGenerationSqlRepository:
    world_table = "rift_world_state"
    operation_table = "rift_generation_operations"

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def normalize_plan(
        value: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], tuple[str, ...]]:
        data = dict(value or {})
        raw_participants = data.pop("l_user_id", []) or []
        participants = tuple(
            dict.fromkeys(str(user_id) for user_id in raw_participants)
        )
        normalized = json.loads(
            json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
        return normalized, participants

    @staticmethod
    def _state(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "rift_key": str(row["rift_key"]),
            "generation_id": str(row["generation_id"]),
            "rift_data": json.loads(str(row["rift_data"])),
            "participants": tuple(str(value) for value in json.loads(str(row["participants"]))),
            "revision": int(row["revision"]),
        }

    @classmethod
    def _read_world(cls, uow: DatabaseUnitOfWork, rift_key: str) -> dict[str, Any] | None:
        row = uow.query_one(
            "SELECT rift_key,generation_id,rift_data,participants,revision "
            f"FROM {cls.world_table} WHERE rift_key=?",
            (rift_key,),
        )
        return cls._state(row)

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

    def get_current(self, rift_key: str) -> dict[str, Any] | None:
        rift_key = str(rift_key).strip()
        if not rift_key:
            raise ValueError("rift_key is required")
        if not self.database.is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if self.world_table not in self._tables(uow):
                return None
            return self._read_world(uow, rift_key)

    def bootstrap(
        self, rift_key: str, legacy_snapshot: dict[str, Any]
    ) -> dict[str, Any] | None:
        rift_key = str(rift_key).strip()
        rift_data, participants = self.normalize_plan(legacy_snapshot)
        if not rift_key or not rift_data:
            raise ValueError("rift_key and legacy snapshot are required")
        if not self.database.is_file():
            return None

        digest = hashlib.sha256(
            json.dumps(
                [rift_key, rift_data, participants],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:24]
        generation_id = f"legacy:{digest}"
        snapshot = json.dumps(
            rift_data, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if self.world_table not in self._tables(uow):
                return None
            current = self._read_world(uow, rift_key)
            if current is not None:
                return current
            uow.execute(
                f"INSERT INTO {self.world_table}("
                "rift_key,generation_id,rift_data,participants,revision) "
                "VALUES(?,?,?,?,1)",
                (
                    rift_key,
                    generation_id,
                    snapshot,
                    json.dumps(participants, ensure_ascii=False, separators=(",", ":")),
                ),
            )
            return {
                "rift_key": rift_key,
                "generation_id": generation_id,
                "rift_data": rift_data,
                "participants": participants,
                "revision": 1,
            }

    def generate(
        self, operation_id: str, rift_key: str, rift_plan: dict[str, Any]
    ) -> RiftGenerationSqlResult:
        operation_id = str(operation_id).strip()
        rift_key = str(rift_key).strip()
        rift_data, _ = self.normalize_plan(rift_plan)
        if (
            not operation_id
            or not rift_key
            or not str(rift_data.get("name", "")).strip()
            or int(rift_data.get("time", 0)) <= 0
        ):
            raise ValueError("valid operation, rift key and fixed plan are required")
        payload = json.dumps(
            [rift_key, rift_data],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if not self.database.is_file():
            return RiftGenerationSqlResult("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not {self.world_table, self.operation_table}.issubset(self._tables(uow)):
                return RiftGenerationSqlResult("schema_missing")
            previous = uow.query_one(
                f"SELECT payload,generation_id FROM {self.operation_table} WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                current = self._read_world(uow, rift_key)
                if not self._payload_matches(previous["payload"], payload):
                    status = "state_changed"
                elif current is None or current["generation_id"] != str(previous["generation_id"]):
                    status = "superseded"
                else:
                    status = "duplicate"
                return RiftGenerationSqlResult(status, current)

            current = self._read_world(uow, rift_key)
            revision = (int(current["revision"]) if current is not None else 0) + 1
            snapshot = json.dumps(
                rift_data, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            uow.execute(
                f"INSERT INTO {self.world_table}("
                "rift_key,generation_id,rift_data,participants,revision,updated_at) "
                "VALUES(?,?,?,'[]',?,CURRENT_TIMESTAMP) "
                "ON CONFLICT(rift_key) DO UPDATE SET "
                "generation_id=excluded.generation_id,rift_data=excluded.rift_data,"
                "participants='[]',revision=excluded.revision,updated_at=excluded.updated_at",
                (rift_key, operation_id, snapshot, revision),
            )
            uow.execute(
                f"INSERT INTO {self.operation_table}("
                "operation_id,payload,rift_key,generation_id,rift_data,revision) "
                "VALUES(?,?,?,?,?,?)",
                (operation_id, payload, rift_key, operation_id, snapshot, revision),
            )
            return RiftGenerationSqlResult(
                "applied",
                {
                    "rift_key": rift_key,
                    "generation_id": operation_id,
                    "rift_data": rift_data,
                    "participants": (),
                    "revision": revision,
                },
            )


__all__ = ["RiftGenerationSqlRepository", "RiftGenerationSqlResult"]
