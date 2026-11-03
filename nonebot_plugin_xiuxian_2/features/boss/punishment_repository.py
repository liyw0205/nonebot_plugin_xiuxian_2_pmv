from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .schema import (
    PUNISHMENT_OPERATION_COLUMNS,
    SCHEMA_MISSING,
    STATE_COLUMNS,
    table_ready,
)


@dataclass(frozen=True)
class WorldBossPunishmentResult:
    status: str
    action: str = ""
    revision: int = 0
    bosses: tuple[dict, ...] = ()
    deleted_bosses: tuple[dict, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"punished", "duplicate"}


class WorldBossPunishmentSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _json(value) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode(value) -> list[dict]:
        try:
            raw = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []

    def snapshot(self) -> tuple[list[dict], int]:
        with DatabaseUnitOfWork(self.database) as uow:
            if not table_ready(uow, "world_boss_state", STATE_COLUMNS):
                return [], 0
            row = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            return (self._decode(row["bosses"]), int(row["revision"] or 0)) if row else ([], 0)

    def get_result(self, operation_id: str) -> WorldBossPunishmentResult | None:
        with DatabaseUnitOfWork(self.database) as uow:
            if not table_ready(uow, "world_boss_punishment_operations", PUNISHMENT_OPERATION_COLUMNS):
                return None
            row = uow.query_one("SELECT result_json FROM world_boss_punishment_operations WHERE operation_id=?", (str(operation_id).strip(),))
            if row is None:
                return None
            return self._from_json(row["result_json"], "duplicate")

    @classmethod
    def _from_json(cls, value: str, status: str) -> WorldBossPunishmentResult:
        data = json.loads(str(value))
        return WorldBossPunishmentResult(status, str(data["action"]), int(data["revision"]), tuple(data["bosses"]), tuple(data["deleted_bosses"]))

    def punish(self, operation_id: str, action: str, expected_revision: int, expected_bosses: list[dict], boss_number: int | None = None) -> WorldBossPunishmentResult:
        operation_id, action = str(operation_id).strip(), str(action).strip()
        if not operation_id or action not in {"single", "all"} or (action == "single" and (boss_number is None or int(boss_number) <= 0)):
            raise ValueError("valid operation and punishment action are required")
        expected = [dict(boss) for boss in expected_bosses]
        payload = self._json({"action": action, "expected_revision": int(expected_revision), "expected_bosses": expected, "boss_number": int(boss_number) if boss_number is not None else None})
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not table_ready(uow, "world_boss_state", STATE_COLUMNS) or not table_ready(
                uow, "world_boss_punishment_operations", PUNISHMENT_OPERATION_COLUMNS
            ):
                return WorldBossPunishmentResult(SCHEMA_MISSING)
            previous = uow.query_one("SELECT payload,result_json FROM world_boss_punishment_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return WorldBossPunishmentResult("operation_conflict")
                return self._from_json(previous["result_json"], "duplicate")
            row = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            current = self._decode(row["bosses"]) if row else []
            revision = int(row["revision"] or 0) if row else 0
            if revision != int(expected_revision) or self._json(current) != self._json(expected):
                return WorldBossPunishmentResult("session_changed")
            if not current:
                return WorldBossPunishmentResult("empty")
            if action == "single":
                index = int(boss_number) - 1
                if index < 0 or index >= len(current):
                    return WorldBossPunishmentResult("invalid_target")
                deleted, remaining = [current[index]], current[:index] + current[index + 1:]
            else:
                deleted, remaining = current, []
            new_revision = revision + 1
            uow.execute("UPDATE world_boss_state SET bosses=?,updated_at=CURRENT_TIMESTAMP,revision=? WHERE state_key='global'", (self._json(remaining), new_revision))
            result = {"action": action, "revision": new_revision, "bosses": remaining, "deleted_bosses": deleted}
            uow.execute("INSERT INTO world_boss_punishment_operations(operation_id,payload,result_json,created_at) VALUES(?,?,?,CURRENT_TIMESTAMP)", (operation_id, payload, self._json(result)))
            return WorldBossPunishmentResult("punished", action, new_revision, tuple(remaining), tuple(deleted))


__all__ = ["WorldBossPunishmentSqlRepository", "WorldBossPunishmentResult"]
