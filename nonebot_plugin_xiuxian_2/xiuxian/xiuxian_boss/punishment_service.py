from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorldBossPunishmentResult:
    status: str
    action: str = ""
    revision: int = 0
    bosses: tuple[dict[str, Any], ...] = ()
    deleted_bosses: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"punished", "duplicate"}


class WorldBossPunishmentService:
    """Atomically remove one or all bosses from the current world-boss session."""

    def __init__(
        self,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode_bosses(value: Any) -> list[dict[str, Any]]:
        try:
            bosses = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        if not isinstance(bosses, list):
            return []
        return [dict(boss) for boss in bosses if isinstance(boss, dict)]

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_state("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "revision INTEGER NOT NULL DEFAULT 0)"
        )
        columns = set(conn.column_names("world_boss_state"))
        if "revision" not in columns:
            conn.execute(
                "ALTER TABLE world_boss_state ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_punishment_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @classmethod
    def _result_from_json(
        cls,
        value: Any,
        status: str,
    ) -> WorldBossPunishmentResult:
        stored = json.loads(str(value))
        return WorldBossPunishmentResult(
            status=status,
            action=str(stored["action"]),
            revision=int(stored["revision"]),
            bosses=tuple(dict(boss) for boss in stored["bosses"]),
            deleted_bosses=tuple(
                dict(boss) for boss in stored["deleted_bosses"]
            ),
        )

    def snapshot(self) -> tuple[list[dict[str, Any]], int]:
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
            ).fetchone()
            if row is None:
                return [], 0
            return self._decode_bosses(row[0]), int(row[1] or 0)

    def get_result(self, operation_id: str) -> WorldBossPunishmentResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT result_json FROM world_boss_punishment_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_json(row[0], "duplicate")

    def punish(
        self,
        *,
        operation_id: str,
        action: str,
        expected_revision: int,
        expected_bosses: list[dict[str, Any]],
        boss_number: int | None = None,
    ) -> WorldBossPunishmentResult:
        operation_id = str(operation_id).strip()
        action = str(action).strip()
        expected_revision = int(expected_revision)
        expected_bosses = [dict(boss) for boss in expected_bosses]
        boss_number = int(boss_number) if boss_number is not None else None
        if not operation_id or action not in {"single", "all"}:
            raise ValueError("valid operation and punishment action are required")
        if action == "single" and (boss_number is None or boss_number <= 0):
            raise ValueError("single punishment requires a positive boss number")

        payload = self._json(
            {
                "action": action,
                "expected_revision": expected_revision,
                "expected_bosses": expected_bosses,
                "boss_number": boss_number,
            }
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                self._ensure_schema(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT payload,result_json FROM world_boss_punishment_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorldBossPunishmentResult("operation_conflict")
                    return self._result_from_json(previous[1], "duplicate")

                row = conn.execute(
                    "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
                ).fetchone()
                current_bosses = self._decode_bosses(row[0]) if row else []
                current_revision = int(row[1] or 0) if row else 0
                if (
                    current_revision != expected_revision
                    or self._json(current_bosses) != self._json(expected_bosses)
                ):
                    conn.rollback()
                    return WorldBossPunishmentResult("session_changed")
                if not current_bosses:
                    conn.rollback()
                    return WorldBossPunishmentResult("empty")

                if action == "single":
                    index = int(boss_number) - 1
                    if index < 0 or index >= len(current_bosses):
                        conn.rollback()
                        return WorldBossPunishmentResult("invalid_target")
                    deleted_bosses = [current_bosses[index]]
                    bosses = current_bosses[:index] + current_bosses[index + 1 :]
                else:
                    deleted_bosses = current_bosses
                    bosses = []

                revision = current_revision + 1
                conn.execute(
                    "UPDATE world_boss_state SET bosses=%s,updated_at=CURRENT_TIMESTAMP,"
                    "revision=%s WHERE state_key='global'",
                    (self._json(bosses), revision),
                )
                result_json = self._json(
                    {
                        "action": action,
                        "revision": revision,
                        "bosses": bosses,
                        "deleted_bosses": deleted_bosses,
                    }
                )
                conn.execute(
                    "INSERT INTO world_boss_punishment_operations("
                    "operation_id,payload,result_json,created_at) "
                    "VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return WorldBossPunishmentResult(
                    status="punished",
                    action=action,
                    revision=revision,
                    bosses=tuple(bosses),
                    deleted_bosses=tuple(deleted_bosses),
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorldBossPunishmentResult", "WorldBossPunishmentService"]
