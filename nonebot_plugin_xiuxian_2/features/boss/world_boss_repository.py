from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


@dataclass(frozen=True)
class WorldBossManualSpawnResult:
    status: str
    bosses: tuple[dict[str, Any], ...] = ()
    boss: dict[str, Any] | None = None
    revision: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"spawned", "duplicate"}


@dataclass(frozen=True)
class WorldBossDailyLimitResetResult:
    status: str
    business_date: str
    task_status: str = ""
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorldBossManualSpawnSqlRepository:
    """Own world-boss session replacement after the player migration is ready."""

    REQUIRED_STATE_COLUMNS = {"state_key", "bosses", "updated_at", "revision"}
    REQUIRED_OPERATION_COLUMNS = {"operation_id", "payload", "result_json"}

    def __init__(
        self,
        database: str | Path,
        config_loader: Callable[[], dict[str, Any]],
        lock: RLock | None = None,
    ) -> None:
        self.database = Path(database)
        self.config_loader = config_loader
        self.lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode(value: Any) -> list[dict[str, Any]]:
        try:
            raw = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]).casefold() for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            cls.REQUIRED_STATE_COLUMNS.issubset(cls._columns(uow, "world_boss_state"))
            and cls.REQUIRED_OPERATION_COLUMNS.issubset(cls._columns(uow, "world_boss_manual_spawn_operations"))
        )

    @classmethod
    def config_snapshot(cls, config: dict[str, Any], realm: str) -> dict[str, Any]:
        return {
            "realm": str(realm),
            "names": list(config.get("Boss名字", [])),
            "stones": list(config.get("Boss灵石", {}).get(realm, [])),
            "multipliers": dict(config.get("Boss倍率", {})),
        }

    @staticmethod
    def _valid_boss(boss: dict[str, Any], config: dict[str, Any]) -> bool:
        required = {"name", "jj", "气血", "总血量", "真元", "攻击", "max_stone", "stone"}
        if not required.issubset(boss) or boss["jj"] != config["realm"]:
            return False
        if boss["name"] not in config["names"]:
            return False
        if boss["max_stone"] not in config["stones"] or boss["stone"] != boss["max_stone"]:
            return False
        try:
            return all(int(boss[field]) >= 0 for field in ("气血", "总血量", "真元", "攻击", "stone"))
        except (TypeError, ValueError):
            return False

    @classmethod
    def _from_json(cls, value: Any, status: str) -> WorldBossManualSpawnResult:
        stored = json.loads(str(value))
        return WorldBossManualSpawnResult(
            status,
            tuple(dict(boss) for boss in stored.get("bosses", ())),
            dict(stored.get("boss", {})),
            int(stored.get("revision", 0)),
        )

    def snapshot(self) -> tuple[list[dict[str, Any]], int]:
        if not self.database.is_file():
            return [], 0
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return [], 0
            row = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            return (self._decode(row["bosses"]), int(row["revision"] or 0)) if row else ([], 0)

    def get_result(self, operation_id: str) -> WorldBossManualSpawnResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not self.database.is_file():
            return None
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT result_json FROM world_boss_manual_spawn_operations WHERE operation_id=?",
                (operation_id,),
            )
            return self._from_json(row["result_json"], "duplicate") if row else None

    def spawn(
        self,
        *,
        operation_id: str,
        expected_revision: int,
        expected_bosses: list[dict[str, Any]],
        expected_config: dict[str, Any],
        boss: dict[str, Any],
    ) -> WorldBossManualSpawnResult:
        operation_id = str(operation_id).strip()
        expected_revision = int(expected_revision)
        expected_bosses = [dict(item) for item in expected_bosses]
        expected_config = dict(expected_config)
        boss = dict(boss)
        if not operation_id:
            raise ValueError("operation_id is required")
        if not self.database.is_file():
            return WorldBossManualSpawnResult("schema_missing")
        payload = self._json({
            "expected_revision": expected_revision,
            "expected_bosses": expected_bosses,
            "expected_config": expected_config,
            "boss": boss,
        })
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorldBossManualSpawnResult("schema_missing")
            previous = uow.query_one(
                "SELECT payload,result_json FROM world_boss_manual_spawn_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return WorldBossManualSpawnResult("operation_conflict")
                return self._from_json(previous["result_json"], "duplicate")

            realm = str(boss.get("jj", ""))
            current_config = self.config_snapshot(self.config_loader(), realm)
            if current_config != expected_config or not self._valid_boss(boss, current_config):
                return WorldBossManualSpawnResult("config_changed")
            row = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            current_bosses = self._decode(row["bosses"]) if row else []
            current_revision = int(row["revision"] or 0) if row else 0
            if current_revision != expected_revision or self._json(current_bosses) != self._json(expected_bosses):
                return WorldBossManualSpawnResult("session_changed")
            bosses = [item for item in current_bosses if item.get("jj") != realm]
            bosses.append(boss)
            revision = current_revision + 1
            uow.execute(
                "INSERT INTO world_boss_state(state_key,bosses,updated_at,revision) "
                "VALUES ('global',?,CURRENT_TIMESTAMP,?) ON CONFLICT(state_key) DO UPDATE SET "
                "bosses=excluded.bosses,updated_at=excluded.updated_at,revision=excluded.revision",
                (self._json(bosses), revision),
            )
            result_json = self._json({"bosses": bosses, "boss": boss, "revision": revision})
            uow.execute(
                "INSERT INTO world_boss_manual_spawn_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                (operation_id, payload, result_json),
            )
            return WorldBossManualSpawnResult("spawned", tuple(bosses), boss, revision)


class WorldBossDailyLimitResetSqlRepository:
    """Own resumable daily limit resets without request-time schema changes."""

    REQUIRED_BOSS_COLUMNS = {"user_id", "boss_integral", "boss_stone", "boss_battle_count"}
    REQUIRED_OPERATION_COLUMNS = {"business_date", "total", "completed", "changed", "skipped", "status"}
    REQUIRED_TARGET_COLUMNS = {"business_date", "user_id", "status", "previous_integral", "previous_stone", "previous_battle_count"}

    def __init__(self, database: str | Path, lock: RLock | None = None, *, clock: Any | None = None) -> None:
        self.database = Path(database)
        self.lock = lock or RLock()
        self.clock = clock or SystemClock()

    @staticmethod
    def _normalize_date(value: Any) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]).casefold() for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            cls.REQUIRED_BOSS_COLUMNS.issubset(cls._columns(uow, "boss"))
            and cls.REQUIRED_OPERATION_COLUMNS.issubset(cls._columns(uow, "world_boss_daily_limit_reset_operations"))
            and cls.REQUIRED_TARGET_COLUMNS.issubset(cls._columns(uow, "world_boss_daily_limit_reset_targets"))
        )

    @staticmethod
    def _result(uow: DatabaseUnitOfWork, business_date: str, status: str) -> WorldBossDailyLimitResetResult:
        row = uow.query_one(
            "SELECT status,total,completed,changed,skipped FROM world_boss_daily_limit_reset_operations WHERE business_date=?",
            (business_date,),
        )
        if row is None:
            return WorldBossDailyLimitResetResult(status, business_date)
        return WorldBossDailyLimitResetResult(
            status=status,
            business_date=business_date,
            task_status=str(row["status"]),
            total=int(row["total"]),
            completed=int(row["completed"]),
            changed=int(row["changed"]),
            skipped=int(row["skipped"]),
        )

    def reset(self, business_date: Any = None, *, chunk_size: int = 500, updated_at: Any = None) -> WorldBossDailyLimitResetResult:
        business_date = self._normalize_date(business_date)
        chunk_size = max(1, int(chunk_size))
        now = self.clock.now()
        updated_at = str(updated_at or now.strftime("%Y-%m-%d %H:%M:%S"))
        if not self.database.is_file():
            return WorldBossDailyLimitResetResult("schema_missing", business_date)

        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorldBossDailyLimitResetResult("schema_missing", business_date)
            operation = uow.query_one(
                "SELECT status FROM world_boss_daily_limit_reset_operations WHERE business_date=?",
                (business_date,),
            )
            if operation is None:
                user_ids = tuple(str(row["user_id"]) for row in uow.query_all("SELECT user_id FROM boss ORDER BY user_id"))
                task_status = "completed" if not user_ids else "running"
                uow.execute(
                    "INSERT INTO world_boss_daily_limit_reset_operations(business_date,total,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (business_date, len(user_ids), task_status, updated_at, updated_at),
                )
                uow.executemany(
                    "INSERT INTO world_boss_daily_limit_reset_targets(business_date,user_id,updated_at) VALUES(?,?,?)",
                    ((business_date, user_id, updated_at) for user_id in user_ids),
                )
                if not user_ids:
                    return self._result(uow, business_date, "applied")
            elif str(operation["status"]) == "completed":
                return self._result(uow, business_date, "duplicate")

        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorldBossDailyLimitResetResult("schema_missing", business_date)
            pending = uow.query_all(
                "SELECT user_id FROM world_boss_daily_limit_reset_targets WHERE business_date=? AND status='pending' ORDER BY user_id LIMIT ?",
                (business_date, chunk_size),
            )
            changed = skipped = 0
            for pending_row in pending:
                user_id = str(pending_row["user_id"])
                row = uow.query_one(
                    "SELECT COALESCE(boss_integral,0) AS boss_integral,COALESCE(boss_stone,0) AS boss_stone,COALESCE(boss_battle_count,0) AS boss_battle_count FROM boss WHERE user_id=?",
                    (user_id,),
                )
                if row is None:
                    skipped += 1
                    uow.execute(
                        "UPDATE world_boss_daily_limit_reset_targets SET status='skipped',updated_at=? WHERE business_date=? AND user_id=? AND status='pending'",
                        (updated_at, business_date, user_id),
                    )
                    continue
                previous = tuple(int(row[field] or 0) for field in ("boss_integral", "boss_stone", "boss_battle_count"))
                updated = uow.execute(
                    "UPDATE boss SET boss_integral=0,boss_stone=0,boss_battle_count=0 WHERE user_id=?",
                    (user_id,),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("world boss daily reset target changed")
                changed += int(any(previous))
                uow.execute(
                    "UPDATE world_boss_daily_limit_reset_targets SET status='applied',previous_integral=?,previous_stone=?,previous_battle_count=?,updated_at=? WHERE business_date=? AND user_id=? AND status='pending'",
                    (*previous, updated_at, business_date, user_id),
                )
            progress = uow.query_one(
                "SELECT COUNT(*) AS total,COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) AS pending FROM world_boss_daily_limit_reset_targets WHERE business_date=?",
                (business_date,),
            )
            completed = int(progress["total"]) - int(progress["pending"])
            task_status = "completed" if int(progress["pending"]) == 0 else "running"
            uow.execute(
                "UPDATE world_boss_daily_limit_reset_operations SET completed=?,changed=changed+?,skipped=skipped+?,status=?,updated_at=? WHERE business_date=?",
                (completed, changed, skipped, task_status, updated_at, business_date),
            )
            return self._result(uow, business_date, "applied")


__all__ = [
    "WorldBossDailyLimitResetResult",
    "WorldBossDailyLimitResetSqlRepository",
    "WorldBossManualSpawnResult",
    "WorldBossManualSpawnSqlRepository",
]
