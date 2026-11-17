from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any, Callable
from zoneinfo import ZoneInfo

from ...core.errors import DomainError
from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from ..base.receipt_payload import build_payload, payload_matches


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
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorldBossDailyResetError(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, f"{reason}: {subject}", {"reason": reason, "subject": subject})


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


@dataclass(frozen=True)
class WorldBossFullRefreshResult:
    status: str
    revision: int = 0
    bosses: tuple[dict[str, Any], ...] = ()
    trigger: str = ""
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"refreshed", "duplicate"}


class WorldBossRefreshError(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, f"{reason}: {subject}", {"reason": reason, "subject": subject})


class WorldBossFullRefreshSqlRepository:
    """Persist an atomic replacement of the complete world-boss session.

    The request path is deliberately read-only with respect to schema.  The
    player migration owns both tables used here, so a missing migration fails
    closed instead of trying to repair the database while handling a command.
    """

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
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]).casefold() for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return (
            cls.REQUIRED_STATE_COLUMNS.issubset(cls._columns(uow, "world_boss_state"))
            and cls.REQUIRED_OPERATION_COLUMNS.issubset(
                cls._columns(uow, "world_boss_full_refresh_operations")
            )
        )

    @staticmethod
    def _decode(value: Any) -> list[dict[str, Any]]:
        try:
            raw = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise WorldBossRefreshError("world_boss_state_invalid", "global") from exc
        if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
            raise WorldBossRefreshError("world_boss_state_invalid", "global")
        return [dict(item) for item in raw]

    @classmethod
    def config_snapshot(cls, config: dict[str, Any], realms: list[str] | tuple[str, ...]) -> dict[str, Any]:
        normalized_realms = [str(realm) for realm in realms]
        stones = config.get("Boss灵石", {})
        return {
            "realms": normalized_realms,
            "names": list(config.get("Boss名字", [])),
            "stones": {realm: list(stones.get(realm, [])) for realm in normalized_realms},
            "multipliers": dict(config.get("Boss倍率", {})),
        }

    @staticmethod
    def _valid_bosses(bosses: list[dict[str, Any]], config: dict[str, Any]) -> bool:
        realms = list(config.get("realms", []))
        if [str(boss.get("jj", "")) for boss in bosses] != realms:
            return False
        if len(set(realms)) != len(realms):
            return False
        names = set(config.get("names", []))
        stones = config.get("stones", {})
        required = {"name", "jj", "气血", "总血量", "真元", "攻击", "max_stone", "stone"}
        for boss in bosses:
            realm = str(boss.get("jj", ""))
            if not required.issubset(boss) or boss.get("name") not in names:
                return False
            if boss.get("max_stone") not in stones.get(realm, []):
                return False
            if boss.get("stone") != boss.get("max_stone"):
                return False
            try:
                if any(int(boss[field]) < 0 for field in ("气血", "总血量", "真元", "攻击", "stone")):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    @classmethod
    def _from_json(cls, value: Any, status: str, operation_id: str = "") -> WorldBossFullRefreshResult:
        try:
            stored = json.loads(str(value))
            if (not isinstance(stored, dict) or not isinstance(stored.get("bosses"), list)
                    or any(not isinstance(boss, dict) for boss in stored["bosses"])
                    or stored.get("trigger") not in {"manual", "scheduled"}):
                raise ValueError("invalid full refresh receipt")
            return WorldBossFullRefreshResult(
                status, int(stored["revision"]), tuple(dict(boss) for boss in stored["bosses"]), stored["trigger"],
            )
        except (TypeError, ValueError, KeyError) as exc:
            raise WorldBossRefreshError("world_boss_refresh_receipt_invalid", operation_id) from exc

    def snapshot(self) -> tuple[list[dict[str, Any]], int]:
        if not self.database.is_file():
            return [], 0
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return [], 0
            row = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            return (self._decode(row["bosses"]), int(row["revision"] or 0)) if row else ([], 0)

    def get_result(self, operation_id: str) -> WorldBossFullRefreshResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not self.database.is_file():
            return None
        with self.lock, DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT result_json FROM world_boss_full_refresh_operations WHERE operation_id=?",
                (operation_id,),
            )
            return self._from_json(row["result_json"], "duplicate", operation_id) if row else None

    def refresh(
        self,
        *,
        operation_id: str,
        trigger: str,
        expected_revision: int,
        expected_bosses: list[dict[str, Any]],
        expected_config: dict[str, Any],
        bosses: list[dict[str, Any]],
    ) -> WorldBossFullRefreshResult:
        try:
            return self._refresh(
                operation_id=operation_id, trigger=trigger, expected_revision=expected_revision,
                expected_bosses=expected_bosses, expected_config=expected_config, bosses=bosses,
            )
        except WorldBossRefreshError as exc:
            # The exception must cross the UoW boundary before becoming a result.
            return WorldBossFullRefreshResult("state_changed", trigger=trigger, reason=exc.code,
                                              subject=exc.details["subject"])

    def _refresh(
        self, *, operation_id: str, trigger: str, expected_revision: int,
        expected_bosses: list[dict[str, Any]], expected_config: dict[str, Any], bosses: list[dict[str, Any]],
    ) -> WorldBossFullRefreshResult:
        operation_id = str(operation_id).strip()
        trigger = str(trigger).strip()
        expected_revision = int(expected_revision)
        expected_bosses = [dict(boss) for boss in expected_bosses]
        expected_config = dict(expected_config)
        bosses = [dict(boss) for boss in bosses]
        if not operation_id or trigger not in {"manual", "scheduled"}:
            raise ValueError("valid operation and trigger are required")
        if not self.database.is_file():
            return WorldBossFullRefreshResult("schema_missing", trigger=trigger, reason="schema_missing", subject="player_db")
        snapshot = self._json({
            "trigger": trigger,
            "expected_revision": expected_revision,
            "expected_bosses": expected_bosses,
            "expected_config": expected_config,
            "bosses": bosses,
        })
        # The receipt is an idempotency token: identity + digest, not a second copy
        # of 25 boss definitions.  The replayable boss list lives in result_json.
        payload = build_payload(
            kind="full_refresh",
            snapshot=snapshot,
            trigger=trigger,
            expected_revision=expected_revision,
            boss_count=len(bosses),
        )
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return WorldBossFullRefreshResult("schema_missing", trigger=trigger, reason="schema_missing", subject="player_db")
            previous = uow.query_one(
                "SELECT payload,result_json FROM world_boss_full_refresh_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if not payload_matches(previous["payload"], payload):
                    return WorldBossFullRefreshResult("operation_conflict", trigger=trigger,
                                                      reason="operation_conflict", subject=operation_id)
                return self._from_json(previous["result_json"], "duplicate", operation_id)

            realms = list(expected_config.get("realms", []))
            current_config = self.config_snapshot(self.config_loader(), realms)
            if current_config != expected_config:
                return WorldBossFullRefreshResult("config_changed", trigger=trigger, reason="config_changed", subject="boss_config")
            if not self._valid_bosses(bosses, current_config):
                return WorldBossFullRefreshResult("config_changed", trigger=trigger,
                                                  reason="world_boss_generated_invalid", subject="generated_bosses")

            row = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            current_bosses = self._decode(row["bosses"]) if row else []
            current_revision = int(row["revision"] or 0) if row else 0
            if current_revision != expected_revision or self._json(current_bosses) != self._json(expected_bosses):
                return WorldBossFullRefreshResult("session_changed", trigger=trigger, reason="session_changed", subject="global")

            revision = expected_revision + 1
            cursor = uow.execute(
                "INSERT INTO world_boss_state(state_key,bosses,updated_at,revision) VALUES('global',?,CURRENT_TIMESTAMP,?) "
                "ON CONFLICT(state_key) DO UPDATE SET bosses=excluded.bosses,updated_at=excluded.updated_at,revision=excluded.revision",
                (self._json(bosses), revision),
            )
            if cursor.rowcount != 1:
                raise WorldBossRefreshError("world_boss_refresh_state_write_rejected", "global")
            result_json = self._json({"revision": revision, "bosses": bosses, "trigger": trigger})
            cursor = uow.execute(
                "INSERT INTO world_boss_full_refresh_operations(operation_id,payload,result_json,created_at) VALUES(?,?,?,CURRENT_TIMESTAMP)",
                (operation_id, payload, result_json),
            )
            if cursor.rowcount != 1:
                raise WorldBossRefreshError("world_boss_refresh_receipt_write_rejected", operation_id)
            # Verify after both writes: a receipt trigger can alter the earlier state.
            state = uow.query_one("SELECT bosses,revision FROM world_boss_state WHERE state_key='global'")
            if state != {"bosses": self._json(bosses), "revision": revision}:
                raise WorldBossRefreshError("world_boss_refresh_state_write_rejected", "global")
            receipt = uow.query_one(
                "SELECT payload,result_json FROM world_boss_full_refresh_operations WHERE operation_id=?", (operation_id,),
            )
            if receipt != {"payload": payload, "result_json": result_json}:
                raise WorldBossRefreshError("world_boss_refresh_receipt_write_rejected", operation_id)
            return WorldBossFullRefreshResult("refreshed", revision, tuple(bosses), trigger)


class WorldBossDailyLimitResetSqlRepository:
    """Own resumable daily limit resets without request-time schema changes."""

    REQUIRED_BOSS_COLUMNS = {"user_id", "boss_integral", "boss_stone", "boss_battle_count"}
    REQUIRED_OPERATION_COLUMNS = {"business_date", "total", "completed", "changed", "skipped", "status", "created_at", "updated_at"}
    REQUIRED_TARGET_COLUMNS = {"business_date", "user_id", "status", "previous_integral", "previous_stone", "previous_battle_count", "updated_at"}
    FIELDS = ("boss_integral", "boss_stone", "boss_battle_count")
    PREVIOUS = ("previous_integral", "previous_stone", "previous_battle_count")

    def __init__(self, database: str | Path, lock: RLock | None = None, *, clock: Any | None = None,
                 business_timezone: Any = "Asia/Shanghai") -> None:
        self.database = Path(database)
        self.lock = lock or RLock()
        self.clock = clock or SystemClock()
        self.business_timezone = ZoneInfo(business_timezone) if isinstance(business_timezone, str) else business_timezone

    def _normalize_date(self, value: Any) -> str:
        if value is None:
            value = self.clock.now()
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(self.business_timezone)
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
    def _result(row, status: str) -> WorldBossDailyLimitResetResult:
        return WorldBossDailyLimitResetResult(
            status=status,
            business_date=str(row["business_date"]),
            task_status=str(row["status"]),
            total=int(row["total"]),
            completed=int(row["completed"]),
            changed=int(row["changed"]),
            skipped=int(row["skipped"]),
        )

    @staticmethod
    def _operation(uow, business_date):
        return uow.query_one(
            "SELECT business_date,total,completed,changed,skipped,status,created_at,updated_at "
            "FROM world_boss_daily_limit_reset_operations WHERE business_date=?", (business_date,))

    @staticmethod
    def _targets(uow, business_date):
        return uow.query_all(
            "SELECT business_date,user_id,status,previous_integral,previous_stone,previous_battle_count,updated_at "
            "FROM world_boss_daily_limit_reset_targets WHERE business_date=? ORDER BY user_id", (business_date,))

    @classmethod
    def _validate_progress(cls, operation, targets, business_date):
        if operation is not None and operation['status'] == 'completed':
            counts = tuple(operation[key] for key in ('total', 'completed', 'changed', 'skipped'))
            if (not all(isinstance(value, int) and value >= 0 for value in counts)
                    or counts[0] != counts[1] or counts[2] + counts[3] > counts[0]):
                raise WorldBossDailyResetError('world_boss_reset_progress_invalid', business_date)
            # Completed receipts remain authoritative after target-detail retention.
            return
        completed = changed = skipped = 0
        for target in targets:
            status = target['status']
            previous = tuple(target[field] for field in cls.PREVIOUS)
            if status not in {'pending', 'applied', 'skipped'} or (
                status != 'applied' and previous != (None, None, None)
            ):
                raise WorldBossDailyResetError('world_boss_reset_progress_invalid', business_date)
            if status == 'applied':
                try:
                    values = tuple(require_exact_integer(value, field=field, subject=target['user_id'])
                                   for field, value in zip(cls.PREVIOUS, previous))
                except DomainError as exc:
                    raise WorldBossDailyResetError(exc.details['reason'], target['user_id']) from exc
                changed += int(any(values))
            completed += int(status != 'pending')
            skipped += int(status == 'skipped')
        expected = (len(targets), completed, changed, skipped,
                    'completed' if completed == len(targets) else 'running')
        if operation is None or tuple(operation[key] for key in ('total', 'completed', 'changed', 'skipped', 'status')) != expected:
            raise WorldBossDailyResetError('world_boss_reset_progress_invalid', business_date)

    def reset(self, business_date: Any = None, *, chunk_size: int = 500, updated_at: Any = None) -> WorldBossDailyLimitResetResult:
        business_date = self._normalize_date(business_date)
        chunk_size = max(1, int(chunk_size))
        now = self.clock.now()
        updated_at = str(updated_at or now.strftime("%Y-%m-%d %H:%M:%S"))
        baseline = [WorldBossDailyLimitResetResult('state_changed', business_date)]
        if not self.database.is_file():
            return replace(baseline[0], status='schema_missing', reason='schema_missing', subject=business_date)
        try:
            return self._reset(business_date, chunk_size, updated_at, baseline)
        except WorldBossDailyResetError as exc:
            return replace(baseline[0], status='schema_missing' if exc.code == 'schema_missing' else 'state_changed',
                           reason=exc.code, subject=exc.details['subject'])

    def _reset(self, business_date, chunk_size, updated_at, baseline):
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not self._schema_ready(uow):
                raise WorldBossDailyResetError('schema_missing', business_date)
            operation = self._operation(uow, business_date)
            if operation is None:
                if self._targets(uow, business_date):
                    raise WorldBossDailyResetError('world_boss_reset_progress_invalid', business_date)
                user_ids = tuple(str(row["user_id"]) for row in uow.query_all("SELECT user_id FROM boss ORDER BY user_id"))
                task_status = "completed" if not user_ids else "running"
                inserted = uow.execute(
                    "INSERT INTO world_boss_daily_limit_reset_operations(business_date,total,status,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (business_date, len(user_ids), task_status, updated_at, updated_at),
                )
                if inserted.rowcount != 1:
                    raise WorldBossDailyResetError('world_boss_reset_operation_write_rejected', business_date)
                frozen = uow.executemany(
                    "INSERT INTO world_boss_daily_limit_reset_targets(business_date,user_id,updated_at) VALUES(?,?,?)",
                    ((business_date, user_id, updated_at) for user_id in user_ids),
                )
                if frozen.rowcount != len(user_ids):
                    raise WorldBossDailyResetError('world_boss_reset_freeze_write_rejected', business_date)
                expected_operation = dict(business_date=business_date, total=len(user_ids), completed=0, changed=0,
                                          skipped=0, status=task_status, created_at=updated_at, updated_at=updated_at)
                expected_targets = [dict(business_date=business_date, user_id=user_id, status='pending',
                                         previous_integral=None, previous_stone=None, previous_battle_count=None,
                                         updated_at=updated_at) for user_id in user_ids]
                operation = self._operation(uow, business_date)
                if operation != expected_operation:
                    raise WorldBossDailyResetError('world_boss_reset_operation_write_rejected', business_date)
                if self._targets(uow, business_date) != expected_targets:
                    raise WorldBossDailyResetError('world_boss_reset_freeze_write_rejected', business_date)
                if not user_ids:
                    return self._result(operation, "applied")
            else:
                targets = [] if operation['status'] == 'completed' else self._targets(uow, business_date)
                self._validate_progress(operation, targets, business_date)
                if operation['status'] == 'completed':
                    return self._result(operation, "duplicate")

        # Retain only committed progress; reporting a rejection requires no further I/O.
        baseline[0] = self._result(operation, 'state_changed')
        with self.lock, DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not self._schema_ready(uow):
                raise WorldBossDailyResetError('schema_missing', business_date)
            operation = self._operation(uow, business_date)
            targets = self._targets(uow, business_date)
            self._validate_progress(operation, targets, business_date)
            baseline[0] = self._result(operation, 'state_changed')
            if operation['status'] == 'completed':
                return self._result(operation, 'duplicate')
            pending = [row for row in targets if row['status'] == 'pending'][:chunk_size]
            changed = skipped = 0
            reset_users = []
            for pending_row in pending:
                user_id = str(pending_row["user_id"])
                row = uow.query_one(
                    "SELECT COALESCE(boss_integral,0) AS boss_integral,COALESCE(boss_stone,0) AS boss_stone,COALESCE(boss_battle_count,0) AS boss_battle_count FROM boss WHERE user_id=?",
                    (user_id,),
                )
                if row is None:
                    skipped += 1
                    marked = uow.execute(
                        "UPDATE world_boss_daily_limit_reset_targets SET status='skipped',updated_at=? WHERE business_date=? AND user_id=? AND status='pending'",
                        (updated_at, business_date, user_id),
                    )
                    if marked.rowcount != 1:
                        raise WorldBossDailyResetError('world_boss_reset_target_write_rejected', user_id)
                    pending_row.update(status='skipped', updated_at=updated_at)
                    continue
                try:
                    previous = tuple(require_exact_integer(row[field], field=field, subject=user_id) for field in self.FIELDS)
                except DomainError as exc:
                    raise WorldBossDailyResetError(exc.details['reason'], user_id) from exc
                updated = uow.execute(
                    "UPDATE boss SET boss_integral=0,boss_stone=0,boss_battle_count=0 WHERE user_id=?",
                    (user_id,),
                )
                if updated.rowcount != 1:
                    raise WorldBossDailyResetError('world_boss_reset_limit_write_rejected', user_id)
                changed += int(any(previous))
                marked = uow.execute(
                    "UPDATE world_boss_daily_limit_reset_targets SET status='applied',previous_integral=?,previous_stone=?,previous_battle_count=?,updated_at=? WHERE business_date=? AND user_id=? AND status='pending'",
                    (*previous, updated_at, business_date, user_id),
                )
                if marked.rowcount != 1:
                    raise WorldBossDailyResetError('world_boss_reset_target_write_rejected', user_id)
                pending_row.update(status='applied', updated_at=updated_at, **dict(zip(self.PREVIOUS, previous)))
                reset_users.append(user_id)
            completed = operation['completed'] + len(pending)
            task_status = "completed" if completed == operation['total'] else "running"
            progressed = uow.execute(
                "UPDATE world_boss_daily_limit_reset_operations SET completed=?,changed=changed+?,skipped=skipped+?,status=?,updated_at=? WHERE business_date=?",
                (completed, changed, skipped, task_status, updated_at, business_date),
            )
            if progressed.rowcount != 1:
                raise WorldBossDailyResetError('world_boss_reset_progress_write_rejected', business_date)
            # Validate after the last write, including effects from SQLite triggers.
            for user_id in reset_users:
                current = uow.query_one('SELECT boss_integral,boss_stone,boss_battle_count FROM boss WHERE user_id=?', (user_id,))
                if current != dict.fromkeys(self.FIELDS, 0):
                    raise WorldBossDailyResetError('world_boss_reset_limit_write_rejected', user_id)
            actual_targets = self._targets(uow, business_date)
            if actual_targets != targets:
                mismatched = next((row['user_id'] for row in targets if row not in actual_targets), business_date)
                raise WorldBossDailyResetError('world_boss_reset_target_write_rejected', mismatched)
            operation.update(completed=completed, changed=operation['changed']+changed, skipped=operation['skipped']+skipped,
                             status=task_status, updated_at=updated_at)
            if self._operation(uow, business_date) != operation:
                raise WorldBossDailyResetError('world_boss_reset_progress_write_rejected', business_date)
            return self._result(operation, "applied")


__all__ = [
    "WorldBossFullRefreshResult",
    "WorldBossFullRefreshSqlRepository",
    "WorldBossDailyLimitResetResult",
    "WorldBossDailyLimitResetSqlRepository",
    "WorldBossManualSpawnResult",
    "WorldBossManualSpawnSqlRepository",
]
