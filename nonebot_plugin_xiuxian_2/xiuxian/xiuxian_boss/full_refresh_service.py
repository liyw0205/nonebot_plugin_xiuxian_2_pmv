from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Iterable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorldBossFullRefreshResult:
    status: str
    revision: int = 0
    bosses: tuple[dict[str, Any], ...] = ()
    trigger: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"refreshed", "duplicate"}


class WorldBossFullRefreshService:
    """Atomically replace the complete world-boss session with a fixed plan."""

    def __init__(
        self,
        player_database: str | Path,
        config_loader: Callable[[], dict[str, Any]],
        lock: RLock | None = None,
    ) -> None:
        self._player_database = Path(player_database)
        self._config_loader = config_loader
        self._lock = lock or RLock()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def config_snapshot(
        cls,
        config: dict[str, Any],
        realms: Iterable[str],
    ) -> dict[str, Any]:
        normalized_realms = [str(realm) for realm in realms]
        stones = config.get("Boss灵石", {})
        return {
            "realms": normalized_realms,
            "names": list(config.get("Boss名字", [])),
            "stones": {
                realm: list(stones.get(realm, []))
                for realm in normalized_realms
            },
            "multipliers": dict(config.get("Boss倍率", {})),
        }

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
            "CREATE TABLE IF NOT EXISTS world_boss_full_refresh_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TEXT NOT NULL)"
        )

    @classmethod
    def _decode_bosses(cls, value) -> list[dict[str, Any]]:
        try:
            bosses = json.loads(value or "[]")
        except (TypeError, ValueError):
            return []
        if not isinstance(bosses, list):
            return []
        return [dict(boss) for boss in bosses if isinstance(boss, dict)]

    @staticmethod
    def _valid_bosses(
        bosses: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> bool:
        realms = list(config.get("realms", []))
        if [str(boss.get("jj", "")) for boss in bosses] != realms:
            return False
        if len(set(realms)) != len(realms):
            return False
        names = set(config.get("names", []))
        stones = config.get("stones", {})
        required = {
            "name",
            "jj",
            "气血",
            "总血量",
            "真元",
            "攻击",
            "max_stone",
            "stone",
        }
        for boss in bosses:
            realm = str(boss.get("jj", ""))
            if not required.issubset(boss) or boss.get("name") not in names:
                return False
            if boss.get("max_stone") not in stones.get(realm, []):
                return False
            if boss.get("stone") != boss.get("max_stone"):
                return False
            try:
                if any(
                    int(boss[field]) < 0
                    for field in ("气血", "总血量", "真元", "攻击", "stone")
                ):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    @classmethod
    def _result_from_json(
        cls,
        value,
        status: str,
    ) -> WorldBossFullRefreshResult:
        stored = json.loads(str(value))
        return WorldBossFullRefreshResult(
            status,
            int(stored["revision"]),
            tuple(dict(boss) for boss in stored["bosses"]),
            str(stored["trigger"]),
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

    def get_result(self, operation_id: str) -> WorldBossFullRefreshResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT result_json FROM world_boss_full_refresh_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_json(row[0], "duplicate")

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
        operation_id = str(operation_id).strip()
        trigger = str(trigger).strip()
        expected_revision = int(expected_revision)
        expected_bosses = [dict(boss) for boss in expected_bosses]
        expected_config = dict(expected_config)
        bosses = [dict(boss) for boss in bosses]
        if not operation_id or trigger not in {"manual", "scheduled"}:
            raise ValueError("valid operation and trigger are required")
        payload = self._json(
            {
                "trigger": trigger,
                "expected_revision": expected_revision,
                "expected_bosses": expected_bosses,
                "expected_config": expected_config,
                "bosses": bosses,
            }
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                self._ensure_schema(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT payload,result_json FROM world_boss_full_refresh_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorldBossFullRefreshResult("operation_conflict")
                    return self._result_from_json(previous[1], "duplicate")

                realms = list(expected_config.get("realms", []))
                current_config = self.config_snapshot(self._config_loader(), realms)
                if current_config != expected_config or not self._valid_bosses(
                    bosses,
                    current_config,
                ):
                    conn.rollback()
                    return WorldBossFullRefreshResult("config_changed")

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
                    return WorldBossFullRefreshResult("session_changed")

                revision = expected_revision + 1
                conn.execute(
                    "INSERT INTO world_boss_state(state_key,bosses,updated_at,revision) "
                    "VALUES('global',%s,CURRENT_TIMESTAMP,%s) "
                    "ON CONFLICT(state_key) DO UPDATE SET bosses=excluded.bosses,"
                    "updated_at=excluded.updated_at,revision=excluded.revision",
                    (self._json(bosses), revision),
                )
                result_json = self._json(
                    {
                        "revision": revision,
                        "bosses": bosses,
                        "trigger": trigger,
                    }
                )
                conn.execute(
                    "INSERT INTO world_boss_full_refresh_operations("
                    "operation_id,payload,result_json,created_at) "
                    "VALUES(%s,%s,%s,CURRENT_TIMESTAMP)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return WorldBossFullRefreshResult(
                    "refreshed",
                    revision,
                    tuple(bosses),
                    trigger,
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorldBossFullRefreshResult", "WorldBossFullRefreshService"]
