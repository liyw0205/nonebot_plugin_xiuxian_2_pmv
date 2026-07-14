from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock
from typing import Any, Callable

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorldBossManualSpawnResult:
    status: str
    bosses: tuple[dict[str, Any], ...] = ()
    boss: dict[str, Any] | None = None
    revision: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"spawned", "duplicate"}


class WorldBossManualSpawnService:
    """Replace one realm's world-boss session and record the operation atomically."""

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
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

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

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_state ("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "revision INTEGER NOT NULL DEFAULT 0)"
        )
        columns = set(conn.column_names("world_boss_state"))
        if "revision" not in columns:
            conn.execute(
                "ALTER TABLE world_boss_state ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS world_boss_manual_spawn_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _result_from_json(
        cls,
        value: Any,
        status: str,
    ) -> WorldBossManualSpawnResult:
        stored = json.loads(str(value))
        return WorldBossManualSpawnResult(
            status,
            tuple(dict(boss) for boss in stored["bosses"]),
            dict(stored["boss"]),
            int(stored.get("revision", 0)),
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
            try:
                bosses = json.loads(row[0])
            except (TypeError, ValueError):
                bosses = []
            if not isinstance(bosses, list):
                bosses = []
            return [dict(boss) for boss in bosses if isinstance(boss, dict)], int(
                row[1] or 0
            )

    def get_result(self, operation_id: str) -> WorldBossManualSpawnResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn)
            conn.commit()
            row = conn.execute(
                "SELECT result_json FROM world_boss_manual_spawn_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_json(row[0], "duplicate")

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
        expected_bosses = list(expected_bosses)
        expected_config = dict(expected_config)
        boss = dict(boss)
        if not operation_id:
            raise ValueError("operation_id is required")

        payload = self._json({
            "expected_revision": expected_revision,
            "expected_bosses": expected_bosses,
            "expected_config": expected_config,
            "boss": boss,
        })

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                self._ensure_schema(conn)
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
                previous = conn.execute(
                    "SELECT payload,result_json FROM world_boss_manual_spawn_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if previous[0] != payload:
                        return WorldBossManualSpawnResult("operation_conflict")
                    return self._result_from_json(previous[1], "duplicate")

                realm = str(boss.get("jj", ""))
                current_config = self.config_snapshot(self._config_loader(), realm)
                if current_config != expected_config or not self._valid_boss(boss, current_config):
                    conn.rollback()
                    return WorldBossManualSpawnResult("config_changed")

                row = conn.execute(
                    "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
                ).fetchone()
                if row is None:
                    current_bosses = []
                    current_revision = 0
                else:
                    try:
                        current_bosses = json.loads(row[0])
                    except (TypeError, ValueError):
                        current_bosses = []
                    current_revision = int(row[1] or 0)
                if (
                    not isinstance(current_bosses, list)
                    or current_revision != expected_revision
                    or self._json(current_bosses) != self._json(expected_bosses)
                ):
                    conn.rollback()
                    return WorldBossManualSpawnResult("session_changed")

                bosses = [item for item in current_bosses if item.get("jj") != realm]
                bosses.append(boss)
                bosses_json = self._json(bosses)
                revision = current_revision + 1
                conn.execute(
                    "INSERT INTO world_boss_state(state_key,bosses,updated_at,revision) "
                    "VALUES ('global',%s,CURRENT_TIMESTAMP,%s) "
                    "ON CONFLICT(state_key) DO UPDATE SET "
                    "bosses=excluded.bosses,updated_at=excluded.updated_at,"
                    "revision=excluded.revision",
                    (bosses_json, revision),
                )
                result_json = self._json(
                    {"bosses": bosses, "boss": boss, "revision": revision}
                )
                conn.execute(
                    "INSERT INTO world_boss_manual_spawn_operations(operation_id,payload,result_json) "
                    "VALUES (%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return WorldBossManualSpawnResult(
                    "spawned", tuple(bosses), boss, revision
                )
            except Exception:
                conn.rollback()
                raise
