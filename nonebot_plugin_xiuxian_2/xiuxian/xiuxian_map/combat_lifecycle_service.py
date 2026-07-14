from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapCombatLifecycleResult:
    status: str
    stamina: int = 0
    task: dict | None = None
    snapshot: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "pending"}


class MapCombatLifecycleService:
    """Persist node-combat cost, cooldown, and recoverable battle plans."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _canonical(value) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _parse(value) -> dict:
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _identity(user_id: str) -> str:
        return json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _ensure_start_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS map_combat_start_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,"
            "task_json TEXT NOT NULL DEFAULT '{}',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _ensure_player_schema(conn, *, attached: bool) -> None:
        prefix = "player_data." if attached else ""
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_combat_settlement("
            "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL DEFAULT '')"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_cooldown("
            "user_id TEXT PRIMARY KEY,combat_cd_until TEXT DEFAULT NULL)"
        )
        pragma = (
            "PRAGMA player_data.table_info(map_cooldown)"
            if attached
            else "PRAGMA table_info(map_cooldown)"
        )
        cooldown_columns = {
            str(row[1]) for row in conn.execute(pragma).fetchall()
        }
        if "combat_cd_until" not in cooldown_columns:
            conn.execute(
                f'ALTER TABLE {prefix}map_cooldown '
                'ADD COLUMN "combat_cd_until" TEXT DEFAULT NULL'
            )

    @staticmethod
    def _columns(conn, table: str) -> set[str]:
        return {
            str(row[1])
            for row in conn.execute(
                f"PRAGMA player_data.table_info({table})"
            ).fetchall()
        }

    @classmethod
    def _start_result(cls, row) -> MapCombatLifecycleResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        task = cls._parse(row[3])
        return MapCombatLifecycleResult(
            status, int(row[2] or 0), task, cls._canonical(task) if task else ""
        )

    @classmethod
    def _record_start(
        cls,
        conn,
        operation_id: str,
        payload: str,
        status: str,
        stamina: int,
        task: dict | None = None,
    ) -> MapCombatLifecycleResult:
        task = dict(task or {})
        task_json = cls._canonical(task)
        conn.execute(
            "INSERT INTO map_combat_start_operations("
            "operation_id,payload,result_status,stamina,task_json) "
            "VALUES(%s,%s,%s,%s,%s)",
            (operation_id, payload, status, stamina, task_json),
        )
        return MapCombatLifecycleResult(
            status, stamina, task, task_json if task else ""
        )

    def replay_start(
        self, operation_id, user_id
    ) -> MapCombatLifecycleResult | None:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = self._identity(user_id)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("map_combat_start_operations"):
                return None
            row = conn.execute(
                "SELECT payload,result_status,stamina,task_json "
                "FROM map_combat_start_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        if row is None:
            return None
        if str(row[0]) != payload:
            return MapCombatLifecycleResult("operation_conflict")
        return self._start_result(row)

    def get_pending(self, user_id) -> MapCombatLifecycleResult | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not conn.table_exists("map_combat_settlement"):
                return None
            row = conn.execute(
                "SELECT snapshot FROM map_combat_settlement WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        snapshot = "" if row is None or row[0] is None else str(row[0])
        if not snapshot:
            return None
        task = self._parse(snapshot)
        return MapCombatLifecycleResult("pending", task=task, snapshot=snapshot)

    def start(
        self,
        operation_id,
        user_id,
        expected_stamina,
        stamina_cost,
        expected_position,
        expected_daily,
        daily_limit,
        expected_cooldown,
        task,
    ) -> MapCombatLifecycleResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        expected_stamina, stamina_cost, daily_limit = map(
            int, (expected_stamina, stamina_cost, daily_limit)
        )
        position = {
            key: str(value) for key, value in dict(expected_position).items()
        }
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        expected_cooldown = (
            "" if expected_cooldown is None else str(expected_cooldown)
        )
        task = dict(task)
        required_task = {
            "task_id",
            "status",
            "started_at",
            "cooldown_until",
            "daily",
            "enemy",
            "node_name",
            "node_type",
        }
        if (
            not operation_id
            or not user_id
            or min(expected_stamina, stamina_cost, daily_limit) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or not daily.get("date")
            or not required_task.issubset(task)
            or str(task["task_id"]) != operation_id
            or str(task["status"]) != "running"
            or dict(task["daily"]) != dict(expected_daily)
        ):
            raise ValueError("valid combat lifecycle snapshots are required")
        payload = self._identity(user_id)
        task_json = self._canonical(task)
        started_at = str(task["started_at"])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute(
                    "ATTACH DATABASE %s AS player_data",
                    (str(self._player_database),),
                )
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_start_schema(conn)
                self._ensure_player_schema(conn, attached=True)
                previous = conn.execute(
                    "SELECT payload,result_status,stamina,task_json "
                    "FROM map_combat_start_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MapCombatLifecycleResult("operation_conflict")
                    return self._start_result(previous)

                user = conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = self._record_start(
                        conn, operation_id, payload, "user_missing", expected_stamina
                    )
                    conn.commit()
                    return result
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result

                position_columns = self._columns(conn, "map_status")
                if not set(position).issubset(position_columns):
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                position_row = conn.execute(
                    "SELECT "
                    + ",".join(f'"{key}"' for key in position)
                    + " FROM player_data.map_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if position_row is None or tuple(
                    str(value) for value in position_row
                ) != tuple(position.values()):
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result

                daily_columns = self._columns(conn, "map_daily_limit")
                required_daily = {"date", "combat_count", "resource_total_count"}
                if not required_daily.issubset(daily_columns):
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                daily_row = conn.execute(
                    "SELECT date,combat_count,resource_total_count "
                    "FROM player_data.map_daily_limit WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                expected_daily_tuple = (
                    daily["date"],
                    daily.get("combat_count", "0"),
                    daily.get("resource_total_count", "0"),
                )
                if daily_row is None or tuple(
                    str(value) for value in daily_row
                ) != expected_daily_tuple:
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                if int(daily_row[1] or 0) >= daily_limit:
                    result = self._record_start(
                        conn, operation_id, payload, "limit_reached", stamina
                    )
                    conn.commit()
                    return result

                cooldown_row = conn.execute(
                    "SELECT combat_cd_until FROM player_data.map_cooldown "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_cooldown = (
                    ""
                    if cooldown_row is None or cooldown_row[0] is None
                    else str(cooldown_row[0])
                )
                if current_cooldown != expected_cooldown:
                    result = self._record_start(
                        conn, operation_id, payload, "state_changed", stamina
                    )
                    conn.commit()
                    return result
                if current_cooldown and current_cooldown > started_at:
                    result = self._record_start(
                        conn,
                        operation_id,
                        payload,
                        "cooldown",
                        stamina,
                        {"cooldown_until": current_cooldown},
                    )
                    conn.commit()
                    return result

                pending = conn.execute(
                    "SELECT snapshot FROM player_data.map_combat_settlement "
                    "WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if pending is not None and str(pending[0] or ""):
                    pending_task = self._parse(pending[0])
                    result = self._record_start(
                        conn,
                        operation_id,
                        payload,
                        "already_running",
                        stamina,
                        pending_task,
                    )
                    conn.commit()
                    return result

                if stamina < stamina_cost:
                    result = self._record_start(
                        conn,
                        operation_id,
                        payload,
                        "stamina_insufficient",
                        stamina,
                    )
                    conn.commit()
                    return result

                remaining = stamina - stamina_cost
                conn.execute(
                    "UPDATE user_xiuxian SET user_stamina=%s WHERE user_id=%s",
                    (remaining, user_id),
                )
                conn.execute(
                    "INSERT INTO player_data.map_cooldown("
                    "user_id,combat_cd_until) VALUES(%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET "
                    "combat_cd_until=EXCLUDED.combat_cd_until",
                    (user_id, str(task["cooldown_until"])),
                )
                conn.execute(
                    "INSERT INTO player_data.map_combat_settlement(user_id,snapshot) "
                    "VALUES(%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                    "snapshot=EXCLUDED.snapshot",
                    (user_id, task_json),
                )
                result = self._record_start(
                    conn,
                    operation_id,
                    payload,
                    "applied",
                    remaining,
                    task,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def save_plan(
        self, user_id, task_id, plan
    ) -> MapCombatLifecycleResult:
        user_id, task_id = str(user_id).strip(), str(task_id).strip()
        plan = dict(plan)
        if (
            not user_id
            or not task_id
            or str(plan.get("task_id", "")) != task_id
            or str(plan.get("status", "")) != "planned"
        ):
            raise ValueError("valid combat plan is required")
        plan_json = self._canonical(plan)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_player_schema(conn, attached=False)
                row = conn.execute(
                    "SELECT snapshot FROM map_combat_settlement WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_json = "" if row is None or row[0] is None else str(row[0])
                current = self._parse(current_json)
                if str(current.get("task_id", "")) != task_id:
                    conn.rollback()
                    return MapCombatLifecycleResult("state_changed")
                if str(current.get("status", "")) == "planned":
                    conn.rollback()
                    return MapCombatLifecycleResult(
                        "duplicate", task=current, snapshot=current_json
                    )
                if str(current.get("status", "")) != "running":
                    conn.rollback()
                    return MapCombatLifecycleResult("state_changed")
                conn.execute(
                    "UPDATE map_combat_settlement SET snapshot=%s WHERE user_id=%s",
                    (plan_json, user_id),
                )
                conn.commit()
                return MapCombatLifecycleResult(
                    "applied", task=plan, snapshot=plan_json
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["MapCombatLifecycleResult", "MapCombatLifecycleService"]
