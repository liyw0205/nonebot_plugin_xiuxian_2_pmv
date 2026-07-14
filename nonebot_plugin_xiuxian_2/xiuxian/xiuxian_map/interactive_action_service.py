from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class MapInteractiveActionResult:
    status: str
    stamina: int = 0
    action: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MapInteractiveActionService:
    """Persist the start and terminal states of timed map resource actions."""

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
        return json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _parse(value: str) -> dict:
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _datetime(value) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _ensure_start_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS map_interactive_start_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,"
            "action_json TEXT NOT NULL DEFAULT '{}',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _ensure_player_schema(conn, *, attached: bool) -> None:
        prefix = "player_data." if attached else ""
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_interactive_actions("
            "user_id TEXT PRIMARY KEY,action_id TEXT NOT NULL UNIQUE,"
            "action_type TEXT NOT NULL,status TEXT NOT NULL,"
            "state_json TEXT NOT NULL,settlement_json TEXT NOT NULL DEFAULT '',"
            "ready_at TEXT NOT NULL,expires_at TEXT NOT NULL,"
            "cooldown_seconds INTEGER NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_interactive_terminal_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,action_json TEXT NOT NULL DEFAULT '{}',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}map_cooldown("
            "user_id TEXT PRIMARY KEY,gather_cd_until TEXT DEFAULT NULL)"
        )
        pragma = (
            "PRAGMA player_data.table_info(map_cooldown)"
            if attached
            else "PRAGMA table_info(map_cooldown)"
        )
        columns = {str(row[1]) for row in conn.execute(pragma).fetchall()}
        if "gather_cd_until" not in columns:
            conn.execute(
                f'ALTER TABLE {prefix}map_cooldown '
                'ADD COLUMN "gather_cd_until" TEXT DEFAULT NULL'
            )

    @staticmethod
    def _columns(conn, table: str, *, attached: bool) -> set[str]:
        pragma = (
            f"PRAGMA player_data.table_info({table})"
            if attached
            else f"PRAGMA table_info({table})"
        )
        return {str(row[1]) for row in conn.execute(pragma).fetchall()}

    @classmethod
    def _start_result(cls, row) -> MapInteractiveActionResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        return MapInteractiveActionResult(
            status, int(row[2] or 0), cls._parse(str(row[3]))
        )

    @staticmethod
    def _record_start(
        conn,
        operation_id: str,
        payload: str,
        status: str,
        stamina: int,
        action: dict | None = None,
    ) -> MapInteractiveActionResult:
        action = dict(action or {})
        conn.execute(
            "INSERT INTO map_interactive_start_operations("
            "operation_id,payload,result_status,stamina,action_json) "
            "VALUES(%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                status,
                stamina,
                MapInteractiveActionService._canonical(action),
            ),
        )
        return MapInteractiveActionResult(status, stamina, action)

    def replay_start(
        self, operation_id, user_id, action_type
    ) -> MapInteractiveActionResult | None:
        operation_id = str(operation_id).strip()
        identity = [str(user_id).strip(), str(action_type).strip()]
        if not operation_id or not all(identity):
            raise ValueError("operation, user and action are required")
        payload = self._canonical(identity)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("map_interactive_start_operations"):
                return None
            row = conn.execute(
                "SELECT payload,result_status,stamina,action_json "
                "FROM map_interactive_start_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
        if row is None:
            return None
        if str(row[0]) != payload:
            return MapInteractiveActionResult("operation_conflict")
        return self._start_result(row)

    def start(
        self,
        operation_id,
        user_id,
        action_type,
        expected_stamina,
        stamina_cost,
        expected_position,
        expected_daily,
        daily_limit,
        expected_cooldown,
        action,
    ) -> MapInteractiveActionResult:
        operation_id = str(operation_id).strip()
        user_id, action_type = str(user_id).strip(), str(action_type).strip()
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
        action = dict(action)
        identity_payload = self._canonical([user_id, action_type])
        required_action = {
            "action_id",
            "action",
            "start_ts",
            "ready_ts",
            "expire_ts",
            "cooldown_sec",
        }
        if (
            not operation_id
            or not user_id
            or not action_type
            or min(expected_stamina, stamina_cost, daily_limit) < 0
            or not {"realm", "heaven", "node_id"}.issubset(position)
            or not daily.get("date")
            or not required_action.issubset(action)
            or str(action["action_id"]) != operation_id
            or str(action["action"]) != action_type
        ):
            raise ValueError("valid interactive action snapshots are required")
        action_json = self._canonical(action)
        started_at = str(action["start_ts"])

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
                    "SELECT payload,result_status,stamina,action_json "
                    "FROM map_interactive_start_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != identity_payload:
                        return MapInteractiveActionResult("operation_conflict")
                    return self._start_result(previous)

                user = conn.execute(
                    "SELECT user_stamina FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "user_missing",
                        expected_stamina,
                    )
                    conn.commit()
                    return result
                stamina = int(user[0] or 0)
                if stamina != expected_stamina:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result

                position_columns = self._columns(
                    conn, "map_status", attached=True
                )
                if not set(position).issubset(position_columns):
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
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
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result

                daily_columns = self._columns(
                    conn, "map_daily_limit", attached=True
                )
                required_daily = {"date", "gather_count", "resource_total_count"}
                if not required_daily.issubset(daily_columns):
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                daily_row = conn.execute(
                    "SELECT date,gather_count,resource_total_count "
                    "FROM player_data.map_daily_limit WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                expected_daily_tuple = (
                    daily["date"],
                    daily.get("gather_count", "0"),
                    daily.get("resource_total_count", "0"),
                )
                if daily_row is None or tuple(
                    str(value) for value in daily_row
                ) != expected_daily_tuple:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                if int(daily_row[1] or 0) >= daily_limit:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "limit_reached",
                        stamina,
                    )
                    conn.commit()
                    return result

                cooldown_row = conn.execute(
                    "SELECT gather_cd_until FROM player_data.map_cooldown "
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
                        conn,
                        operation_id,
                        identity_payload,
                        "state_changed",
                        stamina,
                    )
                    conn.commit()
                    return result
                if current_cooldown and current_cooldown > started_at:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "cooldown",
                        stamina,
                        {"cooldown_until": current_cooldown},
                    )
                    conn.commit()
                    return result

                active = conn.execute(
                    "SELECT action_id,state_json,expires_at,cooldown_seconds "
                    "FROM player_data.map_interactive_actions "
                    "WHERE user_id=%s AND status='active'",
                    (user_id,),
                ).fetchone()
                if active is not None:
                    expires_at = self._datetime(active[2])
                    start_time = self._datetime(started_at)
                    if (
                        expires_at is None
                        or start_time is None
                        or expires_at > start_time
                    ):
                        result = self._record_start(
                            conn,
                            operation_id,
                            identity_payload,
                            "already_running",
                            stamina,
                            self._parse(str(active[1])),
                        )
                        conn.commit()
                        return result
                    cooldown_until = (
                        start_time + timedelta(seconds=int(active[3] or 0))
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    conn.execute(
                        "UPDATE player_data.map_interactive_actions "
                        "SET status='expired',updated_at=%s WHERE user_id=%s",
                        (started_at, user_id),
                    )
                    conn.execute(
                        "INSERT INTO player_data.map_cooldown("
                        "user_id,gather_cd_until) VALUES(%s,%s) "
                        "ON CONFLICT(user_id) DO UPDATE SET "
                        "gather_cd_until=EXCLUDED.gather_cd_until",
                        (user_id, cooldown_until),
                    )
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
                        "cooldown",
                        stamina,
                        {"cooldown_until": cooldown_until},
                    )
                    conn.commit()
                    return result

                if stamina < stamina_cost:
                    result = self._record_start(
                        conn,
                        operation_id,
                        identity_payload,
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
                    "INSERT INTO player_data.map_interactive_actions("
                    "user_id,action_id,action_type,status,state_json,settlement_json,"
                    "ready_at,expires_at,cooldown_seconds,updated_at) "
                    "VALUES(%s,%s,%s,'active',%s,'',%s,%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET "
                    "action_id=EXCLUDED.action_id,action_type=EXCLUDED.action_type,"
                    "status='active',state_json=EXCLUDED.state_json,"
                    "settlement_json='',ready_at=EXCLUDED.ready_at,"
                    "expires_at=EXCLUDED.expires_at,"
                    "cooldown_seconds=EXCLUDED.cooldown_seconds,"
                    "updated_at=EXCLUDED.updated_at",
                    (
                        user_id,
                        operation_id,
                        action_type,
                        action_json,
                        str(action["ready_ts"]),
                        str(action["expire_ts"]),
                        int(action["cooldown_sec"]),
                        started_at,
                    ),
                )
                result = self._record_start(
                    conn,
                    operation_id,
                    identity_payload,
                    "applied",
                    remaining,
                    action,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")

    def get_active(self, user_id) -> dict | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            if not conn.table_exists("map_interactive_actions"):
                return None
            row = conn.execute(
                "SELECT action_id,state_json,settlement_json "
                "FROM map_interactive_actions "
                "WHERE user_id=%s AND status='active'",
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        action = self._parse(str(row[1]))
        action["action_id"] = str(row[0])
        if row[2]:
            action["settlement"] = self._parse(str(row[2]))
        return action

    def save_settlement(
        self, user_id, action_id, settlement
    ) -> MapInteractiveActionResult:
        user_id, action_id = str(user_id).strip(), str(action_id).strip()
        if not user_id or not action_id or not isinstance(settlement, dict):
            raise ValueError("user, action and settlement are required")
        settlement_json = self._canonical(settlement)
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_player_schema(conn, attached=False)
                row = conn.execute(
                    "SELECT state_json,settlement_json FROM map_interactive_actions "
                    "WHERE user_id=%s AND action_id=%s AND status='active'",
                    (user_id, action_id),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return MapInteractiveActionResult("state_changed")
                action = self._parse(str(row[0]))
                action["action_id"] = action_id
                if row[1]:
                    action["settlement"] = self._parse(str(row[1]))
                    conn.rollback()
                    return MapInteractiveActionResult("duplicate", action=action)
                conn.execute(
                    "UPDATE map_interactive_actions SET settlement_json=%s,"
                    "updated_at=CURRENT_TIMESTAMP "
                    "WHERE user_id=%s AND action_id=%s AND status='active'",
                    (settlement_json, user_id, action_id),
                )
                conn.commit()
                action["settlement"] = settlement
                return MapInteractiveActionResult("applied", action=action)
            except Exception:
                conn.rollback()
                raise

    @classmethod
    def _terminal_result(cls, row) -> MapInteractiveActionResult:
        status = "duplicate" if str(row[1]) == "applied" else str(row[1])
        return MapInteractiveActionResult(status, action=cls._parse(str(row[2])))

    def finish_failure(
        self,
        operation_id,
        user_id,
        action_id,
        outcome,
        cooldown_until,
    ) -> MapInteractiveActionResult:
        operation_id = str(operation_id).strip()
        user_id, action_id = str(user_id).strip(), str(action_id).strip()
        outcome, cooldown_until = str(outcome).strip(), str(cooldown_until).strip()
        if (
            not operation_id
            or not user_id
            or not action_id
            or outcome not in {"expired", "failed", "invalid"}
            or not cooldown_until
        ):
            raise ValueError("valid terminal action is required")
        payload = self._canonical(
            [user_id, action_id, outcome, cooldown_until]
        )
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_player_schema(conn, attached=False)
                previous = conn.execute(
                    "SELECT payload,result_status,action_json "
                    "FROM map_interactive_terminal_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MapInteractiveActionResult("operation_conflict")
                    return self._terminal_result(previous)

                row = conn.execute(
                    "SELECT state_json FROM map_interactive_actions "
                    "WHERE user_id=%s AND action_id=%s AND status='active'",
                    (user_id, action_id),
                ).fetchone()
                if row is None:
                    action = {}
                    result_status = "state_changed"
                else:
                    action = self._parse(str(row[0]))
                    action["action_id"] = action_id
                    conn.execute(
                        "UPDATE map_interactive_actions SET status=%s,"
                        "updated_at=CURRENT_TIMESTAMP "
                        "WHERE user_id=%s AND action_id=%s AND status='active'",
                        (outcome, user_id, action_id),
                    )
                    conn.execute(
                        "INSERT INTO map_cooldown(user_id,gather_cd_until) "
                        "VALUES(%s,%s) ON CONFLICT(user_id) DO UPDATE SET "
                        "gather_cd_until=EXCLUDED.gather_cd_until",
                        (user_id, cooldown_until),
                    )
                    result_status = "applied"
                conn.execute(
                    "INSERT INTO map_interactive_terminal_operations("
                    "operation_id,payload,result_status,action_json) "
                    "VALUES(%s,%s,%s,%s)",
                    (
                        operation_id,
                        payload,
                        result_status,
                        self._canonical(action),
                    ),
                )
                conn.commit()
                return MapInteractiveActionResult(
                    result_status, action=action
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["MapInteractiveActionResult", "MapInteractiveActionService"]
