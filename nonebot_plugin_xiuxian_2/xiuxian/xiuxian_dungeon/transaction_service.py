from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from threading import RLock
from datetime import datetime
from ..xiuxian_utils import db_backend
from typing import Any, Callable
from ..xiuxian_utils.utils import number_to
from typing import Any
from collections.abc import Callable, Mapping
from datetime import date, datetime

@dataclass(frozen=True)
class TeamMutationResult:
    status: str
    team_id: str = ""
    team_name: str = ""
    leader_id: str = ""
    member_count: int = 0
    max_members: int = 0
    invite_id: str = ""
    expires_at: float = 0.0
    version: int = 0
    target_id: str = ""
    group_id: str = ""
    cooldown_until: str = ""
    cooldown_seconds: int = 0

@dataclass(frozen=True)
class TeamInviteSnapshot:
    invite_id: str
    team_id: str
    inviter_id: str
    invitee_id: str
    group_id: str
    created_at: float
    expires_at: float
    status: str = "pending"

@dataclass(frozen=True)
class TeamStateSnapshot:
    team_id: str
    team_name: str
    leader_id: str
    members: tuple[str, ...]
    first_join: tuple[tuple[str, int], ...]
    sessions: tuple[tuple[str, str], ...]
    version: int = 0

@dataclass(frozen=True)
class TeamExitResult:
    status: str
    team_id: str = ""
    team_name: str = ""
    new_leader_id: str = ""
    disbanded: bool = False
    cooldown_members: tuple[str, ...] = ()
    cooldown_until: str = ""
    version: int = 0
    target_id: str = ""

class DungeonTeamTransactionService:
    """Transactional owner for the complete dungeon-team lifecycle."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _add_columns(conn, table: str, fields: dict[str, str]) -> None:
        columns = set(conn.column_names(table))
        for name, definition in fields.items():
            if name not in columns:
                conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS teams (user_id TEXT PRIMARY KEY)")
        cls._add_columns(
            conn,
            "teams",
            {
                "team_id": "TEXT DEFAULT NULL",
                "team_name": "TEXT DEFAULT NULL",
                "group_id": "TEXT DEFAULT NULL",
                "leader": "TEXT DEFAULT NULL",
                "members": "TEXT DEFAULT NULL",
                "create_time": "TEXT DEFAULT NULL",
                "max_members": "INTEGER DEFAULT 4",
                "description": "TEXT DEFAULT NULL",
                "version": "INTEGER NOT NULL DEFAULT 0",
            },
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_team_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,team_id TEXT NOT NULL,"
            "result_json TEXT NOT NULL DEFAULT '',action TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cls._add_columns(
            conn,
            "dungeon_team_operations",
            {
                "result_json": "TEXT NOT NULL DEFAULT ''",
                "action": "TEXT NOT NULL DEFAULT ''",
            },
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_team_invites ("
            "invite_id TEXT PRIMARY KEY,team_id TEXT NOT NULL,inviter_id TEXT NOT NULL,"
            "invitee_id TEXT NOT NULL,group_id TEXT NOT NULL,expires_at REAL NOT NULL,"
            "consumed_at TIMESTAMP DEFAULT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "created_at REAL NOT NULL DEFAULT 0,resolved_operation_id TEXT DEFAULT NULL)"
        )
        cls._add_columns(
            conn,
            "dungeon_team_invites",
            {
                "status": "TEXT NOT NULL DEFAULT 'pending'",
                "created_at": "REAL NOT NULL DEFAULT 0",
                "resolved_operation_id": "TEXT DEFAULT NULL",
            },
        )
        conn.execute(
            "UPDATE dungeon_team_invites SET status='consumed' "
            "WHERE consumed_at IS NOT NULL AND status='pending'"
        )
        conn.execute(
            "UPDATE dungeon_team_invites SET created_at=expires_at-60 "
            "WHERE created_at IS NULL OR created_at=0"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dungeon_team_invites_pending "
            "ON dungeon_team_invites (invitee_id,status,expires_at)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS team_cd ("
            "user_id TEXT PRIMARY KEY,join_cd_until TEXT DEFAULT '',"
            "had_first_join INTEGER DEFAULT 0)"
        )
        cls._add_columns(
            conn,
            "team_cd",
            {
                "join_cd_until": "TEXT DEFAULT ''",
                "had_first_join": "INTEGER DEFAULT 0",
            },
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_team_exit_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _json(data: dict) -> str:
        return json.dumps(data, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _intent(cls, action: str, **values) -> str:
        return cls._json({"action": action, **values})

    @staticmethod
    def _members(value) -> list[str]:
        try:
            decoded = json.loads(value or "[]") if isinstance(value, str) else value
        except (TypeError, ValueError):
            decoded = []
        if not isinstance(decoded, list):
            return []
        return [str(item) for item in decoded if str(item).strip()]

    @classmethod
    def _user_team(cls, conn, user_id: str) -> str:
        for row in conn.execute("SELECT user_id,members FROM teams").fetchall():
            if user_id in cls._members(row[1]):
                return str(row[0])
        return ""

    @staticmethod
    def _active_session(conn, user_id: str) -> bool:
        if not conn.table_exists("player_dungeon_status"):
            return False
        row = conn.execute(
            "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        return row is not None and str(row[0]) == "exploring"

    @classmethod
    def _team_has_active_session(cls, conn, members: list[str] | tuple[str, ...]) -> bool:
        return any(cls._active_session(conn, member) for member in members)

    @staticmethod
    def _session_status(conn, user_id: str) -> str:
        if not conn.table_exists("player_dungeon_status"):
            return ""
        row = conn.execute(
            "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        return "" if row is None else str(row[0] or "")

    @staticmethod
    def _first_join(conn, user_id: str) -> int:
        row = conn.execute(
            "SELECT had_first_join FROM team_cd WHERE user_id=%s",
            (user_id,),
        ).fetchone()
        return int(row[0] or 0) if row else 0

    @staticmethod
    def _cooldown_until(conn, user_id: str) -> str:
        row = conn.execute(
            "SELECT join_cd_until FROM team_cd WHERE user_id=%s",
            (str(user_id),),
        ).fetchone()
        return str(row[0] or "") if row else ""

    @classmethod
    def _active_cooldown(cls, conn, user_id: str, now_timestamp: float) -> str:
        until = cls._cooldown_until(conn, user_id)
        if not until:
            return ""
        try:
            deadline = datetime.strptime(until, "%Y-%m-%d %H:%M:%S").timestamp()
        except (TypeError, ValueError):
            return ""
        return until if deadline > float(now_timestamp) else ""

    @staticmethod
    def _cooldown_seconds(until: str, now_timestamp: float) -> int:
        try:
            deadline = datetime.strptime(until, "%Y-%m-%d %H:%M:%S").timestamp()
        except (TypeError, ValueError):
            return 0
        return max(0, int(deadline - float(now_timestamp)))

    @classmethod
    def _snapshot_from_row(cls, conn, team_id: str, row) -> TeamStateSnapshot:
        members = tuple(cls._members(row[2]))
        return TeamStateSnapshot(
            team_id=str(team_id),
            team_name=str(row[0] or ""),
            leader_id=str(row[1] or ""),
            members=members,
            first_join=tuple((member, cls._first_join(conn, member)) for member in members),
            sessions=tuple((member, cls._session_status(conn, member)) for member in members),
            version=int(row[3] or 0),
        )

    @staticmethod
    def _team_state_matches(current: TeamStateSnapshot, expected: TeamStateSnapshot) -> bool:
        return (
            current.team_id == expected.team_id
            and current.team_name == expected.team_name
            and current.leader_id == expected.leader_id
            and current.members == expected.members
            and current.version == expected.version
        )

    def snapshot(self, team_id: str) -> TeamStateSnapshot | None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT team_name,leader,members,version FROM teams WHERE user_id=%s",
                    (str(team_id),),
                ).fetchone()
                result = None if row is None else self._snapshot_from_row(conn, str(team_id), row)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def operation_result(
        self, operation_id: str, expected_action: str = ""
    ) -> TeamMutationResult | None:
        """Return a recorded result before handler preflight reads mutable state."""
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT payload,result_status,team_id,result_json,action "
                    "FROM dungeon_team_operations WHERE operation_id=%s",
                    (str(operation_id),),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return None
                action = str(row[4] or "")
                if not action:
                    try:
                        action = str(json.loads(str(row[0])).get("action", ""))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        action = ""
                if expected_action and action != str(expected_action):
                    conn.commit()
                    return TeamMutationResult("state_changed", str(row[2] or ""))
                result = self._decode_mutation(
                    str(row[3] or ""), str(row[1]), str(row[2] or "")
                )
                conn.commit()
                return replace(result, status="duplicate") if result.status == "applied" else result
            except Exception:
                conn.rollback()
                raise

    def exit_operation_result(
        self,
        operation_id: str,
        expected_action: str = "",
        actor_id: str = "",
        target_id: str | None = None,
    ) -> TeamExitResult | None:
        """Replay leave/kick/disband even after the actor no longer has a team."""
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT payload,result_json FROM dungeon_team_exit_operations "
                    "WHERE operation_id=%s",
                    (str(operation_id),),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return None
                try:
                    payload = json.loads(str(row[0]))
                except (TypeError, ValueError, json.JSONDecodeError):
                    conn.commit()
                    return TeamExitResult("state_changed")
                if expected_action and str(payload.get("action", "")) != str(expected_action):
                    conn.commit()
                    return TeamExitResult("state_changed")
                if actor_id and str(payload.get("actor_id", "")) != str(actor_id):
                    conn.commit()
                    return TeamExitResult("state_changed")
                if target_id is not None and str(payload.get("target_id", "")) != str(target_id):
                    conn.commit()
                    return TeamExitResult("state_changed")
                result = self._decode_result(str(row[1]))
                conn.commit()
                return replace(result, status="duplicate") if result.status == "applied" else result
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _mutation_json(result: TeamMutationResult) -> str:
        return DungeonTeamTransactionService._json(asdict(result))

    @staticmethod
    def _decode_mutation(value: str, fallback_status: str, team_id: str) -> TeamMutationResult:
        if value:
            try:
                data = json.loads(value)
                return TeamMutationResult(
                    status=str(data.get("status", fallback_status)),
                    team_id=str(data.get("team_id", team_id)),
                    team_name=str(data.get("team_name", "")),
                    leader_id=str(data.get("leader_id", "")),
                    member_count=int(data.get("member_count", 0) or 0),
                    max_members=int(data.get("max_members", 0) or 0),
                    invite_id=str(data.get("invite_id", "")),
                    expires_at=float(data.get("expires_at", 0) or 0),
                    version=int(data.get("version", 0) or 0),
                    target_id=str(data.get("target_id", "")),
                    group_id=str(data.get("group_id", "")),
                    cooldown_until=str(data.get("cooldown_until", "")),
                    cooldown_seconds=int(data.get("cooldown_seconds", 0) or 0),
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        return TeamMutationResult(str(fallback_status), str(team_id))

    @classmethod
    def _replay_mutation(cls, row, payload: str) -> TeamMutationResult:
        stored_payload = str(row[0])
        if stored_payload != payload:
            try:
                payload_matches = json.loads(stored_payload) == json.loads(payload)
            except (TypeError, ValueError, json.JSONDecodeError):
                payload_matches = False
            if not payload_matches:
                return TeamMutationResult("state_changed", str(row[2] or ""))
        result = cls._decode_mutation(str(row[3] or ""), str(row[1]), str(row[2] or ""))
        return replace(result, status="duplicate") if result.status == "applied" else result

    @classmethod
    def _write_mutation_operation(
        cls,
        conn,
        operation_id: str,
        action: str,
        payload: str,
        result: TeamMutationResult,
    ) -> None:
        conn.execute(
            "INSERT INTO dungeon_team_operations "
            "(operation_id,payload,result_status,team_id,result_json,action) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                result.status,
                result.team_id,
                cls._mutation_json(result),
                action,
            ),
        )

    @classmethod
    def _finish_mutation(
        cls,
        conn,
        operation_id: str,
        action: str,
        payload: str,
        result: TeamMutationResult,
    ) -> TeamMutationResult:
        cls._write_mutation_operation(conn, operation_id, action, payload, result)
        conn.commit()
        return result

    @staticmethod
    def _result_json(result: TeamExitResult) -> str:
        data = asdict(result)
        data["cooldown_members"] = list(result.cooldown_members)
        return DungeonTeamTransactionService._json(data)

    @staticmethod
    def _decode_result(value: str) -> TeamExitResult:
        data = json.loads(value)
        return TeamExitResult(
            status=str(data.get("status", "duplicate")),
            team_id=str(data.get("team_id", "")),
            team_name=str(data.get("team_name", "")),
            new_leader_id=str(data.get("new_leader_id", "")),
            disbanded=bool(data.get("disbanded", False)),
            cooldown_members=tuple(str(item) for item in data.get("cooldown_members", [])),
            cooldown_until=str(data.get("cooldown_until", "")),
            version=int(data.get("version", 0) or 0),
            target_id=str(data.get("target_id", "")),
        )

    @classmethod
    def _finish_exit(
        cls,
        conn,
        operation_id: str,
        payload: str,
        result: TeamExitResult,
    ) -> TeamExitResult:
        conn.execute(
            "INSERT INTO dungeon_team_exit_operations "
            "(operation_id,payload,result_json) VALUES (%s,%s,%s)",
            (operation_id, payload, cls._result_json(result)),
        )
        conn.commit()
        return result

    @staticmethod
    def _exit_payload_matches(stored: str, current: str) -> bool:
        if stored == current:
            return True
        try:
            old_data, new_data = json.loads(stored), json.loads(current)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        old_team_id = old_data.get("team_id") or (old_data.get("expected") or {}).get("team_id")
        return (
            old_data.get("action") == new_data.get("action")
            and str(old_data.get("actor_id", "")) == str(new_data.get("actor_id", ""))
            and str(old_data.get("target_id", "")) == str(new_data.get("target_id", ""))
            and str(old_team_id or "") == str(new_data.get("team_id", ""))
        )

    def _exit_mutation(
        self,
        *,
        action: str,
        operation_id: str,
        actor_id: str,
        target_id: str,
        expected: TeamStateSnapshot,
        cooldown_until: str,
    ) -> TeamExitResult:
        operation_id = str(operation_id).strip()
        actor_id, target_id = str(actor_id), str(target_id)
        payload = self._intent(
            action,
            actor_id=actor_id,
            target_id=target_id,
            team_id=str(expected.team_id),
        )
        if not operation_id:
            raise ValueError("operation_id is required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM dungeon_team_exit_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if not self._exit_payload_matches(str(previous[0]), payload):
                        return TeamExitResult(
                            "state_changed",
                            expected.team_id,
                            expected.team_name,
                            target_id=target_id,
                        )
                    old_result = self._decode_result(str(previous[1]))
                    return replace(old_result, status="duplicate") if old_result.status == "applied" else old_result

                row = conn.execute(
                    "SELECT team_name,leader,members,version FROM teams WHERE user_id=%s",
                    (expected.team_id,),
                ).fetchone()
                if row is None:
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "team_missing",
                            expected.team_id,
                            expected.team_name,
                            target_id=target_id,
                        ),
                    )
                current = self._snapshot_from_row(conn, expected.team_id, row)
                if not self._team_state_matches(current, expected):
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "state_changed",
                            expected.team_id,
                            expected.team_name,
                            target_id=target_id,
                        ),
                    )
                if actor_id not in current.members:
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "actor_not_member",
                            current.team_id,
                            current.team_name,
                            target_id=target_id,
                        ),
                    )
                if action in {"kick", "disband"} and actor_id != current.leader_id:
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "actor_not_leader",
                            current.team_id,
                            current.team_name,
                            target_id=target_id,
                        ),
                    )
                if action in {"leave", "kick"} and target_id not in current.members:
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "target_not_member",
                            current.team_id,
                            current.team_name,
                            target_id=target_id,
                        ),
                    )
                if action == "kick" and target_id == actor_id:
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "self_target",
                            current.team_id,
                            current.team_name,
                            target_id=target_id,
                        ),
                    )
                if self._team_has_active_session(conn, current.members):
                    return self._finish_exit(
                        conn,
                        operation_id,
                        payload,
                        TeamExitResult(
                            "session_active",
                            current.team_id,
                            current.team_name,
                            target_id=target_id,
                        ),
                    )

                members = list(current.members)
                affected = list(members) if action == "disband" else [target_id]
                disbanded = action == "disband" or (action == "leave" and len(members) == 1)
                new_leader = ""
                next_version = current.version + 1
                if disbanded:
                    cursor = conn.execute(
                        "DELETE FROM teams WHERE user_id=%s AND version=%s",
                        (current.team_id, current.version),
                    )
                else:
                    members.remove(target_id)
                    new_leader = members[0] if target_id == current.leader_id else current.leader_id
                    cursor = conn.execute(
                        "UPDATE teams SET members=%s,leader=%s,version=version+1 "
                        "WHERE user_id=%s AND version=%s",
                        (json.dumps(members), new_leader, current.team_id, current.version),
                    )
                if cursor.rowcount != 1:
                    conn.rollback()
                    return TeamExitResult(
                        "state_changed",
                        current.team_id,
                        current.team_name,
                        target_id=target_id,
                    )

                cooldown_members = []
                first_join = dict(current.first_join)
                for member in affected:
                    if first_join.get(member, 0) == 1:
                        conn.execute(
                            "INSERT INTO team_cd (user_id,join_cd_until,had_first_join) "
                            "VALUES (%s,%s,1) ON CONFLICT(user_id) DO UPDATE SET "
                            "join_cd_until=excluded.join_cd_until",
                            (member, str(cooldown_until)),
                        )
                        cooldown_members.append(member)

                placeholders = ",".join("%s" for _ in affected)
                conn.execute(
                    "UPDATE dungeon_team_invites SET consumed_at=CURRENT_TIMESTAMP,"
                    "status='team_changed',resolved_operation_id=%s "
                    f"WHERE status='pending' AND (team_id=%s OR invitee_id IN ({placeholders}))",
                    tuple([operation_id, current.team_id, *affected]),
                )
                result = TeamExitResult(
                    "applied",
                    current.team_id,
                    current.team_name,
                    new_leader,
                    disbanded,
                    tuple(cooldown_members),
                    str(cooldown_until),
                    next_version,
                    target_id,
                )
                return self._finish_exit(conn, operation_id, payload, result)
            except Exception:
                conn.rollback()
                raise

    def leave(
        self,
        operation_id: str,
        actor_id: str,
        expected: TeamStateSnapshot,
        cooldown_until: str,
    ) -> TeamExitResult:
        return self._exit_mutation(
            action="leave",
            operation_id=operation_id,
            actor_id=actor_id,
            target_id=actor_id,
            expected=expected,
            cooldown_until=cooldown_until,
        )

    def kick(
        self,
        operation_id: str,
        actor_id: str,
        target_id: str,
        expected: TeamStateSnapshot,
        cooldown_until: str,
    ) -> TeamExitResult:
        return self._exit_mutation(
            action="kick",
            operation_id=operation_id,
            actor_id=actor_id,
            target_id=target_id,
            expected=expected,
            cooldown_until=cooldown_until,
        )

    def disband(
        self,
        operation_id: str,
        actor_id: str,
        expected: TeamStateSnapshot,
        cooldown_until: str,
    ) -> TeamExitResult:
        return self._exit_mutation(
            action="disband",
            operation_id=operation_id,
            actor_id=actor_id,
            target_id="",
            expected=expected,
            cooldown_until=cooldown_until,
        )

    def create(
        self,
        operation_id,
        team_id,
        team_name,
        leader_id,
        group_id,
        created_at,
        now_timestamp=None,
    ) -> TeamMutationResult:
        operation_id = str(operation_id).strip()
        team_id, leader_id = str(team_id), str(leader_id)
        payload = self._intent(
            "create",
            team_id=team_id,
            team_name=str(team_name),
            leader_id=leader_id,
            group_id=str(group_id),
        )
        if not operation_id or not team_id:
            raise ValueError("operation_id and team_id are required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,result_status,team_id,result_json "
                    "FROM dungeon_team_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return self._replay_mutation(old, payload)
                base = {
                    "team_id": team_id,
                    "team_name": str(team_name),
                    "leader_id": leader_id,
                    "target_id": leader_id,
                    "group_id": str(group_id),
                }
                if not str(group_id):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "create",
                        payload,
                        TeamMutationResult("group_required", **base),
                    )
                if conn.table_exists("user_xiuxian") and conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (leader_id,)
                ).fetchone() is None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "create",
                        payload,
                        TeamMutationResult("user_missing", **base),
                    )
                if self._user_team(conn, leader_id):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "create",
                        payload,
                        TeamMutationResult("user_has_team", **base),
                    )
                if self._active_session(conn, leader_id):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "create",
                        payload,
                        TeamMutationResult("session_active", **base),
                    )
                now = float(
                    now_timestamp if now_timestamp is not None else datetime.now().timestamp()
                )
                cooldown_until = self._active_cooldown(conn, leader_id, now)
                if cooldown_until:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "create",
                        payload,
                        TeamMutationResult(
                            "cooldown_active",
                            cooldown_until=cooldown_until,
                            cooldown_seconds=self._cooldown_seconds(cooldown_until, now),
                            **base,
                        ),
                    )
                if conn.execute(
                    "SELECT 1 FROM teams WHERE user_id=%s", (team_id,)
                ).fetchone() is not None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "create",
                        payload,
                        TeamMutationResult("team_exists", **base),
                    )
                conn.execute(
                    "INSERT INTO teams "
                    "(user_id,team_id,team_name,group_id,leader,members,create_time,"
                    "max_members,description,version) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0)",
                    (
                        team_id,
                        team_id,
                        str(team_name),
                        str(group_id),
                        leader_id,
                        json.dumps([leader_id]),
                        str(created_at),
                        4,
                        "",
                    ),
                )
                result = TeamMutationResult(
                    "applied", member_count=1, max_members=4, version=0, **base
                )
                return self._finish_mutation(conn, operation_id, "create", payload, result)
            except Exception:
                conn.rollback()
                raise

    def invite(
        self,
        operation_id,
        invite_id,
        team_id,
        inviter_id,
        invitee_id,
        group_id,
        expires_at,
        now_timestamp=None,
    ) -> TeamMutationResult:
        operation_id = str(operation_id).strip()
        invite_id, team_id = str(invite_id), str(team_id)
        inviter_id, invitee_id = str(inviter_id), str(invitee_id)
        payload = self._intent(
            "invite",
            invite_id=invite_id,
            team_id=team_id,
            inviter_id=inviter_id,
            invitee_id=invitee_id,
            group_id=str(group_id),
        )
        if not operation_id:
            raise ValueError("operation_id is required")
        now = float(now_timestamp if now_timestamp is not None else float(expires_at) - 60)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,result_status,team_id,result_json "
                    "FROM dungeon_team_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return self._replay_mutation(old, payload)
                base = dict(
                    team_id=team_id,
                    invite_id=invite_id,
                    expires_at=float(expires_at),
                    target_id=invitee_id,
                    group_id=str(group_id),
                )
                if not str(group_id):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "invite",
                        payload,
                        TeamMutationResult("group_required", **base),
                    )
                if not invitee_id:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "invite",
                        payload,
                        TeamMutationResult("target_missing", **base),
                    )
                if conn.table_exists("user_xiuxian") and conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (invitee_id,)
                ).fetchone() is None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "invite",
                        payload,
                        TeamMutationResult("user_missing", **base),
                    )
                team = conn.execute(
                    "SELECT team_name,leader,members,max_members,version FROM teams WHERE user_id=%s",
                    (team_id,),
                ).fetchone()
                if team is None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "invite",
                        payload,
                        TeamMutationResult("team_disbanded", **base),
                    )
                members, maximum = self._members(team[2]), max(int(team[3] or 4), 1)
                base.update(
                    team_name=str(team[0] or ""),
                    leader_id=str(team[1] or ""),
                    member_count=len(members),
                    max_members=maximum,
                    version=int(team[4] or 0),
                )
                if str(team[1]) != inviter_id:
                    return self._finish_mutation(
                        conn, operation_id, "invite", payload, TeamMutationResult("actor_not_leader", **base)
                    )
                if len(members) >= maximum:
                    return self._finish_mutation(
                        conn, operation_id, "invite", payload, TeamMutationResult("team_full", **base)
                    )
                if self._user_team(conn, invitee_id):
                    return self._finish_mutation(
                        conn, operation_id, "invite", payload, TeamMutationResult("user_has_team", **base)
                    )
                if self._team_has_active_session(conn, members) or self._active_session(
                    conn, invitee_id
                ):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "invite",
                        payload,
                        TeamMutationResult("session_active", **base),
                    )
                cooldown_until = self._active_cooldown(conn, invitee_id, now)
                if cooldown_until:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "invite",
                        payload,
                        TeamMutationResult(
                            "cooldown_active",
                            cooldown_until=cooldown_until,
                            cooldown_seconds=self._cooldown_seconds(cooldown_until, now),
                            **base,
                        ),
                    )
                conn.execute(
                    "UPDATE dungeon_team_invites SET status='expired',consumed_at=CURRENT_TIMESTAMP "
                    "WHERE invitee_id=%s AND status='pending' AND expires_at<=%s",
                    (invitee_id, now),
                )
                pending = conn.execute(
                    "SELECT invite_id FROM dungeon_team_invites "
                    "WHERE invitee_id=%s AND status='pending' AND expires_at>%s LIMIT 1",
                    (invitee_id, now),
                ).fetchone()
                if pending is not None:
                    status = "duplicate" if str(pending[0]) == invite_id else "invite_pending"
                    result = TeamMutationResult(status, **base)
                    return self._finish_mutation(conn, operation_id, "invite", payload, result)
                if conn.execute(
                    "SELECT 1 FROM dungeon_team_invites WHERE invite_id=%s", (invite_id,)
                ).fetchone() is not None:
                    return self._finish_mutation(
                        conn, operation_id, "invite", payload, TeamMutationResult("invite_id_conflict", **base)
                    )
                conn.execute(
                    "INSERT INTO dungeon_team_invites "
                    "(invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,"
                    "consumed_at,status,created_at,resolved_operation_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,NULL,'pending',%s,NULL)",
                    (
                        invite_id,
                        team_id,
                        inviter_id,
                        invitee_id,
                        str(group_id),
                        float(expires_at),
                        now,
                    ),
                )
                return self._finish_mutation(
                    conn, operation_id, "invite", payload, TeamMutationResult("applied", **base)
                )
            except Exception:
                conn.rollback()
                raise

    def record_invite(
        self, invite_id, team_id, inviter_id, invitee_id, group_id, expires_at
    ) -> TeamMutationResult:
        """Compatibility wrapper for handlers that have not supplied a message operation yet."""
        return self.invite(
            f"dungeon-team-invite:{invite_id}",
            invite_id,
            team_id,
            inviter_id,
            invitee_id,
            group_id,
            expires_at,
            float(expires_at) - 60,
        )

    @staticmethod
    def _invite_from_row(row) -> TeamInviteSnapshot:
        return TeamInviteSnapshot(
            invite_id=str(row[0]),
            team_id=str(row[1]),
            inviter_id=str(row[2]),
            invitee_id=str(row[3]),
            group_id=str(row[4]),
            expires_at=float(row[5]),
            created_at=float(row[6] or float(row[5]) - 60),
            status=str(row[7] or "pending"),
        )

    def pending_invite(self, user_id: str, now_timestamp: float) -> TeamInviteSnapshot | None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,"
                    "created_at,status FROM dungeon_team_invites "
                    "WHERE invitee_id=%s AND status='pending' AND consumed_at IS NULL "
                    "AND expires_at>%s ORDER BY created_at DESC LIMIT 1",
                    (str(user_id), float(now_timestamp)),
                ).fetchone()
                conn.commit()
                return None if row is None else self._invite_from_row(row)
            except Exception:
                conn.rollback()
                raise

    def invite_by_id(self, invite_id: str) -> TeamInviteSnapshot | None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,"
                    "created_at,status FROM dungeon_team_invites WHERE invite_id=%s",
                    (str(invite_id),),
                ).fetchone()
                conn.commit()
                return None if row is None else self._invite_from_row(row)
            except Exception:
                conn.rollback()
                raise

    def list_pending_invites(self, now_timestamp: float) -> tuple[TeamInviteSnapshot, ...]:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                rows = conn.execute(
                    "SELECT invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,"
                    "created_at,status FROM dungeon_team_invites "
                    "WHERE status='pending' AND consumed_at IS NULL AND expires_at>%s "
                    "ORDER BY created_at",
                    (float(now_timestamp),),
                ).fetchall()
                conn.commit()
                return tuple(self._invite_from_row(row) for row in rows)
            except Exception:
                conn.rollback()
                raise

    def _resolve_invite(
        self,
        *,
        action: str,
        operation_id: str,
        invite_id: str,
        user_id: str,
        now_timestamp: float,
        group_id: str = "",
    ) -> TeamMutationResult:
        operation_id, invite_id, user_id = (
            str(operation_id).strip(),
            str(invite_id),
            str(user_id),
        )
        payload_values = {"invite_id": invite_id, "user_id": user_id}
        if group_id:
            payload_values["group_id"] = str(group_id)
        payload = self._intent(action, **payload_values)
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,result_status,team_id,result_json "
                    "FROM dungeon_team_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return self._replay_mutation(old, payload)
                invite = conn.execute(
                    "SELECT team_id,inviter_id,invitee_id,group_id,expires_at,status "
                    "FROM dungeon_team_invites WHERE invite_id=%s",
                    (invite_id,),
                ).fetchone()
                if invite is None or (
                    action != "expire" and str(invite[2]) != user_id
                ):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        action,
                        payload,
                        TeamMutationResult(
                            "invite_invalid", invite_id=invite_id, target_id=user_id
                        ),
                    )
                base = dict(
                    team_id=str(invite[0]),
                    leader_id=str(invite[1]),
                    invite_id=invite_id,
                    expires_at=float(invite[4]),
                    target_id=str(invite[2]),
                    group_id=str(invite[3]),
                )
                if group_id and str(invite[3]) != str(group_id):
                    return self._finish_mutation(
                        conn, operation_id, action, payload, TeamMutationResult("wrong_group", **base)
                    )
                if str(invite[5]) != "pending":
                    return self._finish_mutation(
                        conn, operation_id, action, payload, TeamMutationResult("invite_invalid", **base)
                    )
                if action == "expire" and float(now_timestamp) < float(invite[4]):
                    return self._finish_mutation(
                        conn, operation_id, action, payload, TeamMutationResult("not_expired", **base)
                    )
                status = "expired" if action == "expire" else "rejected"
                cursor = conn.execute(
                    "UPDATE dungeon_team_invites SET status=%s,consumed_at=CURRENT_TIMESTAMP,"
                    "resolved_operation_id=%s WHERE invite_id=%s AND status='pending'",
                    (status, operation_id, invite_id),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    return TeamMutationResult("state_changed", **base)
                return self._finish_mutation(
                    conn, operation_id, action, payload, TeamMutationResult("applied", **base)
                )
            except Exception:
                conn.rollback()
                raise

    def reject(
        self,
        operation_id: str,
        invite_id: str,
        user_id: str,
        group_id: str = "",
        now_timestamp: float = 0,
    ) -> TeamMutationResult:
        return self._resolve_invite(
            action="reject",
            operation_id=operation_id,
            invite_id=invite_id,
            user_id=user_id,
            group_id=group_id,
            now_timestamp=now_timestamp,
        )

    def expire(
        self,
        operation_id: str,
        invite_id: str,
        now_timestamp: float,
    ) -> TeamMutationResult:
        return self._resolve_invite(
            action="expire",
            operation_id=operation_id,
            invite_id=invite_id,
            user_id="",
            now_timestamp=now_timestamp,
        )

    def join(
        self,
        operation_id,
        invite_id,
        team_id,
        inviter_id,
        user_id,
        group_id,
        now_timestamp,
    ) -> TeamMutationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        invite_id, team_id, inviter_id = str(invite_id), str(team_id), str(inviter_id)
        payload = self._intent(
            "join",
            invite_id=invite_id,
            team_id=team_id,
            inviter_id=inviter_id,
            user_id=user_id,
            group_id=str(group_id),
        )
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,result_status,team_id,result_json "
                    "FROM dungeon_team_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return self._replay_mutation(old, payload)
                invite = conn.execute(
                    "SELECT team_id,inviter_id,invitee_id,group_id,expires_at,status,consumed_at "
                    "FROM dungeon_team_invites WHERE invite_id=%s",
                    (invite_id,),
                ).fetchone()
                expected_invite = (team_id, inviter_id, user_id, str(group_id))
                if (
                    invite is None
                    or tuple(map(str, invite[:4])) != expected_invite
                    or str(invite[5]) != "pending"
                    or invite[6] is not None
                    or float(invite[4]) <= float(now_timestamp)
                ):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "join",
                        payload,
                        TeamMutationResult(
                            "invite_invalid",
                            team_id=team_id,
                            invite_id=invite_id,
                            target_id=user_id,
                            group_id=str(group_id),
                        ),
                    )
                if conn.table_exists("user_xiuxian") and conn.execute(
                    "SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "join",
                        payload,
                        TeamMutationResult(
                            "user_missing",
                            team_id=team_id,
                            invite_id=invite_id,
                            target_id=user_id,
                            group_id=str(group_id),
                        ),
                    )
                if self._user_team(conn, user_id):
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "join",
                        payload,
                        TeamMutationResult(
                            "user_has_team",
                            team_id=team_id,
                            invite_id=invite_id,
                            target_id=user_id,
                            group_id=str(group_id),
                        ),
                    )
                team = conn.execute(
                    "SELECT team_name,leader,members,max_members,version FROM teams WHERE user_id=%s",
                    (team_id,),
                ).fetchone()
                if team is None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "join",
                        payload,
                        TeamMutationResult(
                            "team_disbanded",
                            team_id=team_id,
                            invite_id=invite_id,
                            target_id=user_id,
                            group_id=str(group_id),
                        ),
                    )
                members, maximum = self._members(team[2]), max(int(team[3] or 4), 1)
                base = dict(
                    team_id=team_id,
                    team_name=str(team[0] or ""),
                    leader_id=str(team[1] or ""),
                    member_count=len(members),
                    max_members=maximum,
                    invite_id=invite_id,
                    expires_at=float(invite[4]),
                    version=int(team[4] or 0),
                    target_id=user_id,
                    group_id=str(group_id),
                )
                if str(team[1]) != inviter_id:
                    return self._finish_mutation(
                        conn, operation_id, "join", payload, TeamMutationResult("invite_invalid", **base)
                    )
                if len(members) >= maximum:
                    return self._finish_mutation(
                        conn, operation_id, "join", payload, TeamMutationResult("team_full", **base)
                    )
                if self._active_session(conn, user_id) or self._team_has_active_session(conn, members):
                    return self._finish_mutation(
                        conn, operation_id, "join", payload, TeamMutationResult("session_active", **base)
                    )
                cooldown_until = self._active_cooldown(conn, user_id, float(now_timestamp))
                if cooldown_until:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "join",
                        payload,
                        TeamMutationResult(
                            "cooldown_active",
                            cooldown_until=cooldown_until,
                            cooldown_seconds=self._cooldown_seconds(
                                cooldown_until, float(now_timestamp)
                            ),
                            **base,
                        ),
                    )
                members.append(user_id)
                cursor = conn.execute(
                    "UPDATE teams SET members=%s,version=version+1 "
                    "WHERE user_id=%s AND version=%s",
                    (json.dumps(members), team_id, int(team[4] or 0)),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    return TeamMutationResult("state_changed", **base)
                conn.execute(
                    "INSERT INTO team_cd (user_id,join_cd_until,had_first_join) VALUES (%s,'',1) "
                    "ON CONFLICT(user_id) DO UPDATE SET had_first_join=1",
                    (user_id,),
                )
                conn.execute(
                    "UPDATE dungeon_team_invites SET consumed_at=CURRENT_TIMESTAMP,status='joined',"
                    "resolved_operation_id=%s WHERE invite_id=%s AND status='pending'",
                    (operation_id, invite_id),
                )
                result = TeamMutationResult(
                    "applied",
                    team_id=team_id,
                    team_name=str(team[0] or ""),
                    leader_id=str(team[1] or ""),
                    member_count=len(members),
                    max_members=maximum,
                    invite_id=invite_id,
                    expires_at=float(invite[4]),
                    version=int(team[4] or 0) + 1,
                    target_id=user_id,
                    group_id=str(group_id),
                )
                return self._finish_mutation(conn, operation_id, "join", payload, result)
            except Exception:
                conn.rollback()
                raise

    def transfer(
        self,
        operation_id: str,
        actor_id: str,
        target_id: str,
        expected: TeamStateSnapshot,
    ) -> TeamMutationResult:
        operation_id = str(operation_id).strip()
        actor_id, target_id = str(actor_id), str(target_id)
        payload = self._intent(
            "transfer",
            team_id=str(expected.team_id),
            actor_id=actor_id,
            target_id=target_id,
        )
        if not operation_id:
            raise ValueError("operation_id is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute(
                    "SELECT payload,result_status,team_id,result_json "
                    "FROM dungeon_team_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    return self._replay_mutation(old, payload)
                row = conn.execute(
                    "SELECT team_name,leader,members,version FROM teams WHERE user_id=%s",
                    (expected.team_id,),
                ).fetchone()
                if row is None:
                    return self._finish_mutation(
                        conn,
                        operation_id,
                        "transfer",
                        payload,
                        TeamMutationResult("team_missing", expected.team_id, expected.team_name),
                    )
                current = self._snapshot_from_row(conn, expected.team_id, row)
                base = dict(
                    team_id=current.team_id,
                    team_name=current.team_name,
                    leader_id=current.leader_id,
                    member_count=len(current.members),
                    max_members=0,
                    version=current.version,
                    target_id=target_id,
                )
                if not self._team_state_matches(current, expected):
                    return self._finish_mutation(
                        conn, operation_id, "transfer", payload, TeamMutationResult("state_changed", **base)
                    )
                if self._team_has_active_session(conn, current.members):
                    return self._finish_mutation(
                        conn, operation_id, "transfer", payload, TeamMutationResult("session_active", **base)
                    )
                if actor_id != current.leader_id:
                    return self._finish_mutation(
                        conn, operation_id, "transfer", payload, TeamMutationResult("actor_not_leader", **base)
                    )
                if target_id == actor_id:
                    return self._finish_mutation(
                        conn, operation_id, "transfer", payload, TeamMutationResult("self_target", **base)
                    )
                if target_id not in current.members:
                    return self._finish_mutation(
                        conn, operation_id, "transfer", payload, TeamMutationResult("target_not_member", **base)
                    )
                cursor = conn.execute(
                    "UPDATE teams SET leader=%s,version=version+1 "
                    "WHERE user_id=%s AND version=%s",
                    (target_id, current.team_id, current.version),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    return TeamMutationResult("state_changed", **base)
                result = TeamMutationResult(
                    "applied",
                    team_id=current.team_id,
                    team_name=current.team_name,
                    leader_id=target_id,
                    member_count=len(current.members),
                    version=current.version + 1,
                    target_id=target_id,
                )
                return self._finish_mutation(conn, operation_id, "transfer", payload, result)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class TeamMemberView:
    user_id: str
    user_name: str
    is_leader: bool

@dataclass(frozen=True)
class TeamViewResult:
    status: str
    team_info: dict[str, Any] | None = None
    members: tuple[TeamMemberView, ...] = ()

@dataclass(frozen=True)
class TeamTransferResult:
    status: str
    target_user_id: str = ""
    target_user_name: str = ""

@dataclass(frozen=True)
class TeamLeaveResult:
    status: str
    team_name: str = ""
    new_leader_name: str = ""
    cooldown_hours: int = 0

@dataclass(frozen=True)
class TeamKickResult:
    status: str
    target_user_id: str = ""
    target_user_name: str = ""
    cooldown_hours: int = 0

@dataclass(frozen=True)
class TeamInviteResult:
    status: str
    target_user_id: str = ""
    target_user_name: str = ""
    inviter_name: str = ""
    group_id: str = ""
    remaining_seconds: int = 0

@dataclass(frozen=True)
class TeamInviteResponseResult:
    status: str
    team_name: str = ""
    leader_name: str = ""
    member_count: int = 0
    max_members: int = 0
    invite_group_id: str = ""

def build_team_view(team_info: dict[str, Any], lookup_user_name: Callable[[str], str]) -> TeamViewResult:
    members = []
    leader_id = str(team_info.get("leader", ""))
    for member_id in team_info.get("members", []):
        normalized_member_id = str(member_id)
        members.append(
            TeamMemberView(
                user_id=normalized_member_id,
                user_name=lookup_user_name(normalized_member_id),
                is_leader=normalized_member_id == leader_id,
            )
        )
    return TeamViewResult("ok", team_info=dict(team_info), members=tuple(members))

def build_team_view_message(result: TeamViewResult) -> str:
    if result.team_info is None:
        raise ValueError("team_info 不能为空")

    members_info = []
    for member in result.members:
        prefix = "👑" if member.is_leader else "👤"
        members_info.append(f"{prefix} {member.user_name}")

    members_str_formatted = "\n".join(members_info)
    team_info = result.team_info
    return (
        f"【队伍信息】\n"
        f"队伍名：{team_info['team_name']}\n"
        f"队伍ID：{team_info['team_id']}\n"
        f"创建时间：{team_info['create_time']}\n"
        f"成员：{len(team_info['members'])}/{team_info['max_members']}\n"
        f"{members_str_formatted}\n"
        f"操作：探索副本 / 离开队伍"
    )

def resolve_transfer_target(
    *,
    actor_user_id: str,
    team_info: dict[str, Any],
    at_target_user_id: str | None,
    arg_target_user_id: str | None,
    lookup_user_name: Callable[[str], str | None],
) -> TeamTransferResult:
    target_user_id = at_target_user_id or arg_target_user_id or ""
    if not target_user_id:
        return TeamTransferResult("target_not_found")
    if target_user_id == actor_user_id:
        return TeamTransferResult("self_target", target_user_id=target_user_id)
    if target_user_id not in team_info.get("members", []):
        return TeamTransferResult("target_not_member", target_user_id=target_user_id)

    target_user_name = lookup_user_name(target_user_id)
    if not target_user_name:
        return TeamTransferResult("target_info_missing", target_user_id=target_user_id)
    return TeamTransferResult(
        "ok",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
    )

def build_transfer_team_success_message(target_user_name: str) -> str:
    return f"👑 队长已成功转移给 {target_user_name}！"

def build_transfer_team_self_message() -> str:
    return "你已经是队长了，无需转移给自己。"

def build_transfer_team_not_member_message() -> str:
    return "只能将队长转移给当前队伍内的成员！"

def build_leave_team_result(
    *,
    team_info: dict[str, Any],
    leaver_user_id: str,
    success: bool,
    cooldown_hours: int,
    new_leader_name: str | None,
) -> TeamLeaveResult:
    if not success:
        return TeamLeaveResult("leave_failed")

    if leaver_user_id != str(team_info.get("leader", "")):
        return TeamLeaveResult(
            "member_left",
            team_name=str(team_info.get("team_name", "")),
            cooldown_hours=cooldown_hours,
        )

    if new_leader_name:
        return TeamLeaveResult(
            "leader_left_transferred",
            team_name=str(team_info.get("team_name", "")),
            new_leader_name=new_leader_name,
            cooldown_hours=cooldown_hours,
        )

    return TeamLeaveResult(
        "leader_left_disbanded",
        team_name=str(team_info.get("team_name", "")),
        cooldown_hours=cooldown_hours,
    )

def build_leave_team_message(result: TeamLeaveResult) -> str:
    if result.status == "leader_left_transferred":
        return (
            f"你已离开队伍【{result.team_name}】，队长已转让给{result.new_leader_name}。\n"
            f"你进入了{result.cooldown_hours}小时组队冷却。"
        )
    if result.status == "leader_left_disbanded":
        return (
            f"你已离开队伍【{result.team_name}】，队伍已解散。\n"
            f"你进入了{result.cooldown_hours}小时组队冷却。"
        )
    if result.status == "member_left":
        return (
            f"你已离开队伍【{result.team_name}】。\n"
            f"你进入了{result.cooldown_hours}小时组队冷却。"
        )
    return "离开队伍失败！"

def resolve_kick_target(
    *,
    actor_user_id: str,
    team_info: dict[str, Any],
    at_target_user_id: str | None,
    arg_target_user_id: str | None,
    lookup_user_name: Callable[[str], str | None],
) -> TeamKickResult:
    target_user_id = at_target_user_id or arg_target_user_id or ""
    if not target_user_id:
        return TeamKickResult("target_not_found")
    if target_user_id == actor_user_id:
        return TeamKickResult("self_target", target_user_id=target_user_id)
    if target_user_id not in team_info.get("members", []):
        return TeamKickResult("target_not_member", target_user_id=target_user_id)

    target_user_name = lookup_user_name(target_user_id)
    if not target_user_name:
        return TeamKickResult("target_info_missing", target_user_id=target_user_id)

    return TeamKickResult(
        "ok",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
    )

def build_kick_team_result(
    *,
    target_user_id: str,
    target_user_name: str,
    success: bool,
    cooldown_hours: int,
) -> TeamKickResult:
    if not success:
        return TeamKickResult("kick_failed", target_user_id=target_user_id)
    return TeamKickResult(
        "kicked",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
        cooldown_hours=cooldown_hours,
    )

def build_kick_team_message(result: TeamKickResult) -> str:
    if result.status == "kicked":
        return (
            f"已将成员{result.target_user_name}踢出队伍。\n"
            f"对方进入{result.cooldown_hours}小时组队冷却。"
        )
    return "踢出成员失败！"

def resolve_team_invite(
    *,
    target_user_id: str | None,
    target_user_name: str | None,
    cooldown_seconds: int,
    target_team_id: str | None,
    pending_inviter_name: str | None,
    pending_remaining_seconds: int,
) -> TeamInviteResult:
    if not target_user_id:
        return TeamInviteResult("target_not_found")
    if not target_user_name:
        return TeamInviteResult("target_info_missing", target_user_id=target_user_id)
    if cooldown_seconds > 0:
        return TeamInviteResult(
            "target_in_cooldown",
            target_user_id=target_user_id,
            target_user_name=target_user_name,
            remaining_seconds=cooldown_seconds,
        )
    if target_team_id:
        return TeamInviteResult(
            "target_has_team",
            target_user_id=target_user_id,
            target_user_name=target_user_name,
        )
    if pending_inviter_name:
        return TeamInviteResult(
            "target_has_pending_invite",
            target_user_id=target_user_id,
            target_user_name=target_user_name,
            inviter_name=pending_inviter_name,
            remaining_seconds=max(0, pending_remaining_seconds),
        )
    return TeamInviteResult(
        "ready",
        target_user_id=target_user_id,
        target_user_name=target_user_name,
    )

def build_team_invite_message(result: TeamInviteResult, format_duration: Callable[[int], str]) -> str:
    if result.status == "target_not_found":
        return "未找到指定的用户，请检查道号或艾特是否正确！"
    if result.status == "target_in_cooldown":
        return (
            f"{result.target_user_name}当前处于组队冷却中"
            f"（剩余{format_duration(result.remaining_seconds)}），不可被邀请。"
        )
    if result.status == "target_has_team":
        return f"{result.target_user_name}已有队伍！"
    if result.status == "target_has_pending_invite":
        return (
            f"对方已有来自{result.inviter_name}的组队邀请"
            f"（剩余{result.remaining_seconds}秒），请稍后再试！"
        )
    if result.status == "ready":
        return f"📨 已向{result.target_user_name}发送组队邀请，等待对方回应..."
    return "目标用户信息异常，无法发送邀请！"

def build_team_invite_private_message(*, group_id: str, inviter_name: str) -> str:
    return (
        f"你在群{group_id}收到了来自{inviter_name}的组队邀请，"
        "请在1分钟内回复【同意组队】或【拒绝组队】。"
    )

def resolve_invite_response(
    *,
    has_invite: bool,
    invite_group_id: str | None,
    current_group_id: str | None,
    team_exists: bool,
    user_has_team: bool,
    member_count: int,
    max_members: int,
) -> TeamInviteResponseResult:
    if not has_invite:
        return TeamInviteResponseResult("no_invite")
    normalized_invite_group = str(invite_group_id or "")
    if current_group_id is not None and str(current_group_id) != normalized_invite_group:
        return TeamInviteResponseResult(
            "wrong_group",
            invite_group_id=normalized_invite_group,
        )
    if not team_exists:
        return TeamInviteResponseResult("team_disbanded")
    if user_has_team:
        return TeamInviteResponseResult("user_has_team")
    if member_count >= max_members:
        return TeamInviteResponseResult("team_full")
    return TeamInviteResponseResult("ready")

def build_invite_response_message(result: TeamInviteResponseResult) -> str:
    if result.status == "no_invite":
        return "没有待处理的组队邀请！"
    if result.status == "wrong_group":
        return f"此邀请是在群{result.invite_group_id}发出的，请在该群或私聊中进行操作。"
    if result.status == "team_disbanded":
        return "该队伍已解散！"
    if result.status == "user_has_team":
        return "你已经在一个队伍中了，无法接受邀请！"
    if result.status == "team_full":
        return "该队伍已满员！"
    if result.status == "rejected":
        return "已拒绝组队邀请。"
    if result.status == "join_failed":
        return "加入队伍失败！"
    if result.status == "joined":
        return (
            f"✅ 你已成功加入队伍【{result.team_name}】！\n"
            f"👑 队长：{result.leader_name}\n"
            f"👥 当前成员：{result.member_count}/{result.max_members}"
        )
    raise ValueError(f"无法展示邀请响应状态: {result.status}")

class DungeonTeamExitService(DungeonTeamTransactionService):
    """Transactional owner for member leave, kick, and team disband flows."""

@dataclass(frozen=True)
class DungeonSessionResult:
    status: str
    dungeon_status: str = ""

class DungeonSessionService:
    """Atomically enter or leave the current dungeon session."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    def enter(self, operation_id, user_id, expected, dungeon) -> DungeonSessionResult:
        return self._transition(operation_id, user_id, expected, dungeon, "enter")

    def exit(self, operation_id, user_id, expected, dungeon) -> DungeonSessionResult:
        return self._transition(operation_id, user_id, expected, dungeon, "exit")

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_session_operations ("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL,dungeon_status TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    def operation_result(
        self, operation_id, user_id, action
    ) -> DungeonSessionResult | None:
        """Replay a recorded transition before reading the current dungeon snapshot."""

        operation_id = str(operation_id).strip()
        user_id, action = str(user_id), str(action)
        if not operation_id or action not in {"enter", "exit"}:
            raise ValueError("valid operation is required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,dungeon_status "
                    "FROM dungeon_session_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if previous is None:
            return None
        try:
            payload = json.loads(str(previous[0]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return DungeonSessionResult("state_changed", str(previous[2]))
        if (
            not isinstance(payload, dict)
            or str(payload.get("user_id", "")) != user_id
            or str(payload.get("action", "")) != action
        ):
            return DungeonSessionResult("state_changed", str(previous[2]))
        result_status = str(previous[1])
        return DungeonSessionResult(
            "duplicate" if result_status == "applied" else result_status,
            str(previous[2]),
        )

    def _transition(self, operation_id, user_id, expected, dungeon, action) -> DungeonSessionResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected = {str(k): expected[k] for k in expected}
        dungeon = {str(k): dungeon[k] for k in dungeon}
        if not operation_id or action not in {"enter", "exit"}:
            raise ValueError("valid operation is required")
        payload = json.dumps({"user_id": user_id, "dungeon": dungeon, "action": action}, ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute("SELECT payload,result_status,dungeon_status FROM dungeon_session_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return DungeonSessionResult("state_changed", str(previous[2]))
                    result_status = str(previous[1])
                    return DungeonSessionResult(
                        "duplicate" if result_status == "applied" else result_status,
                        str(previous[2]),
                    )

                def record_result(result_status: str, dungeon_status: str = ""):
                    conn.execute(
                        "INSERT INTO dungeon_session_operations VALUES "
                        "(%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                        (operation_id, payload, result_status, dungeon_status),
                    )
                    conn.commit()
                    return DungeonSessionResult(result_status, dungeon_status)

                columns = [
                    "dungeon_id",
                    "dungeon_status",
                    "current_layer",
                    "total_layers",
                    "last_reset_date",
                ]
                available = set(conn.column_names("player_dungeon_status"))
                for optional in ("reset_generation", "reset_operation_id"):
                    if optional in available:
                        if optional not in expected:
                            return record_result("state_changed")
                        columns.append(optional)
                row = conn.execute(
                    "SELECT " + ",".join(columns)
                    + " FROM player_dungeon_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None:
                    return record_result("state_changed")
                current = dict(zip(columns, row))
                normalized_expected = {key: expected[key] for key in columns}
                for field in ("dungeon_id", "dungeon_status", "last_reset_date", "reset_operation_id"):
                    if field in current:
                        current[field] = str(current[field] or "")
                        normalized_expected[field] = str(normalized_expected[field] or "")
                for field in ("current_layer", "total_layers", "reset_generation"):
                    if field in current:
                        current[field] = int(current[field] or 0)
                        normalized_expected[field] = int(normalized_expected[field] or 0)
                if current != normalized_expected or current["dungeon_id"] != str(dungeon["dungeon_id"]) or current["last_reset_date"] != str(dungeon["date"]):
                    return record_result("state_changed", current["dungeon_status"])
                if current["dungeon_status"] == "completed":
                    return record_result("completed", "completed")
                if action == "exit" and current["dungeon_status"] != "exploring":
                    return record_result("not_exploring", current["dungeon_status"])
                new_status = "exploring" if action == "enter" else "exited"
                conn.execute("UPDATE player_dungeon_status SET dungeon_status=%s WHERE user_id=%s", (new_status, user_id))
                conn.execute("INSERT INTO dungeon_session_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, "applied", new_status))
                conn.commit()
                return DungeonSessionResult("applied", new_status)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class DungeonPurchaseResult:
    status: str
    quantity: int = 0
    cost: int = 0
    stone: int = 0
    inventory: int = 0
    response: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DungeonPurchaseService:
    """Exchange spirit stones for a dungeon-shop item atomically."""

    _REJECTION_RESPONSES = {
        "stone_insufficient": "灵石不足，无法兑换。",
        "inventory_full": "背包中该物品数量已达上限。",
        "state_changed": "兑换状态已变化，请稍后重试。",
        "user_missing": "未找到道友数据，兑换失败。",
    }

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id: str, item_id: int, quantity: int, bind_flag: int) -> str:
        return json.dumps(
            [user_id, item_id, quantity, bind_flag],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @classmethod
    def _response(cls, status: str, item_name: str, quantity: int, cost: int) -> str:
        if status == "applied":
            return f"成功兑换{item_name}×{quantity}，消耗{number_to(cost)}灵石。"
        return cls._REJECTION_RESPONSES.get(status, "兑换失败。")

    @classmethod
    def _legacy_payload(cls, payload: str) -> tuple[str, str]:
        """Return the immutable request identity and legacy item name."""
        try:
            values = json.loads(payload)
        except (TypeError, ValueError):
            return str(payload), ""
        if not isinstance(values, list):
            return str(payload), ""
        if len(values) == 8:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[4]),
                        int(bool(values[7])),
                    ),
                    str(values[2]),
                )
            except (TypeError, ValueError):
                return str(payload), ""
        if len(values) == 5:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[2]),
                        int(bool(values[4])),
                    ),
                    "",
                )
            except (TypeError, ValueError):
                pass
        if len(values) == 4:
            try:
                return (
                    cls._payload(
                        str(values[0]),
                        int(values[1]),
                        int(values[2]),
                        int(bool(values[3])),
                    ),
                    "",
                )
            except (TypeError, ValueError):
                pass
        return str(payload), ""

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_purchase_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "result_status TEXT NOT NULL DEFAULT 'applied',"
            "quantity INTEGER NOT NULL DEFAULT 0,cost INTEGER NOT NULL DEFAULT 0,"
            "stone INTEGER NOT NULL DEFAULT 0,inventory INTEGER NOT NULL DEFAULT 0,"
            "response TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        for name, definition in (
            ("result_status", "TEXT NOT NULL DEFAULT 'applied'"),
            ("response", "TEXT NOT NULL DEFAULT ''"),
        ):
            if not conn.column_exists("dungeon_purchase_operations", name):
                conn.execute(
                    f"ALTER TABLE dungeon_purchase_operations ADD COLUMN {name} {definition}"
                )

        rows = conn.execute(
            "SELECT operation_id,payload,result_status,quantity,cost,response "
            "FROM dungeon_purchase_operations"
        ).fetchall()
        for row in rows:
            payload, item_name = cls._legacy_payload(str(row[1]))
            result_status = str(row[2] or "applied")
            response = str(row[5] or "")
            if not response:
                response = cls._response(
                    result_status, item_name or "该物品", int(row[3]), int(row[4])
                )
            if payload != str(row[1]) or result_status != str(row[2]) or response != str(row[5]):
                conn.execute(
                    "UPDATE dungeon_purchase_operations "
                    "SET payload=%s,result_status=%s,response=%s WHERE operation_id=%s",
                    (payload, result_status, response, str(row[0])),
                )

    @staticmethod
    def _stored_result(row) -> DungeonPurchaseResult:
        result_status = str(row[1])
        return DungeonPurchaseResult(
            "duplicate" if result_status == "applied" else result_status,
            int(row[2]),
            int(row[3]),
            int(row[4]),
            int(row[5]),
            str(row[6]),
        )

    @staticmethod
    def _record(conn, operation_id: str, payload: str, result: DungeonPurchaseResult) -> None:
        conn.execute(
            "INSERT INTO dungeon_purchase_operations("
            "operation_id,payload,result_status,quantity,cost,stone,inventory,response) "
            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                operation_id,
                payload,
                result.status,
                result.quantity,
                result.cost,
                result.stone,
                result.inventory,
                result.response,
            ),
        )

    def operation_result(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        bind_flag=1,
    ) -> DungeonPurchaseResult | None:
        """Replay a fixed purchase before consulting mutable shop metadata."""

        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id, quantity, bind_flag = map(int, (item_id, quantity, bind_flag))
        if (
            not operation_id
            or not user_id
            or item_id <= 0
            or quantity <= 0
            or bind_flag not in {0, 1}
        ):
            raise ValueError("valid purchase identity is required")
        payload = self._payload(user_id, item_id, quantity, bind_flag)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if previous is None:
            return None
        if str(previous[0]) != payload:
            return DungeonPurchaseResult(
                "state_changed",
                quantity=quantity,
                response=self._REJECTION_RESPONSES["state_changed"],
            )
        return self._stored_result(previous)

    def purchase(
        self,
        operation_id,
        user_id,
        item_id,
        item_name,
        item_type,
        quantity,
        unit_cost,
        expected_stone,
        max_goods,
        bind_flag=1,
    ) -> DungeonPurchaseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id, quantity, unit_cost, expected_stone, max_goods = map(
            int, (item_id, quantity, unit_cost, expected_stone, max_goods)
        )
        item_name, item_type = str(item_name).strip(), str(item_type).strip()
        bind_flag = int(bind_flag)
        if (
            not operation_id
            or not user_id
            or not item_name
            or not item_type
            or item_id <= 0
            or quantity <= 0
            or unit_cost <= 0
            or expected_stone < 0
            or max_goods < 0
            or bind_flag not in {0, 1}
        ):
            raise ValueError("valid purchase is required")
        payload = self._payload(user_id, item_id, quantity, bind_flag)
        cost = quantity * unit_cost

        def rejected(status: str, stone=0, inventory=0) -> DungeonPurchaseResult:
            return DungeonPurchaseResult(
                status,
                quantity,
                cost,
                int(stone),
                int(inventory),
                self._response(status, item_name, quantity, cost),
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_status,quantity,cost,stone,inventory,response "
                    "FROM dungeon_purchase_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.commit()
                    if str(previous[0]) != payload:
                        return rejected("state_changed")
                    return self._stored_result(previous)

                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    result = rejected("user_missing")
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                stone = int(user[0])
                if stone != expected_stone:
                    result = rejected("state_changed", stone)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                if stone < cost:
                    result = rejected("stone_insufficient", stone)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) "
                    "FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                inventory, bound = (
                    (int(item[0]), int(item[1])) if item is not None else (0, 0)
                )
                if inventory < 0 or bound < 0 or bound > inventory:
                    result = rejected("state_changed", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result
                if inventory + quantity > max_goods:
                    result = rejected("inventory_full", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                new_stone = stone - cost
                new_inventory = inventory + quantity
                new_bound = bound + (quantity if bind_flag else 0)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET stone=%s "
                    "WHERE user_id=%s AND COALESCE(stone,0)=%s",
                    (new_stone, user_id, expected_stone),
                )
                if changed.rowcount != 1:
                    result = rejected("state_changed", stone, inventory)
                    self._record(conn, operation_id, payload, result)
                    conn.commit()
                    return result

                now = datetime.now()
                conn.execute(
                    "INSERT INTO back("
                    "user_id,goods_id,goods_name,goods_type,goods_num,"
                    "create_time,update_time,bind_num) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=EXCLUDED.goods_name,goods_type=EXCLUDED.goods_type,"
                    "goods_num=back.goods_num+EXCLUDED.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                    "update_time=EXCLUDED.update_time",
                    (
                        user_id,
                        item_id,
                        item_name,
                        item_type,
                        quantity,
                        now,
                        now,
                        quantity if bind_flag else 0,
                    ),
                )
                final_item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) "
                    "FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if final_item is None or (int(final_item[0]), int(final_item[1])) != (
                    new_inventory,
                    new_bound,
                ):
                    raise RuntimeError("dungeon purchase inventory invariant failed")

                result = DungeonPurchaseResult(
                    "applied",
                    quantity,
                    cost,
                    new_stone,
                    new_inventory,
                    self._response("applied", item_name, quantity, cost),
                )
                self._record(conn, operation_id, payload, result)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

_STATUS_INTEGER_FIELDS = {"current_layer", "total_layers", "reset_generation"}
_STATUS_FIELDS = (
    "dungeon_id",
    "dungeon_name",
    "dungeon_status",
    "current_layer",
    "total_layers",
    "last_reset_date",
    "reset_generation",
    "reset_operation_id",
)

@dataclass(frozen=True)
class DungeonExploreOperationResult:
    status: str
    phase: str = ""
    result_status: str = ""
    response: dict[str, Any] | None = None
    plan: dict[str, Any] | None = None
    current_layer: int = 0
    dungeon_status: str = ""

    @property
    def completed(self) -> bool:
        return self.phase == "completed"

class DungeonExploreOperationService:
    """Persist one resolved exploration and settle every business write once."""

    TABLE = "dungeon_explore_operations"

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
    def _identity(user_id: str) -> str:
        return json.dumps(
            {"action": "explore", "user_id": str(user_id)},
            ensure_ascii=True,
            sort_keys=True,
        )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load_json(value: Any, fallback: Any) -> Any:
        try:
            loaded = json.loads(str(value or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return fallback
        return loaded

    def _ensure_schema(self, conn) -> None:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.TABLE} ("
            "operation_id TEXT PRIMARY KEY,"
            "request_identity TEXT NOT NULL,"
            "phase TEXT NOT NULL,"
            "prepared_json TEXT NOT NULL DEFAULT '{}',"
            "result_status TEXT NOT NULL DEFAULT '',"
            "result_json TEXT NOT NULL DEFAULT '{}',"
            "current_layer INTEGER NOT NULL DEFAULT 0,"
            "dungeon_status TEXT NOT NULL DEFAULT '',"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        migrations = {
            "request_identity": "TEXT NOT NULL DEFAULT ''",
            "phase": "TEXT NOT NULL DEFAULT 'completed'",
            "prepared_json": "TEXT NOT NULL DEFAULT '{}'",
            "result_status": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "current_layer": "INTEGER NOT NULL DEFAULT 0",
            "dungeon_status": "TEXT NOT NULL DEFAULT ''",
            "updated_at": "TIMESTAMP",
        }
        for column, definition in migrations.items():
            if not conn.column_exists(self.TABLE, column):
                conn.execute(f"ALTER TABLE {self.TABLE} ADD COLUMN {column} {definition}")

    def _row_result(self, row, *, duplicate: bool) -> DungeonExploreOperationResult:
        phase = str(row[1] or "")
        result_status = str(row[3] or "")
        response = self._load_json(row[4], {})
        plan = self._load_json(row[2], {})
        status = "duplicate" if duplicate and phase == "completed" else phase
        if not duplicate and phase == "completed":
            status = result_status or "completed"
        return DungeonExploreOperationResult(
            status=status,
            phase=phase,
            result_status=result_status,
            response=response if isinstance(response, dict) else {},
            plan=plan if isinstance(plan, dict) else {},
            current_layer=int(row[5] or 0),
            dungeon_status=str(row[6] or ""),
        )

    def _select(self, conn, operation_id: str):
        return conn.execute(
            f"SELECT request_identity,phase,prepared_json,result_status,result_json,"
            f"current_layer,dungeon_status FROM {self.TABLE} WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    def replay(self, operation_id: str, user_id: str) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id is required")
        identity = self._identity(str(user_id))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if row is None:
            return DungeonExploreOperationResult("missing")
        if str(row[0]) != identity:
            return DungeonExploreOperationResult("operation_conflict")
        return self._row_result(row, duplicate=True)

    def complete_without_writes(
        self,
        operation_id: str,
        user_id: str,
        result_status: str,
        response: dict[str, Any],
        *,
        current_layer: int = 0,
        dungeon_status: str = "",
    ) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        result_status = str(result_status).strip() or "rejected"
        if not operation_id:
            raise ValueError("operation_id is required")
        identity = self._identity(user_id)
        response_json = self._json(dict(response))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                if row is not None:
                    conn.rollback()
                    if str(row[0]) != identity:
                        return DungeonExploreOperationResult("operation_conflict")
                    return self._row_result(row, duplicate=True)
                conn.execute(
                    f"INSERT INTO {self.TABLE} (operation_id,request_identity,phase,prepared_json,"
                    "result_status,result_json,current_layer,dungeon_status,updated_at) "
                    "VALUES (%s,%s,'completed','{}',%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                    (
                        operation_id,
                        identity,
                        result_status,
                        response_json,
                        int(current_layer),
                        str(dungeon_status),
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return DungeonExploreOperationResult(
            "applied",
            "completed",
            result_status,
            dict(response),
            {},
            int(current_layer),
            str(dungeon_status),
        )

    def resolve_rejection(
        self,
        operation_id: str,
        user_id: str,
        result_status: str,
        response: dict[str, Any],
        max_goods_num: int,
        *,
        current_layer: int = 0,
        dungeon_status: str = "",
    ) -> DungeonExploreOperationResult:
        """Persist this rejection, or finish an already prepared winning plan."""

        result = self.complete_without_writes(
            operation_id,
            user_id,
            result_status,
            response,
            current_layer=current_layer,
            dungeon_status=dungeon_status,
        )
        if result.phase == "prepared":
            return self.settle(operation_id, user_id, max_goods_num)
        return result

    def prepare(
        self,
        operation_id: str,
        user_id: str,
        plan: dict[str, Any],
    ) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id or not isinstance(plan, dict) or not plan:
            raise ValueError("operation_id and resolved plan are required")
        identity = self._identity(user_id)
        prepared_json = self._json(plan)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                if row is not None:
                    conn.rollback()
                    if str(row[0]) != identity:
                        return DungeonExploreOperationResult("operation_conflict")
                    return self._row_result(row, duplicate=True)
                conn.execute(
                    f"INSERT INTO {self.TABLE} (operation_id,request_identity,phase,prepared_json,"
                    "result_status,result_json,current_layer,dungeon_status,updated_at) "
                    "VALUES (%s,%s,'prepared',%s,'','{}',0,'',CURRENT_TIMESTAMP)",
                    (operation_id, identity, prepared_json),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return DungeonExploreOperationResult(
            "prepared", "prepared", "", {}, dict(plan), 0, ""
        )

    @staticmethod
    def _normalize_status(status: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in status.items():
            if key not in _STATUS_FIELDS:
                continue
            normalized[key] = int(value or 0) if key in _STATUS_INTEGER_FIELDS else str(value or "")
        return normalized

    @staticmethod
    def _members(value: Any) -> list[str]:
        if not isinstance(value, list):
            try:
                value = json.loads(str(value or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                value = []
        if not isinstance(value, list):
            return []
        return [str(member) for member in value if str(member).strip()]

    def _current_team(self, conn, user_id: str) -> dict[str, Any] | None:
        if conn.execute(
            "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name='teams'"
        ).fetchone() is None:
            return None
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA player_data.table_info(teams)").fetchall()
        }
        selected = ["user_id", "leader", "members"]
        if "version" in columns:
            selected.append("version")
        rows = conn.execute(
            "SELECT " + ",".join(selected) + " FROM player_data.teams"
        ).fetchall()
        for row in rows:
            members = self._members(row[2])
            if str(row[1]) == user_id or user_id in members:
                result = {
                    "team_id": str(row[0]),
                    "leader": str(row[1]),
                    "members": members,
                }
                if len(row) > 3:
                    result["version"] = int(row[3] or 0)
                return result
        return None

    def _complete_in_transaction(
        self,
        conn,
        operation_id: str,
        result_status: str,
        response: dict[str, Any],
        current_layer: int,
        dungeon_status: str,
    ) -> DungeonExploreOperationResult:
        conn.execute(
            f"UPDATE {self.TABLE} SET phase='completed',result_status=%s,result_json=%s,"
            "current_layer=%s,dungeon_status=%s,updated_at=CURRENT_TIMESTAMP "
            "WHERE operation_id=%s AND phase='prepared'",
            (
                str(result_status),
                self._json(response),
                int(current_layer),
                str(dungeon_status),
                operation_id,
            ),
        )
        return DungeonExploreOperationResult(
            "applied",
            "completed",
            str(result_status),
            dict(response),
            None,
            int(current_layer),
            str(dungeon_status),
        )

    def settle(
        self,
        operation_id: str,
        user_id: str,
        max_goods_num: int,
    ) -> DungeonExploreOperationResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or max_goods_num < 0:
            raise ValueError("valid operation and inventory limit are required")
        identity = self._identity(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._select(conn, operation_id)
                if row is None:
                    conn.rollback()
                    return DungeonExploreOperationResult("missing")
                if str(row[0]) != identity:
                    conn.rollback()
                    return DungeonExploreOperationResult("operation_conflict")
                if str(row[1]) == "completed":
                    conn.rollback()
                    return self._row_result(row, duplicate=True)
                if str(row[1]) != "prepared":
                    conn.rollback()
                    return DungeonExploreOperationResult("invalid_phase", str(row[1]))

                plan = self._load_json(row[2], {})
                if not isinstance(plan, dict):
                    conn.rollback()
                    return DungeonExploreOperationResult("invalid_plan")
                expected_status = self._normalize_status(plan.get("expected_status", {}))
                status_columns = set(
                    conn.execute("PRAGMA player_data.table_info(player_dungeon_status)").fetchall()
                )
                available_status_columns = set()
                for info in status_columns:
                    if len(info) > 1:
                        available_status_columns.add(str(info[1]))
                required_status_columns = set(_STATUS_FIELDS) & available_status_columns
                if (
                    not expected_status
                    or not required_status_columns.issubset(expected_status)
                    or any(key not in available_status_columns for key in expected_status)
                ):
                    conflict = self._complete_in_transaction(
                        conn,
                        operation_id,
                        "state_changed",
                        {"battle_messages": [], "message": "副本状态已变化，请重新发起探索。"},
                        int(expected_status.get("current_layer", 0)),
                        str(expected_status.get("dungeon_status", "")),
                    )
                    conn.commit()
                    return conflict
                selected = list(expected_status)
                status_row = conn.execute(
                    "SELECT " + ",".join(selected)
                    + " FROM player_data.player_dungeon_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                current_status = None
                if status_row is not None:
                    current_status = self._normalize_status(dict(zip(selected, status_row)))
                if current_status != expected_status:
                    conflict = self._complete_in_transaction(
                        conn,
                        operation_id,
                        "state_changed",
                        {"battle_messages": [], "message": "副本状态已变化，请重新发起探索。"},
                        int(expected_status.get("current_layer", 0)),
                        str(expected_status.get("dungeon_status", "")),
                    )
                    conn.commit()
                    return conflict

                expected_team = plan.get("team")
                current_team = self._current_team(conn, user_id)
                if expected_team is None:
                    team_matches = current_team is None
                else:
                    normalized_team = {
                        "team_id": str(expected_team.get("team_id", "")),
                        "leader": str(expected_team.get("leader", "")),
                        "members": self._members(expected_team.get("members", [])),
                    }
                    if "version" in expected_team:
                        normalized_team["version"] = int(expected_team.get("version", 0))
                    team_matches = current_team == normalized_team
                if not team_matches:
                    conflict = self._complete_in_transaction(
                        conn,
                        operation_id,
                        "team_changed",
                        {"battle_messages": [], "message": "队伍状态已变化，请重新发起探索。"},
                        int(expected_status.get("current_layer", 0)),
                        str(expected_status.get("dungeon_status", "")),
                    )
                    conn.commit()
                    return conflict

                members = plan.get("members", [])
                if not isinstance(members, list) or not members:
                    conn.rollback()
                    return DungeonExploreOperationResult("invalid_plan")
                seen: set[str] = set()
                inventory_rows: list[tuple[str, dict[str, Any], int, int]] = []
                for member in members:
                    member_id = str(member.get("user_id", ""))
                    if not member_id or member_id in seen:
                        conn.rollback()
                        return DungeonExploreOperationResult("invalid_plan")
                    seen.add(member_id)
                    expected = member.get("expected", {})
                    user = conn.execute(
                        "SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id=%s",
                        (member_id,),
                    ).fetchone()
                    if user is None:
                        conflict = self._complete_in_transaction(
                            conn,
                            operation_id,
                            "user_missing",
                            {"battle_messages": [], "message": "队伍成员数据已不存在，请重新发起探索。"},
                            int(expected_status.get("current_layer", 0)),
                            str(expected_status.get("dungeon_status", "")),
                        )
                        conn.commit()
                        return conflict
                    current_resources = {
                        "hp": int(user[0]),
                        "mp": int(user[1]),
                        "stone": int(user[2]),
                        "exp": int(user[3]),
                    }
                    expected_resources = {
                        key: int(expected.get(key, 0))
                        for key in ("hp", "mp", "stone", "exp")
                    }
                    final_hp = int(member.get("final_hp", expected_resources["hp"]))
                    final_mp = int(member.get("final_mp", expected_resources["mp"]))
                    if final_hp < 1 or final_mp < 0:
                        conn.rollback()
                        return DungeonExploreOperationResult("invalid_plan")
                    cd = conn.execute(
                        "SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s ORDER BY rowid DESC LIMIT 1",
                        (member_id,),
                    ).fetchone()
                    current_cd_type = int(cd[0]) if cd else 0
                    if (
                        current_resources != expected_resources
                        or current_cd_type != int(expected.get("cd_type", 0))
                    ):
                        conflict = self._complete_in_transaction(
                            conn,
                            operation_id,
                            "state_changed",
                            {"battle_messages": [], "message": "队伍成员状态已变化，请重新发起探索。"},
                            int(expected_status.get("current_layer", 0)),
                            str(expected_status.get("dungeon_status", "")),
                        )
                        conn.commit()
                        return conflict
                    member_item_ids: set[int] = set()
                    for item in member.get("items", []):
                        item_id = int(item["id"])
                        if item_id in member_item_ids:
                            conn.rollback()
                            return DungeonExploreOperationResult("invalid_plan")
                        member_item_ids.add(item_id)
                        inventory = conn.execute(
                            "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                            "WHERE user_id=%s AND goods_id=%s",
                            (member_id, item_id),
                        ).fetchone()
                        goods_num = int(inventory[0]) if inventory else 0
                        bind_num = int(inventory[1]) if inventory else 0
                        if goods_num < 0 or bind_num < 0 or bind_num > goods_num:
                            conflict = self._complete_in_transaction(
                                conn,
                                operation_id,
                                "state_changed",
                                {"battle_messages": [], "message": "背包状态异常，请重新发起探索。"},
                                int(expected_status.get("current_layer", 0)),
                                str(expected_status.get("dungeon_status", "")),
                            )
                            conn.commit()
                            return conflict
                        if (
                            goods_num != int(item.get("expected_num", 0))
                            or bind_num != int(item.get("expected_bind_num", 0))
                        ):
                            conflict = self._complete_in_transaction(
                                conn,
                                operation_id,
                                "state_changed",
                                {"battle_messages": [], "message": "背包状态已变化，请重新发起探索。"},
                                int(expected_status.get("current_layer", 0)),
                                str(expected_status.get("dungeon_status", "")),
                            )
                            conn.commit()
                            return conflict
                        amount = int(item.get("amount", 0))
                        if amount <= 0:
                            conn.rollback()
                            return DungeonExploreOperationResult("invalid_plan")
                        if goods_num + amount > max_goods_num:
                            conflict = self._complete_in_transaction(
                                conn,
                                operation_id,
                                "inventory_full",
                                {"battle_messages": [], "message": "背包中该物品数量已达上限，本次探索未结算。"},
                                int(expected_status.get("current_layer", 0)),
                                str(expected_status.get("dungeon_status", "")),
                            )
                            conn.commit()
                            return conflict
                        inventory_rows.append((member_id, item, goods_num, bind_num))

                now = datetime.now()
                for member in members:
                    member_id = str(member["user_id"])
                    conn.execute(
                        "UPDATE user_xiuxian SET hp=%s,mp=%s,stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL),exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) "
                        "WHERE user_id=%s",
                        (
                            int(member.get("final_hp", member["expected"]["hp"])),
                            int(member.get("final_mp", member["expected"]["mp"])),
                            int(member.get("stone_delta", 0)),
                            int(member.get("exp_delta", 0)),
                            member_id,
                        ),
                    )
                    for item in member.get("items", []):
                        amount = int(item["amount"])
                        conn.execute(
                            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,"
                            "create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                            "ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                            "goods_num=back.goods_num+EXCLUDED.goods_num,"
                            "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num,"
                            "update_time=EXCLUDED.update_time",
                            (
                                member_id,
                                int(item["id"]),
                                str(item["name"]),
                                str(item["type"]),
                                amount,
                                now,
                                now,
                                amount,
                            ),
                        )

                for member_id, item, goods_num, bind_num in inventory_rows:
                    final_item = conn.execute(
                        "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (member_id, int(item["id"])),
                    ).fetchone()
                    amount = int(item["amount"])
                    if final_item is None or (int(final_item[0]), int(final_item[1])) != (
                        goods_num + amount,
                        bind_num + amount,
                    ):
                        raise RuntimeError("dungeon exploration inventory invariant failed")

                current_layer = int(expected_status["current_layer"])
                total_layers = int(expected_status["total_layers"])
                if bool(plan.get("complete")):
                    final_layer = total_layers
                elif bool(plan.get("advance")):
                    final_layer = min(current_layer + 1, total_layers)
                else:
                    final_layer = current_layer
                final_status = "completed" if final_layer >= total_layers else "exploring"

                where = " AND ".join(f"{column}=%s" for column in selected)
                expected_values = [expected_status[column] for column in selected]
                updated = conn.execute(
                    "UPDATE player_data.player_dungeon_status SET current_layer=%s,dungeon_status=%s "
                    "WHERE user_id=%s AND " + where,
                    (final_layer, final_status, user_id, *expected_values),
                )
                if updated.rowcount != 1:
                    raise db_backend.OperationalError("dungeon status compare-and-set failed")

                response = plan.get("response", {})
                if not isinstance(response, dict):
                    response = {}
                result = self._complete_in_transaction(
                    conn,
                    operation_id,
                    "applied",
                    response,
                    final_layer,
                    final_status,
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

_LOCKS_GUARD = RLock()
_DATABASE_LOCKS: dict[Path, RLock] = {}

def _database_lock(path: str | Path) -> RLock:
    resolved = Path(path).expanduser().resolve()
    with _LOCKS_GUARD:
        return _DATABASE_LOCKS.setdefault(resolved, RLock())

@dataclass(frozen=True)
class DungeonResetResult:
    status: str
    operation_id: str = ""
    business_date: str = ""
    generation: int = 0
    source: str = ""
    dungeon_snapshot: dict[str, Any] = field(default_factory=dict)
    reset_players: int = 0
    operation_status: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DungeonResetService:
    """Publish one dungeon generation and reset every player in one transaction."""

    _AUTOMATIC_SOURCES = frozenset({"daily", "crossday"})
    _SOURCES = _AUTOMATIC_SOURCES | {"manual"}
    _GLOBAL_COLUMNS = {
        "user_id": "TEXT",
        "dungeon_id": "TEXT",
        "dungeon_name": "TEXT",
        "date": "TEXT",
        "total_layers": "INTEGER NOT NULL DEFAULT 0",
        "dungeon_type": "TEXT NOT NULL DEFAULT 'explore'",
        "description": "TEXT NOT NULL DEFAULT ''",
        "reset_generation": "INTEGER NOT NULL DEFAULT 0",
        "reset_operation_id": "TEXT NOT NULL DEFAULT ''",
    }
    _PLAYER_COLUMNS = {
        "user_id": "TEXT",
        "dungeon_id": "TEXT",
        "dungeon_name": "TEXT",
        "dungeon_status": "TEXT",
        "current_layer": "INTEGER",
        "total_layers": "INTEGER",
        "last_reset_date": "TEXT",
        "reset_generation": "INTEGER NOT NULL DEFAULT 0",
        "reset_operation_id": "TEXT NOT NULL DEFAULT ''",
    }
    _OPERATION_COLUMNS = {
        "operation_id": "TEXT",
        "business_date": "TEXT NOT NULL DEFAULT ''",
        "generation": "INTEGER NOT NULL DEFAULT 0",
        "source": "TEXT NOT NULL DEFAULT 'legacy'",
        "dungeon_snapshot": "TEXT NOT NULL DEFAULT '{}'",
        "result_json": "TEXT NOT NULL DEFAULT '{}'",
        "status": "TEXT NOT NULL DEFAULT 'completed'",
        "created_at": "TEXT NOT NULL DEFAULT ''",
        "updated_at": "TEXT NOT NULL DEFAULT ''",
    }

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or _database_lock(self._database)

    @staticmethod
    def _normalize_date(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @classmethod
    def automatic_operation_id(cls, business_date=None) -> str:
        """Return the shared durable ID used by daily and lazy cross-day reset."""

        return f"dungeon-reset:auto:{cls._normalize_date(business_date)}"

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def _normalize_snapshot(cls, value: Any) -> dict[str, Any]:
        if isinstance(value, Mapping):
            raw = dict(value)
        else:
            raw = {
                "dungeon_id": getattr(value, "id", None),
                "dungeon_name": getattr(value, "name", None),
                "total_layers": getattr(value, "total_layers", None),
                "dungeon_type": getattr(value, "type", None),
                "description": getattr(value, "description", None),
            }

        dungeon_id = str(raw.pop("dungeon_id", raw.pop("id", "")) or "").strip()
        dungeon_name = str(
            raw.pop("dungeon_name", raw.pop("name", "")) or ""
        ).strip()
        try:
            total_layers = int(raw.pop("total_layers", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("dungeon total_layers must be an integer") from exc
        if not dungeon_id or not dungeon_name or total_layers < 1:
            raise ValueError("valid dungeon id, name and total_layers are required")

        snapshot = {
            "dungeon_id": dungeon_id,
            "dungeon_name": dungeon_name,
            "total_layers": total_layers,
        }
        snapshot.update(raw)
        try:
            return json.loads(cls._json(snapshot))
        except (TypeError, ValueError) as exc:
            raise ValueError("dungeon snapshot must be JSON serializable") from exc

    @staticmethod
    def _ensure_columns(conn, table: str, columns: dict[str, str]) -> None:
        existing = {str(name).lower() for name in conn.column_names(table)}
        for name, definition in columns.items():
            if name.lower() in existing:
                continue
            conn.execute(
                f"ALTER TABLE {db_backend.quote_ident(table)} ADD COLUMN "
                f"{db_backend.quote_ident(name)} {definition}"
            )

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_global_state("
            "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,date TEXT)"
        )
        cls._ensure_columns(conn, "dungeon_global_state", cls._GLOBAL_COLUMNS)

        conn.execute(
            "CREATE TABLE IF NOT EXISTS player_dungeon_status("
            "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,"
            "dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,"
            "last_reset_date TEXT)"
        )
        cls._ensure_columns(conn, "player_dungeon_status", cls._PLAYER_COLUMNS)

        conn.execute(
            "CREATE TABLE IF NOT EXISTS dungeon_reset_operations("
            "operation_id TEXT PRIMARY KEY,business_date TEXT NOT NULL,"
            "generation INTEGER NOT NULL,source TEXT NOT NULL,"
            "dungeon_snapshot TEXT NOT NULL,result_json TEXT NOT NULL,"
            "status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        cls._ensure_columns(
            conn, "dungeon_reset_operations", cls._OPERATION_COLUMNS
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS dungeon_reset_operation_id_uq "
            "ON dungeon_reset_operations(operation_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS dungeon_reset_business_date_idx "
            "ON dungeon_reset_operations(business_date,generation)"
        )

    @staticmethod
    def _decode_object(value) -> dict[str, Any]:
        try:
            decoded = json.loads(str(value or "{}"))
        except (TypeError, ValueError):
            return {}
        return decoded if isinstance(decoded, dict) else {}

    @classmethod
    def _stored_result(cls, row, status: str) -> DungeonResetResult:
        snapshot = cls._decode_object(row[4])
        result = cls._decode_object(row[5])
        return DungeonResetResult(
            status=status,
            operation_id=str(row[0] or ""),
            business_date=str(row[1] or ""),
            generation=int(row[2] or 0),
            source=str(row[3] or ""),
            dungeon_snapshot=snapshot,
            reset_players=int(result.get("reset_players", 0) or 0),
            operation_status=str(row[6] or ""),
        )

    @staticmethod
    def _same_request(row, business_date: str, source: str) -> bool:
        stored_source = str(row[3] or "")
        return str(row[1] or "") == business_date and (
            stored_source == source
            or {stored_source, source}.issubset(DungeonResetService._AUTOMATIC_SOURCES)
        )

    @staticmethod
    def _operation(conn, operation_id: str):
        return conn.execute(
            "SELECT operation_id,business_date,generation,source,dungeon_snapshot,"
            "result_json,status FROM dungeon_reset_operations WHERE operation_id=%s",
            (operation_id,),
        ).fetchone()

    @classmethod
    def _automatic_publication(cls, conn, business_date: str):
        placeholders = ",".join("%s" for _ in cls._AUTOMATIC_SOURCES)
        return conn.execute(
            "SELECT operation_id,business_date,generation,source,dungeon_snapshot,"
            "result_json,status FROM dungeon_reset_operations "
            f"WHERE business_date=%s AND source IN ({placeholders}) "
            "AND status='completed' ORDER BY generation LIMIT 1",
            (business_date, *sorted(cls._AUTOMATIC_SOURCES)),
        ).fetchone()

    def operation_result(self, operation_id) -> DungeonResetResult | None:
        """Return one published reset snapshot without creating a new generation."""

        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = self._operation(conn, operation_id)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if row is None:
            return None
        return self._stored_result(row, "duplicate")

    @staticmethod
    def _upsert_global(
        conn,
        snapshot: dict[str, Any],
        business_date: str,
        generation: int,
        operation_id: str,
    ) -> None:
        values = (
            snapshot["dungeon_id"],
            snapshot["dungeon_name"],
            business_date,
            int(snapshot["total_layers"]),
            str(snapshot.get("dungeon_type", "explore")),
            str(snapshot.get("description", "")),
            int(generation),
            str(operation_id),
            "0",
        )
        updated = conn.execute(
            "UPDATE dungeon_global_state SET dungeon_id=%s,dungeon_name=%s,date=%s,"
            "total_layers=%s,dungeon_type=%s,description=%s,reset_generation=%s,"
            "reset_operation_id=%s "
            "WHERE user_id=%s",
            values,
        )
        if updated.rowcount == 0:
            conn.execute(
                "INSERT INTO dungeon_global_state("
                "dungeon_id,dungeon_name,date,total_layers,dungeon_type,description,"
                "reset_generation,reset_operation_id,user_id) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                values,
            )

    def ensure_player_status(
        self,
        user_id,
        fallback_snapshot: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return the current generation, initializing one player atomically."""

        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        fallback = (
            self._normalize_snapshot(fallback_snapshot)
            if fallback_snapshot is not None
            else None
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                global_row = conn.execute(
                    "SELECT dungeon_id,dungeon_name,date,total_layers,dungeon_type,"
                    "description,reset_generation,reset_operation_id "
                    "FROM dungeon_global_state WHERE user_id='0'"
                ).fetchone()
                if global_row is None:
                    raise RuntimeError("dungeon global state is missing")
                global_state = {
                    "dungeon_id": str(global_row[0] or ""),
                    "dungeon_name": str(global_row[1] or ""),
                    "date": str(global_row[2] or ""),
                    "total_layers": int(global_row[3] or 0),
                    "dungeon_type": str(global_row[4] or "explore"),
                    "description": str(global_row[5] or ""),
                    "reset_generation": int(global_row[6] or 0),
                    "reset_operation_id": str(global_row[7] or ""),
                }
                if global_state["total_layers"] < 1:
                    if fallback is None or fallback["dungeon_id"] != global_state["dungeon_id"]:
                        raise RuntimeError("dungeon global snapshot is incomplete")
                    global_state.update(
                        {
                            "dungeon_name": fallback["dungeon_name"],
                            "total_layers": fallback["total_layers"],
                            "dungeon_type": str(fallback.get("dungeon_type", "explore")),
                            "description": str(fallback.get("description", "")),
                        }
                    )
                    conn.execute(
                        "UPDATE dungeon_global_state SET dungeon_name=%s,total_layers=%s,"
                        "dungeon_type=%s,description=%s WHERE user_id='0'",
                        (
                            global_state["dungeon_name"],
                            global_state["total_layers"],
                            global_state["dungeon_type"],
                            global_state["description"],
                        ),
                    )

                columns = (
                    "dungeon_id",
                    "dungeon_name",
                    "dungeon_status",
                    "current_layer",
                    "total_layers",
                    "last_reset_date",
                    "reset_generation",
                    "reset_operation_id",
                )
                row = conn.execute(
                    "SELECT " + ",".join(columns)
                    + " FROM player_dungeon_status WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                needs_reset = row is None
                if row is not None:
                    needs_reset = (
                        str(row[0] or "") != global_state["dungeon_id"]
                        or str(row[5] or "") != global_state["date"]
                        or int(row[6] or 0) != global_state["reset_generation"]
                        or str(row[7] or "") != global_state["reset_operation_id"]
                    )
                values = (
                    global_state["dungeon_id"],
                    global_state["dungeon_name"],
                    "not_started",
                    0,
                    global_state["total_layers"],
                    global_state["date"],
                    global_state["reset_generation"],
                    global_state["reset_operation_id"],
                )
                if row is None:
                    conn.execute(
                        "INSERT INTO player_dungeon_status(user_id,"
                        + ",".join(columns)
                        + ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (user_id, *values),
                    )
                    row = values
                elif needs_reset:
                    conn.execute(
                        "UPDATE player_dungeon_status SET "
                        + ",".join(f"{column}=%s" for column in columns)
                        + " WHERE user_id=%s",
                        (*values, user_id),
                    )
                    row = values
                result = dict(zip(columns, row))
                result["current_layer"] = int(result["current_layer"] or 0)
                result["total_layers"] = int(result["total_layers"] or 0)
                result["reset_generation"] = int(result["reset_generation"] or 0)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def reset(
        self,
        operation_id,
        business_date,
        source,
        dungeon_factory: Callable[[], Any],
        *,
        updated_at=None,
    ) -> DungeonResetResult:
        operation_id = str(operation_id).strip()
        business_date = self._normalize_date(business_date)
        source = str(source).strip().lower()
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not operation_id or source not in self._SOURCES:
            raise ValueError("valid operation_id and reset source are required")
        if not callable(dungeon_factory):
            raise TypeError("dungeon_factory must be callable")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)

                previous = self._operation(conn, operation_id)
                if previous is not None:
                    if not self._same_request(previous, business_date, source):
                        conn.commit()
                        return self._stored_result(previous, "operation_conflict")
                    conn.commit()
                    return self._stored_result(previous, "duplicate")

                if source in self._AUTOMATIC_SOURCES:
                    published = self._automatic_publication(conn, business_date)
                    if published is not None:
                        conn.commit()
                        return self._stored_result(published, "duplicate")

                generation_row = conn.execute(
                    "SELECT COALESCE(MAX(generation),0) FROM dungeon_reset_operations "
                    "WHERE business_date=%s AND status='completed'",
                    (business_date,),
                ).fetchone()
                generation = int(generation_row[0] or 0) + 1
                snapshot = self._normalize_snapshot(dungeon_factory())

                self._upsert_global(
                    conn, snapshot, business_date, generation, operation_id
                )
                reset = conn.execute(
                    "UPDATE player_dungeon_status SET dungeon_id=%s,dungeon_name=%s,"
                    "dungeon_status='not_started',current_layer=0,total_layers=%s,"
                    "last_reset_date=%s,reset_generation=%s,reset_operation_id=%s",
                    (
                        snapshot["dungeon_id"],
                        snapshot["dungeon_name"],
                        snapshot["total_layers"],
                        business_date,
                        generation,
                        operation_id,
                    ),
                )
                reset_players = max(0, int(reset.rowcount))
                result_json = self._json(
                    {
                        "business_date": business_date,
                        "dungeon_snapshot": snapshot,
                        "generation": generation,
                        "operation_id": operation_id,
                        "reset_players": reset_players,
                        "source": source,
                        "status": "completed",
                    }
                )
                conn.execute(
                    "INSERT INTO dungeon_reset_operations("
                    "operation_id,business_date,generation,source,dungeon_snapshot,"
                    "result_json,status,created_at,updated_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,'completed',%s,%s)",
                    (
                        operation_id,
                        business_date,
                        generation,
                        source,
                        self._json(snapshot),
                        result_json,
                        updated_at,
                        updated_at,
                    ),
                )
                stored = self._operation(conn, operation_id)
                if stored is None:
                    raise db_backend.IntegrityError(
                        "dungeon reset operation was not persisted"
                    )
                result = self._stored_result(stored, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class DungeonRewardResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class DungeonRewardService:
    """Award every eligible dungeon member in one game-database transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def award(self, operation_id, rewards, max_goods_num) -> DungeonRewardResult:
        operation_id = str(operation_id).strip()
        max_goods_num = int(max_goods_num)
        normalized = tuple(
            (str(reward["user_id"]), int(reward.get("stone", 0)), int(reward.get("exp", 0)),
             tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in reward.get("items", []) if int(item.get("amount", 0)) > 0))
            for reward in rewards
        )
        if not operation_id or max_goods_num < 0 or not normalized or any(min(stone, exp) < 0 for _, stone, exp, _ in normalized):
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps(normalized, ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dungeon_reward_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload FROM dungeon_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    return DungeonRewardResult("duplicate" if str(previous[0]) == payload else "state_changed")
                for user_id, _, _, _ in normalized:
                    if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                        conn.rollback()
                        return DungeonRewardResult("user_missing")
                for user_id, _, _, reward_items in normalized:
                    for item_id, _, _, amount in reward_items:
                        row = conn.execute("SELECT COALESCE(goods_num,0) FROM back WHERE user_id=%s AND goods_id=%s", (user_id, item_id)).fetchone()
                        if (int(row[0]) if row else 0) + amount > max_goods_num:
                            conn.rollback()
                            return DungeonRewardResult("inventory_full")
                now = datetime.now()
                for user_id, stone, exp, reward_items in normalized:
                    conn.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(%s AS REAL), exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (stone, exp, user_id))
                    for item_id, name, item_type, amount in reward_items:
                        conn.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, update_time=EXCLUDED.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
                conn.execute("INSERT INTO dungeon_reward_operations VALUES (%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload))
                conn.commit()
                return DungeonRewardResult("applied")
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "TeamMutationResult",
    "TeamInviteSnapshot",
    "TeamStateSnapshot",
    "TeamExitResult",
    "DungeonTeamTransactionService",
    "TeamMemberView",
    "TeamViewResult",
    "TeamTransferResult",
    "TeamLeaveResult",
    "TeamKickResult",
    "TeamInviteResult",
    "TeamInviteResponseResult",
    "DungeonTeamExitService",
    "DungeonSessionResult",
    "DungeonSessionService",
    "DungeonPurchaseResult",
    "DungeonPurchaseService",
    "DungeonExploreOperationResult",
    "DungeonExploreOperationService",
    "DungeonResetResult",
    "DungeonResetService",
    "DungeonRewardResult",
    "DungeonRewardService",
]
