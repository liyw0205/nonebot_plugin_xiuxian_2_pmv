from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


__all__ = [
    "DungeonTeamTransactionService",
    "TeamExitResult",
    "TeamInviteSnapshot",
    "TeamMutationResult",
    "TeamStateSnapshot",
]
