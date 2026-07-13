from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
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


@dataclass(frozen=True)
class TeamStateSnapshot:
    team_id: str
    team_name: str
    leader_id: str
    members: tuple[str, ...]
    first_join: tuple[tuple[str, int], ...]
    sessions: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class TeamExitResult:
    status: str
    team_id: str = ""
    team_name: str = ""
    new_leader_id: str = ""
    disbanded: bool = False
    cooldown_members: tuple[str, ...] = ()


class DungeonTeamTransactionService:
    """Own team creation and invitation-based joining in one transaction."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS teams (user_id TEXT PRIMARY KEY)")
        columns = set(conn.column_names("teams"))
        fields = {"team_id": "TEXT", "team_name": "TEXT", "group_id": "TEXT", "leader": "TEXT", "members": "TEXT", "create_time": "TEXT", "max_members": "INTEGER", "description": "TEXT"}
        for name, data_type in fields.items():
            if name not in columns:
                conn.execute(f'ALTER TABLE teams ADD COLUMN "{name}" {data_type} DEFAULT NULL')
        conn.execute("CREATE TABLE IF NOT EXISTS dungeon_team_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,team_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        conn.execute("CREATE TABLE IF NOT EXISTS dungeon_team_invites (invite_id TEXT PRIMARY KEY,team_id TEXT NOT NULL,inviter_id TEXT NOT NULL,invitee_id TEXT NOT NULL,group_id TEXT NOT NULL,expires_at REAL NOT NULL,consumed_at TIMESTAMP DEFAULT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS team_cd (user_id TEXT PRIMARY KEY,join_cd_until TEXT DEFAULT '',had_first_join INTEGER DEFAULT 0)")
        conn.execute("CREATE TABLE IF NOT EXISTS dungeon_team_exit_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

    @staticmethod
    def _members(value) -> list[str]:
        try:
            decoded = json.loads(value or "[]") if isinstance(value, str) else value
        except (TypeError, ValueError):
            decoded = []
        return [str(item) for item in decoded] if isinstance(decoded, list) else []

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
        row = conn.execute("SELECT dungeon_status FROM player_dungeon_status WHERE user_id=%s", (user_id,)).fetchone()
        return row is not None and str(row[0]) == "exploring"

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
        )

    def snapshot(self, team_id: str) -> TeamStateSnapshot | None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                "SELECT team_name,leader,members FROM teams WHERE user_id=%s",
                (str(team_id),),
            ).fetchone()
            if row is None:
                return None
            return self._snapshot_from_row(conn, str(team_id), row)

    @staticmethod
    def _snapshot_payload(snapshot: TeamStateSnapshot) -> dict:
        return {
            "team_id": snapshot.team_id,
            "team_name": snapshot.team_name,
            "leader_id": snapshot.leader_id,
            "members": list(snapshot.members),
            "first_join": [list(item) for item in snapshot.first_join],
            "sessions": [list(item) for item in snapshot.sessions],
        }

    @staticmethod
    def _result_json(result: TeamExitResult) -> str:
        return json.dumps(
            {
                "status": result.status,
                "team_id": result.team_id,
                "team_name": result.team_name,
                "new_leader_id": result.new_leader_id,
                "disbanded": result.disbanded,
                "cooldown_members": list(result.cooldown_members),
            },
            ensure_ascii=True,
            sort_keys=True,
        )

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
        payload = json.dumps(
            {
                "action": action,
                "actor_id": actor_id,
                "target_id": target_id,
                "expected": self._snapshot_payload(expected),
                "cooldown_until": str(cooldown_until),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        if not operation_id:
            raise ValueError("operation_id is required")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT payload,result_json FROM dungeon_team_exit_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return TeamExitResult("state_changed", expected.team_id, expected.team_name)
                    old_result = self._decode_result(str(previous[1]))
                    return TeamExitResult(
                        "duplicate",
                        old_result.team_id,
                        old_result.team_name,
                        old_result.new_leader_id,
                        old_result.disbanded,
                        old_result.cooldown_members,
                    )

                row = conn.execute(
                    "SELECT team_name,leader,members FROM teams WHERE user_id=%s",
                    (expected.team_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return TeamExitResult("team_missing", expected.team_id, expected.team_name)
                current = self._snapshot_from_row(conn, expected.team_id, row)
                if current != expected:
                    conn.rollback()
                    return TeamExitResult("state_changed", expected.team_id, expected.team_name)
                if actor_id not in current.members:
                    conn.rollback()
                    return TeamExitResult("actor_not_member", current.team_id, current.team_name)

                if action in {"kick", "disband"} and actor_id != current.leader_id:
                    conn.rollback()
                    return TeamExitResult("actor_not_leader", current.team_id, current.team_name)
                if action in {"leave", "kick"} and target_id not in current.members:
                    conn.rollback()
                    return TeamExitResult("target_not_member", current.team_id, current.team_name)
                if action == "kick" and target_id == actor_id:
                    conn.rollback()
                    return TeamExitResult("self_target", current.team_id, current.team_name)

                members = list(current.members)
                affected = list(members) if action == "disband" else [target_id]
                disbanded = action == "disband" or (action == "leave" and len(members) == 1)
                new_leader = ""
                if disbanded:
                    conn.execute("DELETE FROM teams WHERE user_id=%s", (current.team_id,))
                else:
                    members.remove(target_id)
                    new_leader = members[0] if target_id == current.leader_id else current.leader_id
                    conn.execute(
                        "UPDATE teams SET members=%s,leader=%s WHERE user_id=%s",
                        (json.dumps(members), new_leader, current.team_id),
                    )

                cooldown_members = []
                first_join = dict(current.first_join)
                for member in affected:
                    if first_join.get(member, 0) == 1:
                        conn.execute(
                            "INSERT INTO team_cd (user_id,join_cd_until,had_first_join) VALUES (%s,%s,1) "
                            "ON CONFLICT(user_id) DO UPDATE SET join_cd_until=excluded.join_cd_until",
                            (member, str(cooldown_until)),
                        )
                        cooldown_members.append(member)
                    if conn.table_exists("player_dungeon_status"):
                        conn.execute(
                            "UPDATE player_dungeon_status SET dungeon_status=%s "
                            "WHERE user_id=%s AND dungeon_status=%s",
                            ("not_started", member, "exploring"),
                        )

                invite_params = [current.team_id, *affected]
                placeholders = ",".join("%s" for _ in affected)
                conn.execute(
                    "UPDATE dungeon_team_invites SET consumed_at=CURRENT_TIMESTAMP "
                    f"WHERE consumed_at IS NULL AND (team_id=%s OR invitee_id IN ({placeholders}))",
                    tuple(invite_params),
                )
                result = TeamExitResult(
                    "applied",
                    current.team_id,
                    current.team_name,
                    new_leader,
                    disbanded,
                    tuple(cooldown_members),
                )
                conn.execute(
                    "INSERT INTO dungeon_team_exit_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, self._result_json(result)),
                )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def leave(self, operation_id: str, actor_id: str, expected: TeamStateSnapshot, cooldown_until: str) -> TeamExitResult:
        return self._exit_mutation(
            action="leave",
            operation_id=operation_id,
            actor_id=actor_id,
            target_id=actor_id,
            expected=expected,
            cooldown_until=cooldown_until,
        )

    def kick(self, operation_id: str, actor_id: str, target_id: str, expected: TeamStateSnapshot, cooldown_until: str) -> TeamExitResult:
        return self._exit_mutation(
            action="kick",
            operation_id=operation_id,
            actor_id=actor_id,
            target_id=target_id,
            expected=expected,
            cooldown_until=cooldown_until,
        )

    def disband(self, operation_id: str, actor_id: str, expected: TeamStateSnapshot, cooldown_until: str) -> TeamExitResult:
        return self._exit_mutation(
            action="disband",
            operation_id=operation_id,
            actor_id=actor_id,
            target_id="",
            expected=expected,
            cooldown_until=cooldown_until,
        )

    def create(self, operation_id, team_id, team_name, leader_id, group_id, created_at) -> TeamMutationResult:
        operation_id, team_id, leader_id = str(operation_id).strip(), str(team_id), str(leader_id)
        payload = json.dumps({"action": "create", "team_id": team_id, "team_name": str(team_name), "leader_id": leader_id, "group_id": str(group_id)}, ensure_ascii=True, sort_keys=True)
        if not operation_id or not team_id:
            raise ValueError("operation_id and team_id are required")
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute("SELECT payload,team_id FROM dungeon_team_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback()
                    return TeamMutationResult("duplicate" if str(old[0]) == payload else "state_changed", str(old[1]))
                if conn.table_exists("user_xiuxian") and conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (leader_id,)).fetchone() is None:
                    conn.rollback(); return TeamMutationResult("user_missing")
                if self._user_team(conn, leader_id):
                    conn.rollback(); return TeamMutationResult("user_has_team")
                if self._active_session(conn, leader_id):
                    conn.rollback(); return TeamMutationResult("session_active")
                conn.execute("INSERT INTO teams (user_id,team_id,team_name,group_id,leader,members,create_time,max_members,description) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (team_id, team_id, str(team_name), str(group_id), leader_id, json.dumps([leader_id]), str(created_at), 4, ""))
                conn.execute("INSERT INTO dungeon_team_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, "applied", team_id))
                conn.commit()
                return TeamMutationResult("applied", team_id, str(team_name), leader_id, 1, 4)
            except Exception:
                conn.rollback(); raise

    def record_invite(self, invite_id, team_id, inviter_id, invitee_id, group_id, expires_at) -> None:
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                conn.execute("INSERT OR REPLACE INTO dungeon_team_invites (invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,consumed_at) VALUES (%s,%s,%s,%s,%s,%s,NULL)", (str(invite_id), str(team_id), str(inviter_id), str(invitee_id), str(group_id), float(expires_at)))
                conn.commit()
            except Exception:
                conn.rollback(); raise

    def join(self, operation_id, invite_id, team_id, inviter_id, user_id, group_id, now_timestamp) -> TeamMutationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        payload = json.dumps({"action": "join", "invite_id": str(invite_id), "team_id": str(team_id), "inviter_id": str(inviter_id), "user_id": user_id, "group_id": str(group_id)}, ensure_ascii=True, sort_keys=True)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                old = conn.execute("SELECT payload,team_id FROM dungeon_team_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old:
                    conn.rollback(); return TeamMutationResult("duplicate" if str(old[0]) == payload else "state_changed", str(old[1]))
                invite = conn.execute("SELECT team_id,inviter_id,invitee_id,group_id,expires_at,consumed_at FROM dungeon_team_invites WHERE invite_id=%s", (str(invite_id),)).fetchone()
                expected_invite = (str(team_id), str(inviter_id), user_id, str(group_id))
                if invite is None or tuple(map(str, invite[:4])) != expected_invite or invite[5] is not None or float(invite[4]) <= float(now_timestamp):
                    conn.rollback(); return TeamMutationResult("invite_invalid")
                if conn.table_exists("user_xiuxian") and conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback(); return TeamMutationResult("user_missing")
                if self._user_team(conn, user_id):
                    conn.rollback(); return TeamMutationResult("user_has_team")
                if self._active_session(conn, user_id):
                    conn.rollback(); return TeamMutationResult("session_active")
                team = conn.execute("SELECT team_name,leader,members,max_members FROM teams WHERE user_id=%s", (str(team_id),)).fetchone()
                if team is None:
                    conn.rollback(); return TeamMutationResult("team_disbanded")
                members, maximum = self._members(team[2]), max(int(team[3] or 4), 1)
                if str(team[1]) != str(inviter_id):
                    conn.rollback(); return TeamMutationResult("invite_invalid")
                if len(members) >= maximum:
                    conn.rollback(); return TeamMutationResult("team_full")
                members.append(user_id)
                conn.execute("UPDATE teams SET members=%s WHERE user_id=%s", (json.dumps(members), str(team_id)))
                conn.execute("UPDATE dungeon_team_invites SET consumed_at=CURRENT_TIMESTAMP WHERE invite_id=%s AND consumed_at IS NULL", (str(invite_id),))
                conn.execute("INSERT INTO dungeon_team_operations VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)", (operation_id, payload, "applied", str(team_id)))
                conn.commit()
                return TeamMutationResult("applied", str(team_id), str(team[0]), str(team[1]), len(members), maximum)
            except Exception:
                conn.rollback(); raise


__all__ = [
    "DungeonTeamTransactionService",
    "TeamExitResult",
    "TeamMutationResult",
    "TeamStateSnapshot",
]
