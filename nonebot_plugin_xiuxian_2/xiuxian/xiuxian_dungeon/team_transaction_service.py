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


__all__ = ["DungeonTeamTransactionService", "TeamMutationResult"]
