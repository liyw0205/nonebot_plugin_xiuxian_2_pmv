from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


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


class DungeonTeamRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @classmethod
    def ensure_schema(cls, uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS teams(user_id TEXT PRIMARY KEY)")
        columns = {row["name"] for row in uow.query_all("PRAGMA table_info(teams)")}
        for name, definition in {
            "team_id": "TEXT DEFAULT NULL", "team_name": "TEXT DEFAULT NULL", "group_id": "TEXT DEFAULT NULL",
            "leader": "TEXT DEFAULT NULL", "members": "TEXT DEFAULT NULL", "create_time": "TEXT DEFAULT NULL",
            "max_members": "INTEGER DEFAULT 4", "description": "TEXT DEFAULT NULL", "version": "INTEGER NOT NULL DEFAULT 0",
        }.items():
            if name not in columns:
                uow.execute(f'ALTER TABLE teams ADD COLUMN "{name}" {definition}')
        uow.execute("CREATE TABLE IF NOT EXISTS dungeon_team_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,team_id TEXT NOT NULL,result_json TEXT NOT NULL DEFAULT '',action TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        uow.execute("CREATE TABLE IF NOT EXISTS dungeon_team_invites(invite_id TEXT PRIMARY KEY,team_id TEXT NOT NULL,inviter_id TEXT NOT NULL,invitee_id TEXT NOT NULL,group_id TEXT NOT NULL,expires_at REAL NOT NULL,consumed_at TIMESTAMP DEFAULT NULL,status TEXT NOT NULL DEFAULT 'pending',created_at REAL NOT NULL DEFAULT 0,resolved_operation_id TEXT DEFAULT NULL)")
        uow.execute("CREATE INDEX IF NOT EXISTS idx_dungeon_team_invites_pending ON dungeon_team_invites(invitee_id,status,expires_at)")
        uow.execute("CREATE TABLE IF NOT EXISTS team_cd(user_id TEXT PRIMARY KEY,join_cd_until TEXT DEFAULT '',had_first_join INTEGER DEFAULT 0)")

    @staticmethod
    def _members(value: Any) -> list[str]:
        try:
            value = json.loads(value or "[]") if isinstance(value, str) else value
        except (TypeError, ValueError):
            return []
        return [str(item) for item in value] if isinstance(value, list) else []

    @classmethod
    def _user_team(cls, uow: DatabaseUnitOfWork, user_id: str) -> str:
        for row in uow.query_all("SELECT user_id,members FROM teams"):
            if str(user_id) in cls._members(row["members"]):
                return str(row["user_id"])
        return ""

    @staticmethod
    def _active_session(uow: DatabaseUnitOfWork, user_id: str) -> bool:
        try:
            row = uow.query_one("SELECT dungeon_status FROM player_dungeon_status WHERE user_id=?", (user_id,))
        except Exception:
            return False
        return row is not None and str(row["dungeon_status"]) == "exploring"

    @staticmethod
    def _decode_mutation(row: Any) -> TeamMutationResult:
        try:
            data = json.loads(str(row["result_json"] or "{}"))
            return TeamMutationResult(
                status=str(data.get("status", row["result_status"])),
                team_id=str(data.get("team_id", row["team_id"])),
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
            return TeamMutationResult(str(row["result_status"]), team_id=str(row["team_id"]))

    def _finish(self, uow: DatabaseUnitOfWork, operation_id: str, action: str, payload: str, result: TeamMutationResult) -> TeamMutationResult:
        uow.execute("INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)", (operation_id, payload, result.status, result.team_id, self._json(result.__dict__), action))
        return result

    def create(self, operation_id: str, team_id: str, team_name: str, leader_id: str, group_id: str, created_at: str, now_timestamp: float) -> TeamMutationResult:
        payload = self._json({"action":"create","team_id":str(team_id),"team_name":str(team_name),"leader_id":str(leader_id),"group_id":str(group_id)})
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", team_id=str(old["team_id"]))
            base = dict(team_id=str(team_id), team_name=str(team_name), leader_id=str(leader_id), target_id=str(leader_id), group_id=str(group_id))
            if not group_id: return self._finish(uow, operation_id, "create", payload, TeamMutationResult("group_required", **base))
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (leader_id,)) is None: return self._finish(uow, operation_id, "create", payload, TeamMutationResult("user_missing", **base))
            if self._user_team(uow, leader_id): return self._finish(uow, operation_id, "create", payload, TeamMutationResult("user_has_team", **base))
            if self._active_session(uow, leader_id): return self._finish(uow, operation_id, "create", payload, TeamMutationResult("session_active", **base))
            if uow.query_one("SELECT 1 FROM teams WHERE user_id=?", (team_id,)): return self._finish(uow, operation_id, "create", payload, TeamMutationResult("team_exists", **base))
            uow.execute("INSERT INTO teams(user_id,team_id,team_name,group_id,leader,members,create_time,max_members,description,version) VALUES(?,?,?,?,?,?,?,?,?,0)", (team_id,team_id,team_name,group_id,leader_id,json.dumps([leader_id]),created_at,4,""))
            return self._finish(uow, operation_id, "create", payload, TeamMutationResult("applied", member_count=1, max_members=4, **base))

    def operation_result(self, operation_id: str, action: str = "") -> TeamMutationResult | None:
        with DatabaseUnitOfWork(self.database) as uow:
            self.ensure_schema(uow)
            row = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        if action and str(row["action"]) != str(action):
            return TeamMutationResult("state_changed", team_id=str(row["team_id"]))
        result = self._decode_mutation(row)
        return TeamMutationResult(**{**result.__dict__, "status": "duplicate" if result.status == "applied" else result.status})

    def invite(self, operation_id: str, invite_id: str, team_id: str, inviter_id: str, invitee_id: str, group_id: str, expires_at: float, now_timestamp: float) -> TeamMutationResult:
        payload = self._json({"action":"invite","invite_id":invite_id,"team_id":team_id,"inviter_id":inviter_id,"invitee_id":invitee_id,"group_id":group_id})
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
            if old is not None: return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", team_id=str(old["team_id"]))
            base = dict(team_id=team_id, invite_id=invite_id, target_id=invitee_id, group_id=group_id, expires_at=float(expires_at))
            if not group_id: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("group_required", **base))
            if not invitee_id: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("target_missing", **base))
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (invitee_id,)) is None: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("user_missing", **base))
            team = uow.query_one("SELECT team_name,leader,members,max_members,version FROM teams WHERE user_id=?", (team_id,))
            if team is None: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("team_disbanded", **base))
            members, maximum = self._members(team["members"]), max(int(team["max_members"] or 4), 1)
            base.update(team_name=str(team["team_name"] or ""), leader_id=str(team["leader"] or ""), member_count=len(members), max_members=maximum, version=int(team["version"] or 0))
            if str(team["leader"]) != inviter_id: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("actor_not_leader", **base))
            if len(members) >= maximum: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("team_full", **base))
            if self._user_team(uow, invitee_id): return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("user_has_team", **base))
            if self._active_session(uow, inviter_id) or self._active_session(uow, invitee_id): return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("session_active", **base))
            uow.execute("UPDATE dungeon_team_invites SET status='expired',consumed_at=CURRENT_TIMESTAMP WHERE invitee_id=? AND status='pending' AND expires_at<=?", (invitee_id, now_timestamp))
            pending = uow.query_one("SELECT invite_id FROM dungeon_team_invites WHERE invitee_id=? AND status='pending' AND expires_at>?", (invitee_id, now_timestamp))
            if pending is not None: return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("duplicate" if pending["invite_id"] == invite_id else "invite_pending", **base))
            uow.execute("INSERT INTO dungeon_team_invites(invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,status,created_at) VALUES(?,?,?,?,?,?,?,?)", (invite_id,team_id,inviter_id,invitee_id,group_id,float(expires_at),"pending",float(now_timestamp)))
            return self._finish(uow, operation_id, "invite", payload, TeamMutationResult("applied", **base))

    def join(self, operation_id: str, invite_id: str, team_id: str, inviter_id: str, user_id: str, group_id: str, now_timestamp: float) -> TeamMutationResult:
        payload = self._json({"action":"join","invite_id":invite_id,"team_id":team_id,"inviter_id":inviter_id,"user_id":user_id,"group_id":group_id})
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
            if old is not None: return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", team_id=str(old["team_id"]))
            invite = uow.query_one("SELECT team_id,inviter_id,invitee_id,group_id,expires_at,status,consumed_at FROM dungeon_team_invites WHERE invite_id=?", (invite_id,))
            base = dict(team_id=team_id, invite_id=invite_id, target_id=user_id, group_id=group_id)
            if invite is None or (str(invite["team_id"]), str(invite["inviter_id"]), str(invite["invitee_id"]), str(invite["group_id"])) != (team_id, inviter_id, user_id, group_id) or str(invite["status"]) != "pending" or invite["consumed_at"] is not None or float(invite["expires_at"]) <= float(now_timestamp):
                return self._finish(uow, operation_id, "join", payload, TeamMutationResult("invite_invalid", **base))
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None: return self._finish(uow, operation_id, "join", payload, TeamMutationResult("user_missing", **base))
            if self._user_team(uow, user_id): return self._finish(uow, operation_id, "join", payload, TeamMutationResult("user_has_team", **base))
            team = uow.query_one("SELECT team_name,leader,members,max_members,version FROM teams WHERE user_id=?", (team_id,))
            if team is None: return self._finish(uow, operation_id, "join", payload, TeamMutationResult("team_disbanded", **base))
            members, maximum = self._members(team["members"]), max(int(team["max_members"] or 4), 1)
            base.update(team_name=str(team["team_name"] or ""), leader_id=str(team["leader"] or ""), member_count=len(members), max_members=maximum, expires_at=float(invite["expires_at"]), version=int(team["version"] or 0))
            if len(members) >= maximum: return self._finish(uow, operation_id, "join", payload, TeamMutationResult("team_full", **base))
            if self._active_session(uow, user_id) or any(self._active_session(uow, member) for member in members): return self._finish(uow, operation_id, "join", payload, TeamMutationResult("session_active", **base))
            members.append(user_id)
            changed = uow.execute("UPDATE teams SET members=?,version=version+1 WHERE user_id=? AND version=?", (json.dumps(members), team_id, int(team["version"] or 0)))
            if changed.rowcount != 1: return TeamMutationResult("state_changed", **base)
            uow.execute("INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES(?,?,1) ON CONFLICT(user_id) DO UPDATE SET had_first_join=1", (user_id, ""))
            uow.execute("UPDATE dungeon_team_invites SET consumed_at=CURRENT_TIMESTAMP,status='joined',resolved_operation_id=? WHERE invite_id=? AND status='pending'", (operation_id, invite_id))
            base["member_count"] = len(members)
            base["version"] = int(team["version"] or 0) + 1
            return self._finish(uow, operation_id, "join", payload, TeamMutationResult("applied", **base))

    def _resolve_invite(self, action: str, operation_id: str, invite_id: str, user_id: str, group_id: str, now_timestamp: float) -> TeamMutationResult:
        payload = self._json({"action":action,"invite_id":invite_id,"user_id":user_id,"group_id":group_id})
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.ensure_schema(uow)
            old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
            if old is not None: return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", team_id=str(old["team_id"]))
            invite = uow.query_one("SELECT team_id,inviter_id,invitee_id,group_id,expires_at,status FROM dungeon_team_invites WHERE invite_id=?", (invite_id,))
            base = dict(invite_id=invite_id, target_id=user_id)
            if invite is None or (action != "expire" and str(invite["invitee_id"]) != user_id): return self._finish(uow, operation_id, action, payload, TeamMutationResult("invite_invalid", **base))
            base.update(team_id=str(invite["team_id"]), leader_id=str(invite["inviter_id"]), group_id=str(invite["group_id"]), expires_at=float(invite["expires_at"]))
            if group_id and str(invite["group_id"]) != str(group_id): return self._finish(uow, operation_id, action, payload, TeamMutationResult("wrong_group", **base))
            if str(invite["status"]) != "pending": return self._finish(uow, operation_id, action, payload, TeamMutationResult("invite_invalid", **base))
            if action == "expire" and float(now_timestamp) < float(invite["expires_at"]): return self._finish(uow, operation_id, action, payload, TeamMutationResult("not_expired", **base))
            status = "expired" if action == "expire" else "rejected"
            changed = uow.execute("UPDATE dungeon_team_invites SET status=?,consumed_at=CURRENT_TIMESTAMP,resolved_operation_id=? WHERE invite_id=? AND status='pending'", (status, operation_id, invite_id))
            if changed.rowcount != 1: return TeamMutationResult("state_changed", **base)
            return self._finish(uow, operation_id, action, payload, TeamMutationResult("applied", **base))

    def reject(self, operation_id: str, invite_id: str, user_id: str, group_id: str = "", now_timestamp: float = 0) -> TeamMutationResult:
        return self._resolve_invite("reject", operation_id, invite_id, user_id, group_id, now_timestamp)

    def expire(self, operation_id: str, invite_id: str, now_timestamp: float) -> TeamMutationResult:
        return self._resolve_invite("expire", operation_id, invite_id, "", "", now_timestamp)

    def pending_invite(self, user_id: str, now_timestamp: float) -> TeamInviteSnapshot | None:
        with DatabaseUnitOfWork(self.database) as uow:
            self.ensure_schema(uow)
            row = uow.query_one("SELECT invite_id,team_id,inviter_id,invitee_id,group_id,created_at,expires_at,status FROM dungeon_team_invites WHERE invitee_id=? AND status='pending' AND expires_at>? ORDER BY created_at DESC LIMIT 1", (str(user_id), float(now_timestamp)))
        return None if row is None else TeamInviteSnapshot(str(row["invite_id"]),str(row["team_id"]),str(row["inviter_id"]),str(row["invitee_id"]),str(row["group_id"]),float(row["created_at"]),float(row["expires_at"]),str(row["status"]))


__all__ = ["DungeonTeamRepository", "TeamInviteSnapshot", "TeamMutationResult"]
