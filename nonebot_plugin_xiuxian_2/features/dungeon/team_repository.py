from __future__ import annotations

from ...core.errors import DomainError

import json
from dataclasses import dataclass, replace
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
    reason: str = ""
    subject: str = ""


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
    bot_id: str = ""
    source_message_id: str = ""
    notification_scene: str = ""


@dataclass(frozen=True)
class TeamInviteBatch:
    status: str
    invites: tuple[TeamInviteSnapshot, ...] = ()
    failures: int = 0
    next_cursor: tuple[Any, Any] | None = None


@dataclass(frozen=True)
class TeamStateSnapshot:
    team_id: str
    team_name: str
    leader_id: str
    members: tuple[str, ...]
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
    reason: str = ""
    subject: str = ""


class _TeamCreateWriteRejected(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, reason, {"reason": reason, "subject": subject})


class _TeamInviteWriteRejected(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, reason, {"reason": reason, "subject": subject})


class _TeamJoinWriteRejected(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, reason, {"reason": reason, "subject": subject})


class _TeamRejectWriteRejected(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, reason, {"reason": reason, "subject": subject})


class _TeamTransferWriteRejected(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, reason, {"reason": reason, "subject": subject})


class _TeamLeaveWriteRejected(DomainError):
    def __init__(self, reason: str, subject: str) -> None:
        super().__init__(reason, reason, {"reason": reason, "subject": subject})


class DungeonTeamRepository:
    @staticmethod
    def _snapshot_reason(current, expected):
        for field, reason in (("team_id", "team_changed"), ("team_name", "team_name_changed"), ("leader_id", "team_leader_changed"), ("members", "team_members_changed"), ("version", "team_version_changed")):
            if getattr(current, field) != getattr(expected, field):
                return reason
        raise ValueError("identical team snapshots")

    MAX_INVITE_EXPIRY_BATCH = 100
    _TEAM_COLUMNS = (
        "user_id,team_id,team_name,group_id,leader,members,"
        "create_time,max_members,description,version"
    )

    def __init__(self, database: str | Path, *, game_database: str | Path | None = None) -> None:
        self.database = str(database)
        self.game_database = str(game_database or database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    def _database_exists(self) -> bool:
        return Path(self.database).is_file()

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        return {str(row["name"]) for row in uow.query_all(f"PRAGMA {schema}.table_info({table})")}

    @classmethod
    def _table_ready(
        cls, uow: DatabaseUnitOfWork, table: str, required: set[str], schema: str = "main"
    ) -> bool:
        return required <= cls._columns(uow, table, schema)

    @classmethod
    def _team_select(cls, uow: DatabaseUnitOfWork) -> str | None:
        """Build a fixed-column projection that tolerates legacy omissions."""
        columns = cls._columns(uow, "teams")
        if "user_id" not in columns:
            return None
        selected = [column for column in cls._TEAM_COLUMNS.split(",") if column in columns]
        return ",".join(selected) if selected else None

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        required = {
            "teams": {"user_id", "team_id", "team_name", "group_id", "leader", "members", "create_time", "max_members", "description", "version"},
            "dungeon_team_operations": {"operation_id", "payload", "result_status", "team_id", "result_json", "action"},
            "dungeon_team_invites": {"invite_id", "team_id", "inviter_id", "invitee_id", "group_id", "expires_at", "consumed_at", "status", "created_at", "resolved_operation_id"},
            "team_cd": {"user_id", "join_cd_until", "had_first_join"},
            "dungeon_team_exit_operations": {"operation_id", "payload", "result_json"},
            "player_dungeon_status": {"user_id", "dungeon_status"},
        }
        return all(cls._table_ready(uow, table, columns) for table, columns in required.items())

    @classmethod
    def _member_index_ready(cls, uow: DatabaseUnitOfWork, schema: str = "main") -> bool:
        if not cls._table_ready(
            uow, "dungeon_team_members", {"team_id", "member_id", "version"}, schema
        ):
            return False
        indexes = {
            str(row["name"])
            for row in uow.query_all(f"PRAGMA {schema}.index_list(dungeon_team_members)")
            if not row["partial"]
        }
        if "dungeon_team_members_member_idx" not in indexes:
            return False
        return [
            str(row["name"])
            for row in uow.query_all(f"PRAGMA {schema}.index_info(dungeon_team_members_member_idx)")
        ] == ["member_id", "team_id"]

    @classmethod
    def _mutation_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return cls._schema_ready(uow) and cls._member_index_ready(uow)

    @classmethod
    def _invite_expiry_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        if not cls._table_ready(
            uow, "dungeon_team_invites",
            {"invite_id", "team_id", "inviter_id", "invitee_id", "group_id", "created_at",
             "expires_at", "status", "consumed_at", "bot_id", "source_message_id", "notification_scene"},
        ):
            return False
        index = uow.query_one(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='dungeon_team_invites_expiry_idx' "
            "AND tbl_name='dungeon_team_invites'"
        )
        if index is None:
            return False
        normalized = " ".join(str(index["sql"] or "").split())
        return (
            normalized.endswith("WHERE status='pending' AND consumed_at IS NULL")
            and [str(row["name"]) for row in uow.query_all(
                "PRAGMA index_info(dungeon_team_invites_expiry_idx)"
            )] == ["expires_at", "invite_id"]
        )

    def due_invites(self, now_timestamp: float, *, limit: int = 100, after: tuple[Any, Any] | None = None) -> TeamInviteBatch:
        limit = min(max(int(limit), 0), self.MAX_INVITE_EXPIRY_BATCH)
        if not self._database_exists():
            return TeamInviteBatch("schema_missing")
        with DatabaseUnitOfWork(self.database, read_only=True, timeout=0) as uow:
            if not self._invite_expiry_schema_ready(uow):
                return TeamInviteBatch("schema_missing")
            continuation = ""
            parameters: tuple[Any, ...] = (float(now_timestamp),)
            if after is not None:
                continuation = "AND (expires_at,invite_id)>(?,?) "
                parameters += after
            rows = uow.query_all(
                "SELECT invite_id,team_id,inviter_id,invitee_id,group_id,created_at,"
                "expires_at,status,bot_id,source_message_id,notification_scene "
                "FROM dungeon_team_invites WHERE status='pending' AND consumed_at IS NULL "
                f"AND expires_at<=? {continuation}ORDER BY expires_at,invite_id LIMIT ?",
                parameters + (limit,),
            )
        invites = []
        failures = 0
        for row in rows:
            try:
                invites.append(self._invite_snapshot(row))
            except (TypeError, ValueError, OverflowError):
                failures += 1
        cursor = (rows[-1]["expires_at"], rows[-1]["invite_id"]) if rows and len(rows) == limit else None
        return TeamInviteBatch("ok", tuple(invites), failures, cursor)

    def _user_exists(self, uow: DatabaseUnitOfWork, user_id: str) -> bool | None:
        if Path(self.game_database).resolve() == Path(self.database).resolve():
            if not self._table_ready(uow, "user_xiuxian", {"user_id"}):
                return None
            return uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is not None
        if not Path(self.game_database).is_file():
            return None
        with DatabaseUnitOfWork(self.game_database, read_only=True) as game:
            if not self._table_ready(game, "user_xiuxian", {"user_id"}):
                return None
            return game.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is not None

    @classmethod
    def _sync_membership(
        cls, uow: DatabaseUnitOfWork, team_id: str, members: list[str], version: int
    ) -> int:
        """Keep the bounded member lookup projection aligned with teams.members."""
        uow.execute("DELETE FROM dungeon_team_members WHERE team_id=?", (str(team_id),))
        rows = [
            (str(team_id), str(member), int(version))
            for member in dict.fromkeys(members)
            if str(member).strip()
        ]
        inserted = 0
        if rows:
            write = uow.executemany(
                "INSERT INTO dungeon_team_members(team_id,member_id,version) VALUES(?,?,?)",
                rows,
            )
            inserted = max(int(write.rowcount), 0)
        return inserted

    @staticmethod
    def _members(value: Any) -> list[str]:
        try:
            value = json.loads(value or "[]") if isinstance(value, str) else value
        except (TypeError, ValueError):
            return []
        return [
            str(item) for item in value
            if type(item) in (str, int) and str(item).strip()
        ] if isinstance(value, list) else []

    @staticmethod
    def _decode_team_record(record: dict[str, Any]) -> dict[str, Any]:
        decoded = {}
        for key, value in record.items():
            if isinstance(value, str):
                try:
                    decoded[key] = json.loads(value)
                except json.JSONDecodeError:
                    decoded[key] = value
            else:
                decoded[key] = value
        return decoded

    @classmethod
    def _normalize_team_record(cls, record: dict[str, Any]) -> dict[str, Any]:
        team = cls._decode_team_record(record)
        members = team.get("members", [])
        if not isinstance(members, list):
            try:
                members = json.loads(members or "[]")
            except (json.JSONDecodeError, TypeError, ValueError):
                members = []
        if not isinstance(members, list):
            members = []
        team["members"] = [
            str(member)
            for member in members
            if type(member) in (str, int) and str(member).strip()
        ]

        leader = team.get("leader")
        if leader is None or isinstance(leader, (dict, list)):
            leader = team["members"][0] if team["members"] else ""
        team["leader"] = str(leader)

        try:
            team["max_members"] = max(int(team.get("max_members", 4)), 1)
        except (TypeError, ValueError):
            team["max_members"] = 4
        try:
            team["version"] = max(int(team.get("version", 0)), 0)
        except (TypeError, ValueError):
            team["version"] = 0
        return team

    def team_info(self, team_id: str) -> dict[str, Any] | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            projection = self._team_select(uow)
            if projection is None:
                return None
            record = uow.query_one(
                f"SELECT {projection} FROM teams WHERE user_id=?",
                (str(team_id),),
            )
        return None if record is None else self._normalize_team_record(record)

    def team_id_for_user(self, user_id: str) -> str | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_ready(uow, "teams", {"user_id", "members"}):
                return None
            if self._member_index_ready(uow):
                row = uow.query_one(
                    "SELECT team_id FROM dungeon_team_members "
                    "WHERE member_id=? ORDER BY team_id LIMIT 1",
                    (str(user_id),),
                )
            else:
                # Read-only compatibility for databases before dungeon.009.
                row = uow.query_one(
                    "SELECT t.user_id FROM teams AS t "
                    "JOIN json_each(CASE WHEN json_valid(t.members) "
                    "AND json_type(t.members)='array' THEN t.members ELSE '[]' END) AS m "
                    "WHERE m.type IN ('text','integer') AND CAST(m.value AS TEXT)=? "
                    "ORDER BY t.user_id LIMIT 1",
                    (str(user_id),),
                )
        return None if row is None else str(row.get("team_id") or row.get("user_id") or "") or None

    @staticmethod
    def _invite_snapshot(row: dict[str, Any]) -> TeamInviteSnapshot:
        expires_at = float(row["expires_at"])
        return TeamInviteSnapshot(
            invite_id=str(row["invite_id"]),
            team_id=str(row["team_id"]),
            inviter_id=str(row["inviter_id"]),
            invitee_id=str(row["invitee_id"]),
            group_id=str(row["group_id"]),
            created_at=float(row["created_at"] or expires_at - 60),
            expires_at=expires_at,
            status=str(row["status"] or "pending"),
            bot_id=str(row.get("bot_id") or ""),
            source_message_id=str(row.get("source_message_id") or ""),
            notification_scene=str(row.get("notification_scene") or ""),
        )

    def invite_by_id(self, invite_id: str) -> TeamInviteSnapshot | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_ready(
                uow,
                "dungeon_team_invites",
                {"invite_id", "team_id", "inviter_id", "invitee_id", "group_id", "created_at", "expires_at", "status"},
            ):
                return None
            routing = ""
            columns = self._columns(uow, "dungeon_team_invites")
            if {"bot_id", "source_message_id", "notification_scene"} <= columns:
                routing = ",bot_id,source_message_id,notification_scene"
            row = uow.query_one(
                "SELECT invite_id,team_id,inviter_id,invitee_id,group_id,"
                f"created_at,expires_at,status{routing} FROM dungeon_team_invites WHERE invite_id=?",
                (str(invite_id),),
            )
        return None if row is None else self._invite_snapshot(row)

    @classmethod
    def _user_team(cls, uow: DatabaseUnitOfWork, user_id: str) -> str:
        row = uow.query_one(
            "SELECT team_id FROM dungeon_team_members "
            "WHERE member_id=? ORDER BY team_id LIMIT 1",
            (str(user_id),),
        )
        return "" if row is None else str(row.get("team_id") or row.get("user_id") or "")

    @staticmethod
    def _active_session(uow: DatabaseUnitOfWork, user_id: str) -> bool:
        row = uow.query_one(
            "SELECT dungeon_status FROM player_dungeon_status WHERE user_id=?",
            (user_id,),
        )
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
                reason=str(data.get("reason") or ("legacy_rejection" if row["result_status"] == "state_changed" else "")),
                subject=str(data.get("subject", "")),
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            return TeamMutationResult(str(row["result_status"]), team_id=str(row["team_id"]), reason="receipt_corrupt")

    def _finish(self, uow: DatabaseUnitOfWork, operation_id: str, action: str, payload: str, result: TeamMutationResult, *, retry_receipt: bool = False) -> TeamMutationResult:
        if retry_receipt:
            changed = uow.execute(
                "UPDATE dungeon_team_operations SET result_status=?,team_id=?,result_json=?,action='expire' "
                "WHERE operation_id=? AND COALESCE(action,'') IN ('','expire') AND payload=? AND result_status='not_expired'",
                (result.status, result.team_id, self._json(result.__dict__), operation_id, payload),
            )
            if changed.rowcount != 1:
                raise RuntimeError("invite expiry receipt changed")
        else:
            uow.execute("INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)", (operation_id, payload, result.status, result.team_id, self._json(result.__dict__), action))
        return result

    def create(self, operation_id: str, team_id: str, team_name: str, leader_id: str, group_id: str, created_at: str, now_timestamp: float) -> TeamMutationResult:
        payload = self._json({"action":"create","team_id":str(team_id),"team_name":str(team_name),"leader_id":str(leader_id),"group_id":str(group_id)})
        if not self._database_exists():
            return TeamMutationResult("schema_missing", team_id=str(team_id), target_id=str(leader_id), group_id=str(group_id))
        base = dict(team_id=str(team_id), team_name=str(team_name), leader_id=str(leader_id), target_id=str(leader_id), group_id=str(group_id))
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._mutation_schema_ready(uow):
                    return TeamMutationResult("schema_missing", **base)
                old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                if old is not None:
                    if old["payload"] != payload:
                        return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="operation_conflict")
                    return self._decode_mutation(old)

                def finish(result: TeamMutationResult) -> TeamMutationResult:
                    receipt_payload = self._json(result.__dict__)
                    write = uow.execute(
                        "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)",
                        (operation_id, payload, result.status, result.team_id, receipt_payload, "create"),
                    )
                    if write.rowcount != 1:
                        raise _TeamCreateWriteRejected("dungeon_team_create_receipt_write_rejected", team_id)
                    receipt = uow.query_one(
                        "SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?",
                        (operation_id,),
                    )
                    if receipt != {
                        "payload": payload,
                        "result_status": result.status,
                        "team_id": result.team_id,
                        "result_json": receipt_payload,
                        "action": "create",
                    }:
                        raise _TeamCreateWriteRejected("dungeon_team_create_receipt_projection_mismatch", team_id)
                    return result

                if not group_id:
                    return finish(TeamMutationResult("group_required", **base))
                user_exists = self._user_exists(uow, leader_id)
                if user_exists is None:
                    return TeamMutationResult("schema_missing", **base)
                if not user_exists:
                    return finish(TeamMutationResult("user_missing", **base))
                if self._user_team(uow, leader_id):
                    return finish(TeamMutationResult("user_has_team", **base))
                if self._active_session(uow, leader_id):
                    return finish(TeamMutationResult("session_active", **base))
                if uow.query_one("SELECT 1 FROM teams WHERE user_id=?", (team_id,)):
                    return finish(TeamMutationResult("team_exists", **base))

                members_json = json.dumps([leader_id])
                expected_team = {
                    "user_id": str(team_id),
                    "team_id": str(team_id),
                    "team_name": str(team_name),
                    "group_id": str(group_id),
                    "leader": str(leader_id),
                    "members": members_json,
                    "create_time": str(created_at),
                    "max_members": 4,
                    "description": "",
                    "version": 0,
                }
                team_write = uow.execute(
                    "INSERT INTO teams(user_id,team_id,team_name,group_id,leader,members,create_time,max_members,description,version) VALUES(?,?,?,?,?,?,?,?,?,0)",
                    (team_id, team_id, team_name, group_id, leader_id, members_json, created_at, 4, ""),
                )
                if team_write.rowcount != 1:
                    raise _TeamCreateWriteRejected("dungeon_team_create_state_write_rejected", team_id)
                team_columns = "user_id,team_id,team_name,group_id,leader,members,create_time,max_members,description,version"
                team_state = uow.query_one(f"SELECT {team_columns} FROM teams WHERE user_id=?", (team_id,))
                if team_state != expected_team:
                    raise _TeamCreateWriteRejected("dungeon_team_create_state_projection_mismatch", team_id)

                member_write_count = self._sync_membership(uow, team_id, [leader_id], 0)
                if member_write_count != 1:
                    raise _TeamCreateWriteRejected("dungeon_team_create_members_write_rejected", team_id)
                members = uow.query_all(
                    "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY team_id,member_id",
                    (team_id,),
                )
                expected_members = [{"team_id": str(team_id), "member_id": str(leader_id), "version": 0}]
                if members != expected_members:
                    raise _TeamCreateWriteRejected("dungeon_team_create_members_projection_mismatch", team_id)

                result = TeamMutationResult("applied", member_count=1, max_members=4, subject=str(team_id), **base)
                finish(result)
                team_state = uow.query_one(f"SELECT {team_columns} FROM teams WHERE user_id=?", (team_id,))
                if team_state != expected_team:
                    raise _TeamCreateWriteRejected("dungeon_team_create_state_projection_mismatch", team_id)
                members = uow.query_all(
                    "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY team_id,member_id",
                    (team_id,),
                )
                if members != expected_members:
                    raise _TeamCreateWriteRejected("dungeon_team_create_members_projection_mismatch", team_id)
                return result
        except _TeamCreateWriteRejected as exc:
            return TeamMutationResult("failed", reason=exc.code, subject=str(exc.details.get("subject") or team_id), **base)

    def operation_result(self, operation_id: str, action: str = "") -> TeamMutationResult | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_ready(
                uow, "dungeon_team_operations", {"operation_id", "payload", "result_status", "team_id", "result_json", "action"}
            ):
                return None
            row = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        if action and str(row["action"]) != str(action):
            return TeamMutationResult("state_changed", team_id=str(row["team_id"]), reason="operation_conflict")
        result = self._decode_mutation(row)
        return TeamMutationResult(**{**result.__dict__, "status": "duplicate" if result.status == "applied" else result.status})

    def snapshot(self, team_id: str) -> TeamStateSnapshot | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            projection = self._team_select(uow)
            if projection is None:
                return None
            row = uow.query_one(
                f"SELECT {projection} FROM teams WHERE user_id=?",
                (str(team_id),),
            )
        if row is None:
            return None
        return TeamStateSnapshot(str(team_id), str(row.get("team_name") or ""), str(row.get("leader") or ""), tuple(self._members(row.get("members"))), int(row.get("version") or 0))

    def transfer(self, operation_id: str, actor_id: str, target_id: str, expected: TeamStateSnapshot | None) -> TeamMutationResult:
        if expected is None:
            return TeamMutationResult("team_missing", target_id=str(target_id))
        payload = self._json({"action":"transfer","team_id":expected.team_id,"actor_id":str(actor_id),"target_id":str(target_id)})
        if not self._database_exists():
            return TeamMutationResult("schema_missing", team_id=expected.team_id, target_id=str(target_id), version=expected.version)
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._mutation_schema_ready(uow):
                    return TeamMutationResult("schema_missing", team_id=expected.team_id, target_id=str(target_id), version=expected.version)
                old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                if old is not None:
                    return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="operation_conflict")

                team_columns = "user_id,team_id,team_name,group_id,leader,members,create_time,max_members,description,version"
                team_sql = f"SELECT {team_columns} FROM teams WHERE user_id=?"
                team_row = uow.query_one(team_sql, (expected.team_id,))
                base = dict(team_id=expected.team_id, team_name=expected.team_name, leader_id=expected.leader_id,
                            member_count=len(expected.members), target_id=str(target_id), version=expected.version)
                member_sql = "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id"
                expected_members = uow.query_all(member_sql, (expected.team_id,))
                expected_team = dict(team_row) if team_row is not None else None

                def verify_state() -> None:
                    if uow.query_one(team_sql, (expected.team_id,)) != expected_team:
                        raise _TeamTransferWriteRejected("dungeon_team_transfer_state_projection_mismatch", expected.team_id)
                    if uow.query_all(member_sql, (expected.team_id,)) != expected_members:
                        raise _TeamTransferWriteRejected("dungeon_team_transfer_members_projection_mismatch", expected.team_id)

                def finish(result: TeamMutationResult) -> TeamMutationResult:
                    receipt_json = self._json(result.__dict__)
                    write = uow.execute(
                        "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)",
                        (operation_id, payload, result.status, result.team_id, receipt_json, "transfer"),
                    )
                    if write.rowcount != 1:
                        raise _TeamTransferWriteRejected("dungeon_team_transfer_receipt_write_rejected", expected.team_id)
                    receipt = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                    if receipt != {"payload": payload, "result_status": result.status, "team_id": result.team_id,
                                  "result_json": receipt_json, "action": "transfer"}:
                        raise _TeamTransferWriteRejected("dungeon_team_transfer_receipt_projection_mismatch", expected.team_id)
                    verify_state()
                    return result

                if team_row is None:
                    return finish(TeamMutationResult("team_missing", **base))
                current = TeamStateSnapshot(expected.team_id, str(team_row["team_name"] or ""), str(team_row["leader"] or ""),
                                            tuple(self._members(team_row["members"])), int(team_row["version"] or 0))
                if current != expected:
                    return finish(TeamMutationResult("state_changed", reason=self._snapshot_reason(current, expected), **base))
                if actor_id != current.leader_id:
                    return finish(TeamMutationResult("actor_not_leader", **base))
                if target_id == actor_id:
                    return finish(TeamMutationResult("self_target", **base))
                if target_id not in current.members:
                    return finish(TeamMutationResult("target_not_member", **base))
                if any(self._active_session(uow, member) for member in current.members):
                    return finish(TeamMutationResult("session_active", **base))

                canonical_members = [{"team_id": expected.team_id, "member_id": member, "version": current.version}
                                     for member in sorted(set(current.members))]
                if expected_members != canonical_members:
                    raise _TeamTransferWriteRejected("dungeon_team_transfer_members_projection_mismatch", expected.team_id)
                changed = uow.execute("UPDATE teams SET leader=?,version=version+1 WHERE user_id=? AND version=?",
                                      (target_id, current.team_id, current.version))
                if changed.rowcount != 1:
                    raise _TeamTransferWriteRejected("dungeon_team_transfer_state_write_rejected", expected.team_id)
                expected_team.update(leader=str(target_id), version=current.version + 1)
                verify_state()

                changed_members = uow.execute("UPDATE dungeon_team_members SET version=? WHERE team_id=?",
                                              (current.version + 1, current.team_id))
                if changed_members.rowcount != len(expected_members):
                    raise _TeamTransferWriteRejected("dungeon_team_transfer_members_write_rejected", expected.team_id)
                expected_members = [{**row, "version": current.version + 1} for row in expected_members]
                verify_state()
                base.update(leader_id=str(target_id), version=current.version + 1)
                return finish(TeamMutationResult("applied", **base))
        except _TeamTransferWriteRejected as exc:
            return TeamMutationResult("failed", team_id=expected.team_id, team_name=expected.team_name,
                                      target_id=str(target_id), reason=exc.code,
                                      subject=str(exc.details.get("subject") or expected.team_id))

    def invite(self, operation_id: str, invite_id: str, team_id: str, inviter_id: str, invitee_id: str, group_id: str, expires_at: float, now_timestamp: float, *, bot_id: str = "", source_message_id: str = "", notification_scene: str = "") -> TeamMutationResult:
        payload = self._json({"action":"invite","invite_id":invite_id,"team_id":team_id,"inviter_id":inviter_id,"invitee_id":invitee_id,"group_id":group_id})
        base = dict(team_id=team_id, invite_id=invite_id, target_id=invitee_id, group_id=group_id, expires_at=float(expires_at))
        if not self._database_exists():
            return TeamMutationResult("schema_missing", **base)
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._mutation_schema_ready(uow):
                    return TeamMutationResult("schema_missing", **base)
                old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                if old is not None:
                    return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", reason="operation_conflict", **base)
                routing = bool(bot_id or source_message_id or notification_scene)
                if routing and not self._invite_expiry_schema_ready(uow):
                    return TeamMutationResult("schema_missing", **base)
                columns = "invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,consumed_at,status,created_at,resolved_operation_id"
                route_ready = {"bot_id", "source_message_id", "notification_scene"} <= self._columns(uow, "dungeon_team_invites")
                if route_ready:
                    columns += ",bot_id,source_message_id,notification_scene"
                expired = []
                expected_invite = None

                def verify_invites() -> None:
                    for expected in expired:
                        actual = uow.query_one(f"SELECT {columns} FROM dungeon_team_invites WHERE invite_id=?", (expected["invite_id"],))
                        if actual != expected:
                            raise _TeamInviteWriteRejected("dungeon_team_invite_expiry_projection_mismatch", str(expected["invite_id"]))
                    if expected_invite is not None:
                        actual = uow.query_one(f"SELECT {columns} FROM dungeon_team_invites WHERE invite_id=?", (invite_id,))
                        if actual != expected_invite:
                            raise _TeamInviteWriteRejected("dungeon_team_invite_state_projection_mismatch", invitee_id)

                def finish(result: TeamMutationResult) -> TeamMutationResult:
                    receipt_json = self._json(result.__dict__)
                    write = uow.execute(
                        "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)",
                        (operation_id, payload, result.status, result.team_id, receipt_json, "invite"),
                    )
                    if write.rowcount != 1:
                        raise _TeamInviteWriteRejected("dungeon_team_invite_receipt_write_rejected", invitee_id)
                    receipt = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                    if receipt != {"payload": payload, "result_status": result.status, "team_id": result.team_id, "result_json": receipt_json, "action": "invite"}:
                        raise _TeamInviteWriteRejected("dungeon_team_invite_receipt_projection_mismatch", invitee_id)
                    verify_invites()
                    return result

                if not group_id: return finish(TeamMutationResult("group_required", **base))
                if not invitee_id: return finish(TeamMutationResult("target_missing", **base))
                user_exists = self._user_exists(uow, invitee_id)
                if user_exists is None: return TeamMutationResult("schema_missing", **base)
                if not user_exists: return finish(TeamMutationResult("user_missing", **base))
                team = uow.query_one("SELECT team_name,leader,members,max_members,version FROM teams WHERE user_id=?", (team_id,))
                if team is None: return finish(TeamMutationResult("team_disbanded", **base))
                members, maximum = self._members(team["members"]), max(int(team["max_members"] or 4), 1)
                base.update(team_name=str(team["team_name"] or ""), leader_id=str(team["leader"] or ""), member_count=len(members), max_members=maximum, version=int(team["version"] or 0))
                if str(team["leader"]) != inviter_id: return finish(TeamMutationResult("actor_not_leader", **base))
                if len(members) >= maximum: return finish(TeamMutationResult("team_full", **base))
                if self._user_team(uow, invitee_id): return finish(TeamMutationResult("user_has_team", **base))
                if self._active_session(uow, inviter_id) or self._active_session(uow, invitee_id): return finish(TeamMutationResult("session_active", **base))

                # Keep exact old rows for rollback verification, including their original routing.
                expired = uow.query_all(
                    f"SELECT {columns} FROM dungeon_team_invites WHERE invitee_id=? AND status='pending' AND expires_at<=? ORDER BY invite_id",
                    (invitee_id, now_timestamp),
                )
                consumed_at = uow.query_one("SELECT CURRENT_TIMESTAMP AS consumed_at")["consumed_at"]
                for expected in expired:
                    write = uow.execute(
                        "UPDATE dungeon_team_invites SET status='expired',consumed_at=? WHERE invite_id=? AND status='pending' AND expires_at<=?",
                        (consumed_at, expected["invite_id"], now_timestamp),
                    )
                    if write.rowcount != 1:
                        raise _TeamInviteWriteRejected("dungeon_team_invite_expiry_write_rejected", str(expected["invite_id"]))
                    expected.update(status="expired", consumed_at=consumed_at)
                verify_invites()
                pending = uow.query_one("SELECT invite_id FROM dungeon_team_invites WHERE invitee_id=? AND status='pending' AND expires_at>?", (invitee_id, now_timestamp))
                if pending is not None:
                    return finish(TeamMutationResult("duplicate" if pending["invite_id"] == invite_id else "invite_pending", **base))
                expected_invite = dict(invite_id=invite_id, team_id=team_id, inviter_id=inviter_id, invitee_id=invitee_id,
                                       group_id=group_id, expires_at=float(expires_at), consumed_at=None, status="pending",
                                       created_at=float(now_timestamp), resolved_operation_id=None)
                if route_ready:
                    expected_invite.update(bot_id=str(bot_id), source_message_id=str(source_message_id), notification_scene=str(notification_scene))
                write = uow.execute(
                    f"INSERT INTO dungeon_team_invites({','.join(expected_invite)}) VALUES({','.join('?' for _ in expected_invite)})",
                    tuple(expected_invite.values()),
                )
                if write.rowcount != 1:
                    raise _TeamInviteWriteRejected("dungeon_team_invite_state_write_rejected", invitee_id)
                verify_invites()
                return finish(TeamMutationResult("applied", subject=invitee_id, **base))
        except _TeamInviteWriteRejected as exc:
            return TeamMutationResult("failed", reason=exc.code, subject=str(exc.details.get("subject") or invitee_id), **base)

    def join(self, operation_id: str, invite_id: str, team_id: str, inviter_id: str, user_id: str, group_id: str, now_timestamp: float) -> TeamMutationResult:
        payload = self._json({"action":"join","invite_id":invite_id,"team_id":team_id,"inviter_id":inviter_id,"user_id":user_id,"group_id":group_id})
        base = dict(team_id=team_id, invite_id=invite_id, target_id=user_id, group_id=group_id)
        if not self._database_exists():
            return TeamMutationResult("schema_missing", **base)
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._mutation_schema_ready(uow):
                    return TeamMutationResult("schema_missing", **base)
                old = uow.query_one("SELECT payload,result_status,team_id,result_json FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                if old is not None:
                    return self._decode_mutation(old) if old["payload"] == payload else TeamMutationResult("state_changed", reason="operation_conflict", **base)
                checks = []

                def verify_join() -> None:
                    for sql, params, expected, reason, subject in checks:
                        actual = uow.query_all(sql, params) if isinstance(expected, list) else uow.query_one(sql, params)
                        if actual != expected:
                            raise _TeamJoinWriteRejected(reason, subject)

                def finish(result: TeamMutationResult) -> TeamMutationResult:
                    receipt_json = self._json(result.__dict__)
                    write = uow.execute(
                        "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)",
                        (operation_id, payload, result.status, result.team_id, receipt_json, "join"),
                    )
                    if write.rowcount != 1:
                        raise _TeamJoinWriteRejected("dungeon_team_join_receipt_write_rejected", team_id)
                    receipt = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                    if receipt != {"payload": payload, "result_status": result.status, "team_id": result.team_id, "result_json": receipt_json, "action": "join"}:
                        raise _TeamJoinWriteRejected("dungeon_team_join_receipt_projection_mismatch", team_id)
                    verify_join()
                    return result

                invite_columns = "invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,status,consumed_at,created_at,resolved_operation_id"
                if {"bot_id", "source_message_id", "notification_scene"} <= self._columns(uow, "dungeon_team_invites"):
                    invite_columns += ",bot_id,source_message_id,notification_scene"
                invite_sql = f"SELECT {invite_columns} FROM dungeon_team_invites WHERE invite_id=?"
                invite = uow.query_one(invite_sql, (invite_id,))
                invite_reason = ""
                if invite is None:
                    invite_reason = "team_invite_missing"
                elif (str(invite["team_id"]), str(invite["inviter_id"]), str(invite["invitee_id"]), str(invite["group_id"])) != (team_id, inviter_id, user_id, group_id):
                    invite_reason = "invite_subject_mismatch"
                elif str(invite["status"]) != "pending" or invite["consumed_at"] is not None:
                    invite_reason = "team_invite_resolved"
                elif float(invite["expires_at"]) <= float(now_timestamp):
                    invite_reason = "team_invite_expired"
                if invite_reason:
                    return finish(TeamMutationResult("invite_invalid", reason=invite_reason, **base))
                user_exists = self._user_exists(uow, user_id)
                if user_exists is None: return TeamMutationResult("schema_missing", **base)
                if not user_exists: return finish(TeamMutationResult("user_missing", **base))
                if self._user_team(uow, user_id): return finish(TeamMutationResult("user_has_team", **base))
                team_sql = f"SELECT {self._TEAM_COLUMNS} FROM teams WHERE user_id=?"
                team = uow.query_one(team_sql, (team_id,))
                if team is None: return finish(TeamMutationResult("team_disbanded", **base))
                members, maximum = self._members(team["members"]), max(int(team["max_members"] or 4), 1)
                base.update(team_name=str(team["team_name"] or ""), leader_id=str(team["leader"] or ""), member_count=len(members), max_members=maximum, expires_at=float(invite["expires_at"]), version=int(team["version"] or 0))
                if len(members) >= maximum: return finish(TeamMutationResult("team_full", **base))
                if self._active_session(uow, user_id) or any(self._active_session(uow, member) for member in members): return finish(TeamMutationResult("session_active", **base))

                # Freeze every input before writes so an early trigger cannot redefine a later expectation.
                member_sql = "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id"
                old_members = uow.query_all(member_sql, (team_id,))
                cooldown_sql = "SELECT user_id,join_cd_until,had_first_join FROM team_cd WHERE user_id=?"
                cooldown = uow.query_one(cooldown_sql, (user_id,))
                expected_cooldown = dict(cooldown) if cooldown is not None else dict(user_id=user_id, join_cd_until="")
                expected_cooldown["had_first_join"] = 1
                consumed_at = uow.query_one("SELECT CURRENT_TIMESTAMP AS consumed_at")["consumed_at"]
                expected_invite = {**invite, "status": "joined", "consumed_at": consumed_at, "resolved_operation_id": operation_id}
                members.append(user_id)
                version = int(team["version"] or 0) + 1
                expected_team = {**team, "members": json.dumps(members), "version": version}
                expected_members = [{"team_id": team_id, "member_id": member, "version": version} for member in sorted(set(members))]

                write = uow.execute("UPDATE teams SET members=?,version=version+1 WHERE user_id=? AND version=?", (expected_team["members"], team_id, int(team["version"] or 0)))
                if write.rowcount != 1:
                    raise _TeamJoinWriteRejected("dungeon_team_join_state_write_rejected", team_id)
                checks.append((team_sql, (team_id,), expected_team, "dungeon_team_join_state_projection_mismatch", team_id))
                verify_join()

                removed = uow.execute("DELETE FROM dungeon_team_members WHERE team_id=?", (team_id,))
                if removed.rowcount != len(old_members):
                    raise _TeamJoinWriteRejected("dungeon_team_join_members_write_rejected", team_id)
                if uow.query_all(member_sql, (team_id,)):
                    raise _TeamJoinWriteRejected("dungeon_team_join_members_projection_mismatch", team_id)
                write = uow.executemany("INSERT INTO dungeon_team_members(team_id,member_id,version) VALUES(?,?,?)",
                                        [(team_id, row["member_id"], version) for row in expected_members])
                if write.rowcount != len(expected_members):
                    raise _TeamJoinWriteRejected("dungeon_team_join_members_write_rejected", team_id)
                checks.append((member_sql, (team_id,), expected_members, "dungeon_team_join_members_projection_mismatch", team_id))
                verify_join()

                write = uow.execute("INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES(?,?,1) ON CONFLICT(user_id) DO UPDATE SET had_first_join=1", (user_id, ""))
                if write.rowcount != 1:
                    raise _TeamJoinWriteRejected("dungeon_team_join_cooldown_write_rejected", user_id)
                checks.append((cooldown_sql, (user_id,), expected_cooldown, "dungeon_team_join_cooldown_projection_mismatch", user_id))
                verify_join()

                write = uow.execute("UPDATE dungeon_team_invites SET consumed_at=?,status='joined',resolved_operation_id=? WHERE invite_id=? AND status='pending'", (consumed_at, operation_id, invite_id))
                if write.rowcount != 1:
                    raise _TeamJoinWriteRejected("dungeon_team_join_invite_write_rejected", invite_id)
                checks.append((invite_sql, (invite_id,), expected_invite, "dungeon_team_join_invite_projection_mismatch", invite_id))
                verify_join()
                base.update(member_count=len(members), version=version)
                return finish(TeamMutationResult("applied", subject=team_id, **base))
        except _TeamJoinWriteRejected as exc:
            return TeamMutationResult("failed", reason=exc.code, subject=str(exc.details.get("subject") or team_id), **base)

    def _resolve_invite(self, action: str, operation_id: str, invite_id: str, user_id: str, group_id: str, now_timestamp: float, *, lock_timeout: float = 30) -> TeamMutationResult:
        payload = self._json({"action":action,"invite_id":invite_id,"user_id":user_id,"group_id":group_id})
        if not self._database_exists():
            return TeamMutationResult("schema_missing", invite_id=str(invite_id), target_id=str(user_id), group_id=str(group_id))
        with DatabaseUnitOfWork(self.database, immediate=True, timeout=lock_timeout) as uow:
            if not self._table_ready(uow, "dungeon_team_operations", {"operation_id", "payload", "result_status", "team_id", "result_json", "action"}):
                return TeamMutationResult("schema_missing", invite_id=str(invite_id), target_id=str(user_id), group_id=str(group_id))
            old = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
            retry_receipt = False
            if old is not None:
                if old["payload"] != payload or str(old["action"] or "") not in {"", action}:
                    return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="operation_conflict")
                try:
                    stored = json.loads(str(old["result_json"] or "{}"))
                except (TypeError, ValueError):
                    return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="receipt_corrupt")
                if not isinstance(stored, dict) or str(stored.get("status", old["result_status"])) != str(old["result_status"]):
                    return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="receipt_corrupt")
                previous = self._decode_mutation(old)
                retry_receipt = action == "expire" and previous.status == "not_expired"
                if not retry_receipt:
                    return replace(previous, status="duplicate") if action == "expire" and previous.status == "applied" else previous
            if not self._mutation_schema_ready(uow):
                return TeamMutationResult("schema_missing", invite_id=str(invite_id), target_id=str(user_id), group_id=str(group_id))
            invite = uow.query_one("SELECT team_id,inviter_id,invitee_id,group_id,expires_at,status,consumed_at FROM dungeon_team_invites WHERE invite_id=?", (invite_id,))
            base = dict(invite_id=invite_id, target_id=user_id)
            if invite is None or (action != "expire" and str(invite["invitee_id"]) != user_id): return self._finish(uow, operation_id, action, payload, TeamMutationResult("invite_invalid", **base), retry_receipt=retry_receipt)
            base.update(team_id=str(invite["team_id"]), leader_id=str(invite["inviter_id"]), group_id=str(invite["group_id"]), expires_at=float(invite["expires_at"]))
            if group_id and str(invite["group_id"]) != str(group_id): return self._finish(uow, operation_id, action, payload, TeamMutationResult("wrong_group", **base), retry_receipt=retry_receipt)
            if str(invite["status"]) != "pending" or invite["consumed_at"] is not None: return self._finish(uow, operation_id, action, payload, TeamMutationResult("invite_invalid", **base), retry_receipt=retry_receipt)
            if action == "expire" and float(now_timestamp) < float(invite["expires_at"]):
                return TeamMutationResult("not_expired", **base)
            status = "expired" if action == "expire" else "rejected"
            changed = uow.execute("UPDATE dungeon_team_invites SET status=?,consumed_at=CURRENT_TIMESTAMP,resolved_operation_id=? WHERE invite_id=? AND status='pending'", (status, operation_id, invite_id))
            if changed.rowcount != 1: raise DomainError("team_invite_write_rejected", "team mutation write count mismatch")
            return self._finish(uow, operation_id, action, payload, TeamMutationResult("applied", **base), retry_receipt=retry_receipt)

    def reject(self, operation_id: str, invite_id: str, user_id: str, group_id: str = "", now_timestamp: float = 0) -> TeamMutationResult:
        payload = self._json({"action": "reject", "invite_id": invite_id, "user_id": user_id, "group_id": group_id})
        base = dict(invite_id=invite_id, target_id=user_id, group_id=group_id)
        if not self._database_exists():
            return TeamMutationResult("schema_missing", **base)
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._table_ready(uow, "dungeon_team_operations", {"operation_id", "payload", "result_status", "team_id", "result_json", "action"}):
                    return TeamMutationResult("schema_missing", **base)
                old = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                if old is not None:
                    if old["payload"] != payload or str(old["action"] or "") not in {"", "reject"}:
                        return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="operation_conflict")
                    try:
                        stored = json.loads(str(old["result_json"] or "{}"))
                    except (TypeError, ValueError):
                        return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="receipt_corrupt")
                    if not isinstance(stored, dict) or str(stored.get("status", old["result_status"])) != str(old["result_status"]):
                        return TeamMutationResult("state_changed", team_id=str(old["team_id"]), reason="receipt_corrupt")
                    return self._decode_mutation(old)
                if not self._mutation_schema_ready(uow):
                    return TeamMutationResult("schema_missing", **base)
                columns = "invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,status,consumed_at,created_at,resolved_operation_id"
                if {"bot_id", "source_message_id", "notification_scene"} <= self._columns(uow, "dungeon_team_invites"):
                    columns += ",bot_id,source_message_id,notification_scene"
                invite_sql = f"SELECT {columns} FROM dungeon_team_invites WHERE invite_id=?"
                invite = uow.query_one(invite_sql, (invite_id,))
                expected_invite = dict(invite) if invite is not None else None

                def verify_invite() -> None:
                    if uow.query_one(invite_sql, (invite_id,)) != expected_invite:
                        raise _TeamRejectWriteRejected("dungeon_team_reject_state_projection_mismatch", invite_id)

                def finish(result: TeamMutationResult) -> TeamMutationResult:
                    receipt_json = self._json(result.__dict__)
                    write = uow.execute(
                        "INSERT INTO dungeon_team_operations(operation_id,payload,result_status,team_id,result_json,action) VALUES(?,?,?,?,?,?)",
                        (operation_id, payload, result.status, result.team_id, receipt_json, "reject"),
                    )
                    if write.rowcount != 1:
                        raise _TeamRejectWriteRejected("dungeon_team_reject_receipt_write_rejected", invite_id)
                    receipt = uow.query_one("SELECT payload,result_status,team_id,result_json,action FROM dungeon_team_operations WHERE operation_id=?", (operation_id,))
                    if receipt != {"payload": payload, "result_status": result.status, "team_id": result.team_id, "result_json": receipt_json, "action": "reject"}:
                        raise _TeamRejectWriteRejected("dungeon_team_reject_receipt_projection_mismatch", invite_id)
                    verify_invite()
                    return result

                if invite is None:
                    return finish(TeamMutationResult("invite_invalid", reason="team_invite_missing", subject=invite_id, **base))
                if str(invite["invitee_id"]) != user_id:
                    return finish(TeamMutationResult("invite_invalid", reason="invite_subject_mismatch", subject=invite_id, **base))
                base.update(team_id=str(invite["team_id"]), leader_id=str(invite["inviter_id"]), group_id=str(invite["group_id"]), expires_at=float(invite["expires_at"]))
                if group_id and str(invite["group_id"]) != str(group_id):
                    return finish(TeamMutationResult("wrong_group", **base))
                if str(invite["status"]) != "pending" or invite["consumed_at"] is not None:
                    return finish(TeamMutationResult("invite_invalid", reason="team_invite_resolved", subject=invite_id, **base))
                consumed_at = uow.query_one("SELECT CURRENT_TIMESTAMP AS consumed_at")["consumed_at"]
                expected_invite = {**invite, "status": "rejected", "consumed_at": consumed_at, "resolved_operation_id": operation_id}
                write = uow.execute(
                    "UPDATE dungeon_team_invites SET status='rejected',consumed_at=?,resolved_operation_id=? WHERE invite_id=? AND status='pending'",
                    (consumed_at, operation_id, invite_id),
                )
                if write.rowcount != 1:
                    raise _TeamRejectWriteRejected("dungeon_team_reject_state_write_rejected", invite_id)
                verify_invite()
                return finish(TeamMutationResult("applied", subject=invite_id, **base))
        except _TeamRejectWriteRejected as exc:
            return TeamMutationResult("failed", reason=exc.code, subject=str(exc.details.get("subject") or invite_id), **base)

    def expire(self, operation_id: str, invite_id: str, now_timestamp: float, *, lock_timeout: float = 30) -> TeamMutationResult:
        return self._resolve_invite("expire", operation_id, invite_id, "", "", now_timestamp, lock_timeout=lock_timeout)

    def exit_operation_result(self, operation_id: str, action: str, actor_id: str, target_id: str | None = None) -> TeamExitResult | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_ready(
                uow, "dungeon_team_exit_operations", {"operation_id", "payload", "result_json"}
            ):
                return None
            row = uow.query_one("SELECT payload,result_json FROM dungeon_team_exit_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload"]))
            data = json.loads(str(row["result_json"]))
            if payload.get("action") != action or str(payload.get("actor_id")) != str(actor_id) or (target_id is not None and str(payload.get("target_id")) != str(target_id)):
                return TeamExitResult("state_changed", reason="operation_conflict")
            data["cooldown_members"] = tuple(data.get("cooldown_members") or ())
            data["status"] = "duplicate" if data.get("status") == "applied" else data.get("status", "state_changed")
            return TeamExitResult(**data)
        except (TypeError, ValueError, json.JSONDecodeError):
            return TeamExitResult("state_changed", reason="receipt_corrupt")

    def _finish_exit(self, uow: DatabaseUnitOfWork, operation_id: str, payload: str, result: TeamExitResult) -> TeamExitResult:
        uow.execute("INSERT INTO dungeon_team_exit_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, json.dumps(result.__dict__, ensure_ascii=True, default=list)))
        return result

    def _exit_mutation(self, action: str, operation_id: str, actor_id: str, target_id: str, expected: TeamStateSnapshot | None, cooldown_until: str) -> TeamExitResult:
        if expected is None:
            return TeamExitResult("team_missing", target_id=target_id)
        payload = self._json({"action": action, "actor_id": str(actor_id), "target_id": str(target_id), "team_id": expected.team_id})
        if not self._database_exists():
            return TeamExitResult("schema_missing", expected.team_id, expected.team_name, target_id=target_id)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._mutation_schema_ready(uow):
                return TeamExitResult("schema_missing", expected.team_id, expected.team_name, target_id=target_id)
            old = uow.query_one("SELECT payload,result_json FROM dungeon_team_exit_operations WHERE operation_id=?", (operation_id,))
            if old is not None:
                if str(old["payload"]) != payload:
                    return TeamExitResult("state_changed", reason="operation_conflict")
                data = json.loads(str(old["result_json"]))
                data["cooldown_members"] = tuple(data.get("cooldown_members") or ())
                data["status"] = "duplicate" if data.get("status") == "applied" else data.get("status", "state_changed")
                if data["status"] == "state_changed" and not data.get("reason"):
                    data["reason"] = "legacy_rejection"
                return TeamExitResult(**data)
            row = uow.query_one("SELECT team_name,leader,members,version FROM teams WHERE user_id=?", (expected.team_id,))
            if row is None: return self._finish_exit(uow, operation_id, payload, TeamExitResult("team_missing", expected.team_id, expected.team_name, target_id=target_id))
            current = TeamStateSnapshot(expected.team_id, str(row["team_name"] or ""), str(row["leader"] or ""), tuple(self._members(row["members"])), int(row["version"] or 0))
            if current != expected: return self._finish_exit(uow, operation_id, payload, TeamExitResult("state_changed", expected.team_id, expected.team_name, target_id=target_id, reason=self._snapshot_reason(current, expected)))
            if actor_id not in current.members: return self._finish_exit(uow, operation_id, payload, TeamExitResult("actor_not_member", current.team_id, current.team_name, target_id=target_id))
            if action in {"kick", "disband"} and actor_id != current.leader_id: return self._finish_exit(uow, operation_id, payload, TeamExitResult("actor_not_leader", current.team_id, current.team_name, target_id=target_id))
            if action in {"leave", "kick"} and target_id not in current.members: return self._finish_exit(uow, operation_id, payload, TeamExitResult("target_not_member", current.team_id, current.team_name, target_id=target_id))
            if action == "kick" and actor_id == target_id: return self._finish_exit(uow, operation_id, payload, TeamExitResult("self_target", current.team_id, current.team_name, target_id=target_id))
            if any(self._active_session(uow, member) for member in current.members): return self._finish_exit(uow, operation_id, payload, TeamExitResult("session_active", current.team_id, current.team_name, target_id=target_id))
            members = list(current.members); affected = list(members) if action == "disband" else [target_id]; disbanded = action == "disband" or (action == "leave" and len(members) == 1); new_leader = ""
            if disbanded: changed = uow.execute("DELETE FROM teams WHERE user_id=? AND version=?", (current.team_id, current.version))
            else:
                members.remove(target_id); new_leader = members[0] if target_id == current.leader_id else current.leader_id; changed = uow.execute("UPDATE teams SET members=?,leader=?,version=version+1 WHERE user_id=? AND version=?", (json.dumps(members), new_leader, current.team_id, current.version))
            if changed.rowcount != 1: raise DomainError("team_members_write_rejected", "team exit write count mismatch")
            if disbanded:
                uow.execute("DELETE FROM dungeon_team_members WHERE team_id=?", (current.team_id,))
            else:
                self._sync_membership(uow, current.team_id, members, current.version + 1)
            cooldown_members = []
            for member in affected:
                first = uow.query_one("SELECT had_first_join FROM team_cd WHERE user_id=?", (member,))
                if first and int(first["had_first_join"] or 0) == 1:
                    uow.execute("INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES(?,?,1) ON CONFLICT(user_id) DO UPDATE SET join_cd_until=excluded.join_cd_until", (member, str(cooldown_until))); cooldown_members.append(member)
            uow.execute("UPDATE dungeon_team_invites SET consumed_at=CURRENT_TIMESTAMP,status='team_changed',resolved_operation_id=? WHERE status='pending' AND team_id=?", (operation_id, current.team_id))
            return self._finish_exit(uow, operation_id, payload, TeamExitResult("applied", current.team_id, current.team_name, new_leader, disbanded, tuple(cooldown_members), str(cooldown_until), current.version + 1, target_id))

    def leave(self, operation_id: str, actor_id: str, expected: TeamStateSnapshot | None, cooldown_until: str) -> TeamExitResult:
        if expected is None:
            return TeamExitResult("team_missing", target_id=actor_id)
        base = dict(team_id=expected.team_id, team_name=expected.team_name, target_id=actor_id)
        payload = self._json({"action": "leave", "actor_id": str(actor_id), "target_id": str(actor_id), "team_id": expected.team_id})
        if not self._database_exists():
            return TeamExitResult("schema_missing", **base)
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._mutation_schema_ready(uow):
                    return TeamExitResult("schema_missing", **base)
                old = uow.query_one("SELECT payload,result_json FROM dungeon_team_exit_operations WHERE operation_id=?", (operation_id,))
                if old is not None:
                    if old["payload"] != payload:
                        return TeamExitResult("state_changed", reason="operation_conflict")
                    try:
                        data = json.loads(str(old["result_json"]))
                        if not isinstance(data, dict):
                            return TeamExitResult("state_changed", reason="receipt_corrupt")
                        data["cooldown_members"] = tuple(data.get("cooldown_members") or ())
                        data["status"] = "duplicate" if data.get("status") == "applied" else data.get("status", "state_changed")
                        if data["status"] == "state_changed" and not data.get("reason"):
                            data["reason"] = "legacy_rejection"
                        return TeamExitResult(**data)
                    except (TypeError, ValueError):
                        return TeamExitResult("state_changed", reason="receipt_corrupt")

                # Freeze all inputs before any write can affect a later business decision.
                team_sql = f"SELECT {self._TEAM_COLUMNS} FROM teams WHERE user_id=?"
                team = uow.query_one(team_sql, (expected.team_id,))
                expected_team = dict(team) if team is not None else None
                member_sql = "SELECT team_id,member_id,version FROM dungeon_team_members WHERE team_id=? ORDER BY member_id"
                expected_members = uow.query_all(member_sql, (expected.team_id,))
                cooldown_sql = "SELECT user_id,join_cd_until,had_first_join FROM team_cd WHERE user_id=?"
                cooldown = uow.query_one(cooldown_sql, (actor_id,))
                expected_cooldown = dict(cooldown) if cooldown is not None else None
                invite_columns = "invite_id,team_id,inviter_id,invitee_id,group_id,expires_at,status,consumed_at,created_at,resolved_operation_id"
                if {"bot_id", "source_message_id", "notification_scene"} <= self._columns(uow, "dungeon_team_invites"):
                    invite_columns += ",bot_id,source_message_id,notification_scene"
                invite_sql = f"SELECT {invite_columns} FROM dungeon_team_invites WHERE team_id=? ORDER BY invite_id"
                expected_invites = uow.query_all(invite_sql, (expected.team_id,))
                pending_ids = [row["invite_id"] for row in expected_invites if row["status"] == "pending"]

                def verify_state() -> None:
                    if uow.query_one(team_sql, (expected.team_id,)) != expected_team:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_state_projection_mismatch", expected.team_id)
                    if uow.query_all(member_sql, (expected.team_id,)) != expected_members:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_members_projection_mismatch", expected.team_id)
                    if uow.query_one(cooldown_sql, (actor_id,)) != expected_cooldown:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_cooldown_projection_mismatch", actor_id)
                    actual_invites = uow.query_all(invite_sql, (expected.team_id,))
                    if actual_invites != expected_invites:
                        actual_by_id = {row["invite_id"]: row for row in actual_invites}
                        subject = next((row["invite_id"] for row in expected_invites
                                        if actual_by_id.get(row["invite_id"]) != row), expected.team_id)
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_invite_projection_mismatch", subject)

                def finish(result: TeamExitResult) -> TeamExitResult:
                    receipt_json = json.dumps(result.__dict__, ensure_ascii=True, default=list)
                    write = uow.execute("INSERT INTO dungeon_team_exit_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                                        (operation_id, payload, receipt_json))
                    if write.rowcount != 1:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_receipt_write_rejected", expected.team_id)
                    receipt = uow.query_one("SELECT payload,result_json FROM dungeon_team_exit_operations WHERE operation_id=?", (operation_id,))
                    if receipt != {"payload": payload, "result_json": receipt_json}:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_receipt_projection_mismatch", expected.team_id)
                    verify_state()
                    return result

                if team is None:
                    return finish(TeamExitResult("team_missing", **base))
                current = TeamStateSnapshot(expected.team_id, str(team["team_name"] or ""), str(team["leader"] or ""),
                                            tuple(self._members(team["members"])), int(team["version"] or 0))
                if current != expected:
                    return finish(TeamExitResult("state_changed", reason=self._snapshot_reason(current, expected), subject=expected.team_id, **base))
                if actor_id not in current.members:
                    return finish(TeamExitResult("actor_not_member", **base))
                if any(self._active_session(uow, member) for member in current.members):
                    return finish(TeamExitResult("session_active", **base))

                canonical_members = [{"team_id": expected.team_id, "member_id": member, "version": current.version}
                                     for member in sorted(set(current.members))]
                if expected_members != canonical_members:
                    raise _TeamLeaveWriteRejected("dungeon_team_leave_members_projection_mismatch", expected.team_id)
                disbanded = len(current.members) == 1
                members = list(current.members)
                members.remove(actor_id)
                new_leader = "" if disbanded else members[0] if actor_id == current.leader_id else current.leader_id
                consumed_at = uow.query_one("SELECT CURRENT_TIMESTAMP AS consumed_at")["consumed_at"]
                if disbanded:
                    changed = uow.execute("DELETE FROM teams WHERE user_id=? AND version=?", (current.team_id, current.version))
                    expected_team = None
                else:
                    changed = uow.execute("UPDATE teams SET members=?,leader=?,version=version+1 WHERE user_id=? AND version=?",
                                          (json.dumps(members), new_leader, current.team_id, current.version))
                    expected_team.update(members=json.dumps(members), leader=new_leader, version=current.version + 1)
                if changed.rowcount != 1:
                    raise _TeamLeaveWriteRejected("dungeon_team_leave_state_write_rejected", expected.team_id)
                verify_state()

                removed = uow.execute("DELETE FROM dungeon_team_members WHERE team_id=?", (current.team_id,))
                if removed.rowcount != len(expected_members):
                    raise _TeamLeaveWriteRejected("dungeon_team_leave_members_write_rejected", expected.team_id)
                expected_members = []
                verify_state()
                if members:
                    new_members = [{"team_id": current.team_id, "member_id": member, "version": current.version + 1}
                                   for member in sorted(set(members))]
                    inserted = uow.executemany("INSERT INTO dungeon_team_members(team_id,member_id,version) VALUES(?,?,?)",
                                              [(row["team_id"], row["member_id"], row["version"]) for row in new_members])
                    if inserted.rowcount != len(new_members):
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_members_write_rejected", expected.team_id)
                    expected_members = new_members
                    verify_state()

                cooldown_members = ()
                if cooldown and int(cooldown["had_first_join"] or 0) == 1:
                    changed = uow.execute("INSERT INTO team_cd(user_id,join_cd_until,had_first_join) VALUES(?,?,1) "
                                          "ON CONFLICT(user_id) DO UPDATE SET join_cd_until=excluded.join_cd_until",
                                          (actor_id, str(cooldown_until)))
                    if changed.rowcount != 1:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_cooldown_write_rejected", actor_id)
                    expected_cooldown.update(join_cd_until=str(cooldown_until))
                    cooldown_members = (actor_id,)
                    verify_state()

                for invite_id in pending_ids:
                    changed = uow.execute("UPDATE dungeon_team_invites SET consumed_at=?,status='team_changed',resolved_operation_id=? "
                                          "WHERE invite_id=? AND status='pending'", (consumed_at, operation_id, invite_id))
                    if changed.rowcount != 1:
                        raise _TeamLeaveWriteRejected("dungeon_team_leave_invite_write_rejected", str(invite_id))
                    for row in expected_invites:
                        if row["invite_id"] == invite_id:
                            row.update(consumed_at=consumed_at, status="team_changed", resolved_operation_id=operation_id)
                    verify_state()
                return finish(TeamExitResult("applied", new_leader_id=new_leader, disbanded=disbanded,
                                             cooldown_members=cooldown_members, cooldown_until=str(cooldown_until),
                                             version=current.version + 1, **base))
        except _TeamLeaveWriteRejected as exc:
            return TeamExitResult("failed", reason=exc.code, subject=str(exc.details.get("subject") or expected.team_id), **base)

    def kick(self, operation_id: str, actor_id: str, target_id: str, expected: TeamStateSnapshot | None, cooldown_until: str) -> TeamExitResult:
        return self._exit_mutation("kick", operation_id, actor_id, target_id, expected, cooldown_until)

    def disband(self, operation_id: str, actor_id: str, expected: TeamStateSnapshot | None, cooldown_until: str) -> TeamExitResult:
        return self._exit_mutation("disband", operation_id, actor_id, "", expected, cooldown_until)

    def pending_invite(self, user_id: str, now_timestamp: float) -> TeamInviteSnapshot | None:
        if not self._database_exists():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._table_ready(
                uow,
                "dungeon_team_invites",
                {"invite_id", "team_id", "inviter_id", "invitee_id", "group_id", "created_at", "expires_at", "status"},
            ):
                return None
            row = uow.query_one("SELECT invite_id,team_id,inviter_id,invitee_id,group_id,created_at,expires_at,status FROM dungeon_team_invites WHERE invitee_id=? AND status='pending' AND expires_at>? ORDER BY created_at DESC LIMIT 1", (str(user_id), float(now_timestamp)))
        return None if row is None else self._invite_snapshot(row)


__all__ = ["DungeonTeamRepository", "TeamInviteSnapshot", "TeamMutationResult"]
