from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Callable

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import append_mentor_history, ensure_player_field, get_json_field, increment_stat, set_field


@dataclass(frozen=True)
class MentorBindResult:
    status: str
    bind_time: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MentorBindService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        mentor_id,
        apprentice_id,
        invite_id,
        *,
        bind_time,
        expected_mentor_level,
        expected_apprentice_level,
        max_apprentices,
        history_limit,
        mentor_desc,
        apprentice_desc,
        invitation_validator: Callable[[str, str, str], bool] | None = None,
        now: datetime | None = None,
    ) -> MentorBindResult:
        operation_id = str(operation_id).strip()
        mentor_id, apprentice_id, invite_id = str(mentor_id), str(apprentice_id), str(invite_id)
        bind_time = str(bind_time)
        max_apprentices, history_limit = int(max_apprentices), int(history_limit)
        if not operation_id or mentor_id == apprentice_id or not invite_id or max_apprentices <= 0 or history_limit <= 0:
            raise ValueError("invalid mentor bind operation")
        payload = json.dumps(
            [mentor_id, apprentice_id, invite_id, bind_time, str(expected_mentor_level), str(expected_apprentice_level),
             max_apprentices, history_limit, mentor_desc, apprentice_desc],
            ensure_ascii=False, separators=(",", ":"),
        )
        check_time = now or datetime.now()
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mentor_bind_operations (operation_id TEXT PRIMARY KEY,"
                    "payload TEXT NOT NULL,bind_time TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,bind_time FROM mentor_bind_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    status = "duplicate" if str(previous[0]) == payload else "operation_conflict"
                    return MentorBindResult(status, str(previous[1]))
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS player_data.mentor_applications ("
                    "invite_id TEXT PRIMARY KEY,mentor_id TEXT NOT NULL,apprentice_id TEXT NOT NULL,"
                    "status TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL,resolved_at REAL)"
                )
                application = conn.execute(
                    "SELECT status,expires_at FROM player_data.mentor_applications "
                    "WHERE invite_id=%s AND mentor_id=%s AND apprentice_id=%s",
                    (invite_id, mentor_id, apprentice_id),
                ).fetchone()
                if application is None:
                    valid_legacy = invitation_validator and invitation_validator(mentor_id, apprentice_id, invite_id)
                    if not valid_legacy:
                        conn.rollback()
                        return MentorBindResult("invitation_changed", bind_time)
                elif str(application[0]) != "pending" or float(application[1]) <= check_time.timestamp():
                    conn.rollback()
                    return MentorBindResult("invitation_changed", bind_time)
                users = conn.execute(
                    "SELECT user_id,level FROM user_xiuxian WHERE user_id IN (%s,%s)", (mentor_id, apprentice_id)
                ).fetchall()
                levels = {str(row[0]): str(row[1]) for row in users}
                if levels != {mentor_id: str(expected_mentor_level), apprentice_id: str(expected_apprentice_level)}:
                    conn.rollback()
                    return MentorBindResult("state_changed", bind_time)
                mentor_parent = conn.execute(
                    "SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (mentor_id,)
                ).fetchone()
                apprentice_parent = conn.execute(
                    "SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)
                ).fetchone()
                if apprentice_parent is not None and apprentice_parent[0] not in (None, ""):
                    conn.rollback()
                    return MentorBindResult("already_bound", bind_time)
                if mentor_parent is not None and str(mentor_parent[0]) == apprentice_id:
                    conn.rollback()
                    return MentorBindResult("state_changed", bind_time)
                ensure_player_field(conn, "mentor", "mentor_cd_until")
                ensure_player_field(conn, "mentor", "apprentice_cd_until")
                mentor_cd_row = conn.execute(
                    "SELECT mentor_cd_until FROM player_data.mentor WHERE user_id=%s", (mentor_id,)
                ).fetchone()
                apprentice_cd_row = conn.execute(
                    "SELECT apprentice_cd_until FROM player_data.mentor WHERE user_id=%s", (apprentice_id,)
                ).fetchone()
                mentor_cd = mentor_cd_row[0] if mentor_cd_row is not None else None
                apprentice_cd = apprentice_cd_row[0] if apprentice_cd_row is not None else None
                rebind = get_json_field(conn, "mentor", apprentice_id, "mentor_rebind_cd", {})
                if _active(mentor_cd, check_time) or _active(apprentice_cd, check_time) or _active(rebind.get(mentor_id), check_time):
                    conn.rollback()
                    return MentorBindResult("cooldown_active", bind_time)
                apprentices = [str(value) for value in get_json_field(conn, "mentor", mentor_id, "apprentice_ids", [])]
                valid = []
                for user_id in apprentices:
                    row = conn.execute("SELECT mentor_id FROM player_data.mentor WHERE user_id=%s", (user_id,)).fetchone()
                    if row is not None and str(row[0]) == mentor_id:
                        valid.append(user_id)
                if apprentice_id not in valid and len(valid) >= max_apprentices:
                    conn.rollback()
                    return MentorBindResult("capacity_reached", bind_time)
                set_field(conn, "mentor", mentor_id, "apprentice_ids", [*valid, apprentice_id])
                set_field(conn, "mentor", apprentice_id, "mentor_id", mentor_id)
                set_field(conn, "mentor", apprentice_id, "bind_time", bind_time)
                set_field(conn, "mentor", apprentice_id, "breakthrough_reward_count", 0, "INTEGER")
                increment_stat(conn, mentor_id, "收徒次数", 1)
                increment_stat(conn, apprentice_id, "拜师次数", 1)
                append_mentor_history(conn, mentor_id, "bind", apprentice_id, mentor_desc, history_limit)
                append_mentor_history(conn, apprentice_id, "bind", mentor_id, apprentice_desc, history_limit)
                if application is not None:
                    consumed = conn.execute(
                        "UPDATE player_data.mentor_applications SET status='accepted',resolved_at=%s "
                        "WHERE invite_id=%s AND status='pending'",
                        (check_time.timestamp(), invite_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback()
                        return MentorBindResult("invitation_changed", bind_time)
                conn.execute(
                    "INSERT INTO mentor_bind_operations (operation_id,payload,bind_time) VALUES (%s,%s,%s)",
                    (operation_id, payload, bind_time),
                )
                conn.commit()
                return MentorBindResult("applied", bind_time)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


def _active(value, now: datetime) -> bool:
    if value in (None, ""):
        return False
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S") > now
    except (TypeError, ValueError):
        return True


__all__ = ["MentorBindResult", "MentorBindService"]
