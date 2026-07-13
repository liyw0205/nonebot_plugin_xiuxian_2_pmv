from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import ensure_player_field


@dataclass(frozen=True)
class PartnerBindResult:
    status: str
    bind_time: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerBindService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        invitee_id,
        inviter_id,
        *,
        bind_time,
        expected_invitee_partner=None,
        expected_inviter_partner=None,
    ) -> PartnerBindResult:
        operation_id = str(operation_id).strip()
        invitee_id, inviter_id = str(invitee_id), str(inviter_id)
        bind_time = str(bind_time).strip()
        expected_invitee_partner = self._partner_value(expected_invitee_partner)
        expected_inviter_partner = self._partner_value(expected_inviter_partner)
        if not operation_id or not bind_time or invitee_id == inviter_id:
            raise ValueError("invalid partner bind operation")
        payload = json.dumps(
            [invitee_id, inviter_id, bind_time, expected_invitee_partner, expected_inviter_partner],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_partner_table(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_bind_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,bind_time TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,bind_time FROM partner_bind_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PartnerBindResult(
                        "duplicate" if str(previous[0]) == payload else "operation_conflict", str(previous[1])
                    )
                users = conn.execute(
                    "SELECT user_id FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (invitee_id, inviter_id),
                ).fetchall()
                if {str(row[0]) for row in users} != {invitee_id, inviter_id}:
                    conn.rollback()
                    return PartnerBindResult("user_missing")
                actual = {
                    user_id: self._current_partner(conn, user_id)
                    for user_id in (invitee_id, inviter_id)
                }
                if actual != {
                    invitee_id: expected_invitee_partner,
                    inviter_id: expected_inviter_partner,
                }:
                    conn.rollback()
                    return PartnerBindResult("state_changed")
                for user_id, partner_id in ((invitee_id, inviter_id), (inviter_id, invitee_id)):
                    changed = conn.execute(
                        "UPDATE player_data.partner SET partner_id=%s,bind_time=%s,affection=0 "
                        "WHERE user_id=%s",
                        (partner_id, bind_time, user_id),
                    )
                    if changed.rowcount == 0:
                        conn.execute(
                            "INSERT INTO player_data.partner(user_id,partner_id,bind_time,affection) VALUES(%s,%s,%s,0)",
                            (user_id, partner_id, bind_time),
                        )
                conn.execute(
                    "INSERT INTO partner_bind_operations(operation_id,payload,bind_time) VALUES(%s,%s,%s)",
                    (operation_id, payload, bind_time),
                )
                conn.commit()
                return PartnerBindResult("applied", bind_time)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass

    @staticmethod
    def _partner_value(value):
        if value is None or str(value).strip().lower() in {"", "none", "null"}:
            return None
        return str(value)

    @classmethod
    def _current_partner(cls, conn, user_id):
        row = conn.execute(
            "SELECT partner_id FROM player_data.partner WHERE user_id=%s", (user_id,)
        ).fetchone()
        return None if row is None else cls._partner_value(row[0])

    @staticmethod
    def _ensure_partner_table(conn):
        ensure_player_field(conn, "partner", "partner_id", "TEXT")
        ensure_player_field(conn, "partner", "bind_time", "TEXT")
        ensure_player_field(conn, "partner", "affection", "INTEGER")


__all__ = ["PartnerBindResult", "PartnerBindService"]
