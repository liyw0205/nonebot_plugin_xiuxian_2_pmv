from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import ensure_player_field


@dataclass(frozen=True)
class PartnerUnbindResult:
    status: str
    partner_id: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerUnbindService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self,
        operation_id,
        user_id,
        partner_id,
        *,
        expected_user_bind_time,
        expected_partner_bind_time,
        expected_user_affection,
        expected_partner_affection,
        checked_at,
        minimum_days=7,
    ) -> PartnerUnbindResult:
        operation_id = str(operation_id).strip()
        user_id, partner_id = str(user_id), str(partner_id)
        expected_user_bind_time = self._text_or_none(expected_user_bind_time)
        expected_partner_bind_time = self._text_or_none(expected_partner_bind_time)
        expected_user_affection = int(expected_user_affection or 0)
        expected_partner_affection = int(expected_partner_affection or 0)
        checked_at = str(checked_at).strip()
        minimum_days = int(minimum_days)
        if not operation_id or user_id == partner_id or not checked_at or minimum_days < 0:
            raise ValueError("invalid partner unbind operation")
        payload = json.dumps(
            [user_id, partner_id, expected_user_bind_time, expected_partner_bind_time,
             expected_user_affection, expected_partner_affection, checked_at, minimum_days],
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
                    "CREATE TABLE IF NOT EXISTS partner_unbind_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,partner_id TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,partner_id FROM partner_unbind_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PartnerUnbindResult(
                        "duplicate" if str(previous[0]) == payload else "operation_conflict", str(previous[1])
                    )
                rows = conn.execute(
                    "SELECT user_id,partner_id,bind_time,COALESCE(affection,0) FROM player_data.partner "
                    "WHERE user_id IN (%s,%s)",
                    (user_id, partner_id),
                ).fetchall()
                actual = {
                    str(row[0]): (self._text_or_none(row[1]), self._text_or_none(row[2]), int(row[3]))
                    for row in rows
                }
                expected = {
                    user_id: (partner_id, expected_user_bind_time, expected_user_affection),
                    partner_id: (user_id, expected_partner_bind_time, expected_partner_affection),
                }
                if actual != expected:
                    conn.rollback()
                    return PartnerUnbindResult("state_changed", partner_id)
                if self._within_minimum_period(expected_user_bind_time, checked_at, minimum_days):
                    conn.rollback()
                    return PartnerUnbindResult("too_early", partner_id)
                for owner_id, related_id, bind_time, affection in (
                    (user_id, partner_id, expected_user_bind_time, expected_user_affection),
                    (partner_id, user_id, expected_partner_bind_time, expected_partner_affection),
                ):
                    changed = conn.execute(
                        "UPDATE player_data.partner SET partner_id=NULL,bind_time=NULL,affection=0 "
                        "WHERE user_id=%s AND partner_id=%s AND "
                        "COALESCE(bind_time,'')=%s AND COALESCE(affection,0)=%s",
                        (owner_id, related_id, bind_time or "", affection),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return PartnerUnbindResult("state_changed", partner_id)
                conn.execute(
                    "INSERT INTO partner_unbind_operations(operation_id,payload,partner_id) VALUES(%s,%s,%s)",
                    (operation_id, payload, partner_id),
                )
                conn.commit()
                return PartnerUnbindResult("applied", partner_id)
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
    def _text_or_none(value):
        if value is None or str(value).strip().lower() in {"", "none", "null"}:
            return None
        return str(value)

    @staticmethod
    def _within_minimum_period(bind_time, checked_at, minimum_days):
        if not bind_time:
            return False
        try:
            bound = datetime.strptime(bind_time, "%Y-%m-%d %H:%M:%S")
            checked = datetime.strptime(checked_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return False
        return checked < bound + timedelta(days=minimum_days)

    @staticmethod
    def _ensure_partner_table(conn):
        ensure_player_field(conn, "partner", "partner_id", "TEXT")
        ensure_player_field(conn, "partner", "bind_time", "TEXT")
        ensure_player_field(conn, "partner", "affection", "INTEGER")


__all__ = ["PartnerUnbindResult", "PartnerUnbindService"]
