from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ImpartClosingEnterResult:
    status: str
    started_at: str = ""
    entry_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _increment_entry_stat(conn, user_id: str) -> int:
    conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()
    }
    key = "虚神界闭关次数"
    if key not in columns:
        conn.execute(
            f"ALTER TABLE player_data.statistics ADD COLUMN {db_backend.quote_ident(key)} "
            "INTEGER DEFAULT 0"
        )
    field = db_backend.quote_ident(key)
    changed = conn.execute(
        f"UPDATE player_data.statistics SET {field}=COALESCE({field},0)+1 WHERE user_id=%s",
        (user_id,),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.statistics(user_id,{field}) VALUES(%s,1)",
            (user_id,),
        )
    return int(
        conn.execute(
            f"SELECT {field} FROM player_data.statistics WHERE user_id=%s", (user_id,)
        ).fetchone()[0]
    )


class ImpartClosingEnterService:
    """Atomically enter virtual-world closing from the authoritative idle state."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartClosingEnterResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_closing_enter_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,result_json FROM impart_closing_enter_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartClosingEnterResult("duplicate", str(saved[0]), int(saved[1]))

    def enter(self, operation_id, user_id, started_at) -> ImpartClosingEnterResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        started_at = str(started_at).strip()
        if not operation_id or not user_id or not started_at:
            raise ValueError("operation, user and start time are required")
        # Request identity only — started_at is outcome, stored in result_json.
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_closing_enter_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_closing_enter_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old is not None:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartClosingEnterResult("operation_conflict")
                    saved = json.loads(str(old[1]))
                    return ImpartClosingEnterResult("duplicate", str(saved[0]), int(saved[1]))

                user = conn.execute(
                    "SELECT root_type FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT COALESCE(type,0) FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return ImpartClosingEnterResult("user_missing")
                if str(user[0] or "") == "伪灵根":
                    conn.rollback()
                    return ImpartClosingEnterResult("ineligible")
                if int(cd[0]) != 0:
                    conn.rollback()
                    return ImpartClosingEnterResult("busy")

                changed = conn.execute(
                    "UPDATE user_cd SET type=4,create_time=%s,scheduled_time=NULL "
                    "WHERE user_id=%s AND COALESCE(type,0)=0",
                    (started_at, user_id),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return ImpartClosingEnterResult("state_changed")

                entry_count = _increment_entry_stat(conn, user_id)
                saved = [started_at, entry_count]
                conn.execute(
                    "INSERT INTO impart_closing_enter_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, ensure_ascii=True, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartClosingEnterResult("applied", started_at, entry_count)
            except Exception:
                conn.rollback()
                raise


__all__ = ["ImpartClosingEnterResult", "ImpartClosingEnterService"]
