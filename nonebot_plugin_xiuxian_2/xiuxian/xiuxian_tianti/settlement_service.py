from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .tianti_data import TiantiDataManager
from .tianti_service import settle_tianti_gain


@dataclass(frozen=True)
class TiantiSettlementResult:
    status: str
    user_id: str
    detail: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class TiantiSettlementService:
    """Settle elapsed tianti gain atomically and reuse the first event result."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tianti_settlement_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_level INTEGER NOT NULL, "
            "result_status TEXT NOT NULL, detail_json TEXT NOT NULL, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(tianti_info)").fetchall()}
        for field in fields:
            if field not in columns:
                conn.execute(f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT")

    def get_result(self, operation_id: str) -> TiantiSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            self._ensure_schema(conn, tuple(self._manager._default().keys()))
            previous = conn.execute(
                "SELECT user_id, detail_json FROM tianti_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return TiantiSettlementResult("duplicate", str(previous[0]), json.loads(previous[1]))

    def settle(self, operation_id, user_id, now_t: datetime,
               *, sect_fairyland_level=0) -> TiantiSettlementResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        sect_level = int(sect_fairyland_level)
        if not operation_id:
            raise ValueError("operation_id is required")

        fields = tuple(self._manager._default().keys())
        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields)
                previous = conn.execute(
                    "SELECT user_id, sect_level, detail_json FROM tianti_settlement_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != user_id or int(previous[1]) != sect_level:
                        return TiantiSettlementResult("state_changed", user_id, {})
                    return TiantiSettlementResult("duplicate", user_id, json.loads(previous[2]))

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,),
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                detail = settle_tianti_gain(data, now_t, sect_level)
                values = [
                    json.dumps(data[field], ensure_ascii=False)
                    if isinstance(data[field], (list, dict)) else data[field]
                    for field in fields
                ]
                columns = ", ".join(["user_id", *(db_backend.quote_ident(field) for field in fields)])
                placeholders = ", ".join(["%s"] * (len(fields) + 1))
                updates = ", ".join(
                    f"{db_backend.quote_ident(field)}=EXCLUDED.{db_backend.quote_ident(field)}"
                    for field in fields
                )
                conn.execute(
                    f"INSERT INTO tianti_info ({columns}) VALUES ({placeholders}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}", (user_id, *values),
                )
                conn.execute(
                    "INSERT INTO tianti_settlement_operations "
                    "(operation_id, user_id, sect_level, result_status, detail_json) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_level, str(detail["status"]),
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return TiantiSettlementResult("settled", user_id, detail)
            except Exception:
                conn.rollback()
                raise


__all__ = ["TiantiSettlementResult", "TiantiSettlementService"]
