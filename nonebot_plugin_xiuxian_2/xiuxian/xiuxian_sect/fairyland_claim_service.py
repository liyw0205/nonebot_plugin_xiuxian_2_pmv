from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from ..xiuxian_tianti.tianti_data import TiantiDataManager
from ..xiuxian_tianti.tianti_service import grant_tianti_settle_minutes
from .sect_fairyland import SECT_FAIRYLAND_CLAIM_TABLE, _fairyland_claim_key


@dataclass(frozen=True)
class FairylandClaimResult:
    status: str
    user_id: str
    sect_id: str
    day: str
    level: int
    minutes: int
    detail: dict

    @property
    def succeeded(self) -> bool:
        return self.status in {"claimed", "duplicate"}


class FairylandClaimService:
    """Grant daily sect fairyland tianti gain and mark the claim atomically."""

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(player_database)
        self._lock = lock or RLock()
        self._manager = TiantiDataManager()

    @staticmethod
    def _ensure_schema(conn, fields, claim_field) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sect_fairyland_claim_operations ("
            "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_id TEXT NOT NULL, "
            "claim_day TEXT NOT NULL, level INTEGER NOT NULL, minutes INTEGER NOT NULL, "
            "detail_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
        tianti_columns = set(conn.column_names("tianti_info"))
        for field in fields:
            if field not in tianti_columns:
                conn.execute(
                    f"ALTER TABLE tianti_info ADD COLUMN {db_backend.quote_ident(field)} TEXT"
                )
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} "
            "(user_id TEXT PRIMARY KEY)"
        )
        claim_columns = set(conn.column_names(SECT_FAIRYLAND_CLAIM_TABLE))
        if claim_field not in claim_columns:
            conn.execute(
                f"ALTER TABLE {db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} "
                f"ADD COLUMN {db_backend.quote_ident(claim_field)} TEXT"
            )

    def claim(self, operation_id, user_id, sect_id, day, level, minutes):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        sect_id = str(sect_id)
        day = str(day).strip()
        level = int(level)
        minutes = int(minutes)
        if not operation_id or not day or level <= 0 or minutes <= 0:
            raise ValueError("operation_id, day, level and minutes must be valid")

        def result(status, detail=None, result_level=level, result_minutes=minutes):
            return FairylandClaimResult(
                status, user_id, sect_id, day, int(result_level), int(result_minutes),
                detail or {},
            )

        fields = tuple(self._manager._default().keys())
        claim_field = _fairyland_claim_key(sect_id)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn, fields, claim_field)
                previous = conn.execute(
                    "SELECT user_id, sect_id, claim_day, level, minutes, detail_json "
                    "FROM sect_fairyland_claim_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if (
                        str(previous[0]) != user_id or str(previous[1]) != sect_id
                        or str(previous[2]) != day or int(previous[3]) != level
                        or int(previous[4]) != minutes
                    ):
                        return result("state_changed")
                    return result("duplicate", json.loads(previous[5]), previous[3], previous[4])

                prior_claim = conn.execute(
                    f"SELECT {db_backend.quote_ident(claim_field)} FROM "
                    f"{db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if prior_claim is not None and str(prior_claim[0] or "") == day:
                    conn.rollback()
                    return result("already_claimed")

                row = conn.execute(
                    "SELECT " + ", ".join(db_backend.quote_ident(field) for field in fields)
                    + " FROM tianti_info WHERE user_id=%s", (user_id,)
                ).fetchone()
                data = self._manager._clean_user_data(dict(zip(fields, row)) if row else {})
                detail = grant_tianti_settle_minutes(
                    data, minutes, sect_fairyland_level=level
                )
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
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates}",
                    (user_id, *values),
                )
                conn.execute(
                    f"INSERT INTO {db_backend.quote_ident(SECT_FAIRYLAND_CLAIM_TABLE)} "
                    f"(user_id, {db_backend.quote_ident(claim_field)}) VALUES (%s, %s) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {db_backend.quote_ident(claim_field)}=EXCLUDED.{db_backend.quote_ident(claim_field)}",
                    (user_id, day),
                )
                conn.execute(
                    "INSERT INTO sect_fairyland_claim_operations "
                    "(operation_id, user_id, sect_id, claim_day, level, minutes, detail_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (operation_id, user_id, sect_id, day, level, minutes,
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
                conn.commit()
                return result("claimed", detail)
            except Exception:
                conn.rollback()
                raise


__all__ = ["FairylandClaimResult", "FairylandClaimService"]
