from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PartnerProtectionResult:
    status: str
    previous_status: str = "off"
    current_status: str = "off"

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerProtectionService:
    """Own the protection state used by invite and settlement transactions."""

    VALID_STATUSES = {"on", "off", "refusal"}

    def __init__(self, player_database: str | Path, lock: RLock | None = None):
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @classmethod
    def normalize(cls, status) -> str:
        status = str(status or "off").strip().lower()
        return status if status in cls.VALID_STATUSES else "off"

    @classmethod
    def require_valid(cls, status) -> str:
        status = str(status).strip().lower()
        if status not in cls.VALID_STATUSES:
            raise ValueError("invalid partner protection status")
        return status

    @staticmethod
    def _table_ref(schema="") -> str:
        table = db_backend.quote_ident("status")
        return f"{schema}.{table}" if schema else table

    @classmethod
    def ensure_schema(cls, conn, schema="") -> None:
        table = cls._table_ref(schema)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (user_id TEXT PRIMARY KEY)")
        pragma = f"PRAGMA {schema + '.' if schema else ''}table_info(status)"
        columns = {str(row[1]) for row in conn.execute(pragma).fetchall()}
        if "two_exp_protect" not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN "
                f"{db_backend.quote_ident('two_exp_protect')} TEXT"
            )

    @classmethod
    def read_status(cls, conn, user_id, schema="") -> str:
        cls.ensure_schema(conn, schema)
        row = conn.execute(
            f"SELECT {db_backend.quote_ident('two_exp_protect')} "
            f"FROM {cls._table_ref(schema)} WHERE user_id=%s",
            (str(user_id),),
        ).fetchone()
        return cls.normalize(row[0] if row is not None else "off")

    def get_status(self, user_id) -> str:
        with self._lock, db_backend.transaction(self._player_database) as conn:
            return self.read_status(conn, user_id)

    def set_status(
        self, operation_id, user_id, expected_status, new_status
    ) -> PartnerProtectionResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        expected_status = self.require_valid(expected_status)
        new_status = self.require_valid(new_status)
        if not operation_id or not user_id:
            raise ValueError("invalid partner protection operation")
        payload = json.dumps(
            [user_id, expected_status, new_status],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self.ensure_schema(conn)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_protection_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "previous_status TEXT NOT NULL,current_status TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,previous_status,current_status "
                    "FROM partner_protection_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return PartnerProtectionResult("operation_conflict")
                    return PartnerProtectionResult(
                        "duplicate", str(previous[1]), str(previous[2])
                    )

                current_status = self.read_status(conn, user_id)
                if current_status != expected_status:
                    conn.rollback()
                    return PartnerProtectionResult(
                        "state_changed", current_status, current_status
                    )
                field = db_backend.quote_ident("two_exp_protect")
                table = self._table_ref()
                changed = conn.execute(
                    f"UPDATE {table} SET {field}=%s WHERE user_id=%s",
                    (new_status, user_id),
                )
                if changed.rowcount == 0:
                    conn.execute(
                        f"INSERT INTO {table}(user_id,{field}) VALUES(%s,%s)",
                        (user_id, new_status),
                    )
                conn.execute(
                    "INSERT INTO partner_protection_operations("
                    "operation_id,payload,previous_status,current_status"
                    ") VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, current_status, new_status),
                )
                conn.commit()
                return PartnerProtectionResult(
                    "applied", current_status, new_status
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["PartnerProtectionResult", "PartnerProtectionService"]
