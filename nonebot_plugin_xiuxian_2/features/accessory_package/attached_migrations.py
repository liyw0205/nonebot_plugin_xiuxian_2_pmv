from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


ATTACHED_SCHEMA_VERSION = "accessory_package.player_data.001"
ATTACHED_SCHEMA_NAME = "player_accessory"


def _checksum() -> str:
    return hashlib.sha256(f"{ATTACHED_SCHEMA_VERSION}:{ATTACHED_SCHEMA_NAME}:v1".encode()).hexdigest()


def ensure_attached_ledger(uow: AttachedDatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_data.attached_schema_migrations "
        "(version TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )


def apply_attached_player_accessory(uow: AttachedDatabaseUnitOfWork) -> bool:
    """Apply the attached schema exactly once and record durable history."""
    ensure_attached_ledger(uow)
    checksum = _checksum()
    row = uow.query_one(
        "SELECT name, checksum FROM player_data.attached_schema_migrations WHERE version = ?",
        (ATTACHED_SCHEMA_VERSION,),
    )
    if row is not None:
        if row["name"] != ATTACHED_SCHEMA_NAME or row["checksum"] != checksum:
            raise ValueError(f"attached migration checksum changed: {ATTACHED_SCHEMA_VERSION}")
        return False
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_data.player_accessory "
        "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()}
    if "equipped" not in columns:
        uow.execute("ALTER TABLE player_data.player_accessory ADD COLUMN equipped TEXT")
    if "bag" not in columns:
        uow.execute("ALTER TABLE player_data.player_accessory ADD COLUMN bag TEXT")
    uow.execute(
        "INSERT INTO player_data.attached_schema_migrations(version,name,checksum,applied_at) VALUES (?, ?, ?, ?)",
        (ATTACHED_SCHEMA_VERSION, ATTACHED_SCHEMA_NAME, checksum, datetime.now(timezone.utc).isoformat()),
    )
    return True


__all__ = ["ATTACHED_SCHEMA_VERSION", "apply_attached_player_accessory", "ensure_attached_ledger"]
