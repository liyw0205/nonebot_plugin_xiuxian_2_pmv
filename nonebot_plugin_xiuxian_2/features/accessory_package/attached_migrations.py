from __future__ import annotations

import hashlib
from typing import Any

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


ATTACHED_SCHEMA_VERSION = "accessory_package.player_data.001"
ATTACHED_SCHEMA_NAME = "player_accessory"
ATTACHED_OPERATION_VERSION = "accessory_package.player_data.002"
ATTACHED_OPERATION_NAME = "accessory_package_operations"
ATTACHED_PRESET_VERSION = "accessory_package.player_data.003"
ATTACHED_PRESET_NAME = "accessory_presets"


def _checksum() -> str:
    return hashlib.sha256(f"{ATTACHED_SCHEMA_VERSION}:{ATTACHED_SCHEMA_NAME}:v1".encode()).hexdigest()


def ensure_attached_ledger(uow: AttachedDatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_data.attached_schema_migrations "
        "(version TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )


def apply_attached_player_accessory(uow: AttachedDatabaseUnitOfWork, *, clock: Any | None = None) -> bool:
    """Apply the attached schema exactly once and record durable history."""
    ensure_attached_ledger(uow)
    applied_at = (clock.now() if clock is not None else SystemClock().now()).isoformat()
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
        (ATTACHED_SCHEMA_VERSION, ATTACHED_SCHEMA_NAME, checksum, applied_at),
    )
    return True


def apply_attached_player_accessory_operations(uow: AttachedDatabaseUnitOfWork, *, clock: Any | None = None) -> bool:
    """Create the player-side accessory replay table without rewriting v1."""
    ensure_attached_ledger(uow)
    applied_at = (clock.now() if clock is not None else SystemClock().now()).isoformat()
    checksum = hashlib.sha256(f"{ATTACHED_OPERATION_VERSION}:{ATTACHED_OPERATION_NAME}:v1".encode()).hexdigest()
    row = uow.query_one(
        "SELECT name, checksum FROM player_data.attached_schema_migrations WHERE version = ?",
        (ATTACHED_OPERATION_VERSION,),
    )
    if row is not None:
        if row["name"] != ATTACHED_OPERATION_NAME or row["checksum"] != checksum:
            raise ValueError(f"attached migration checksum changed: {ATTACHED_OPERATION_VERSION}")
        return False
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_data.accessory_package_operations "
        "(operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, accessories_json TEXT NOT NULL, "
        "before_json TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "INSERT INTO player_data.attached_schema_migrations(version,name,checksum,applied_at) VALUES (?, ?, ?, ?)",
        (ATTACHED_OPERATION_VERSION, ATTACHED_OPERATION_NAME, checksum, applied_at),
    )
    return True


def apply_attached_player_accessory_presets(
    uow: AttachedDatabaseUnitOfWork, *, clock: Any | None = None
) -> bool:
    """Add preset columns through a durable attached migration."""
    ensure_attached_ledger(uow)
    applied_at = (clock.now() if clock is not None else SystemClock().now()).isoformat()
    checksum = hashlib.sha256(
        f"{ATTACHED_PRESET_VERSION}:{ATTACHED_PRESET_NAME}:v1".encode()
    ).hexdigest()
    row = uow.query_one(
        "SELECT name, checksum FROM player_data.attached_schema_migrations WHERE version = ?",
        (ATTACHED_PRESET_VERSION,),
    )
    if row is not None:
        if row["name"] != ATTACHED_PRESET_NAME or row["checksum"] != checksum:
            raise ValueError(f"attached migration checksum changed: {ATTACHED_PRESET_VERSION}")
        return False

    columns = {
        str(item[1])
        for item in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()
    }
    for field_name in ("preset_1", "preset_2", "preset_3"):
        if field_name not in columns:
            uow.execute(
                f"ALTER TABLE player_data.player_accessory ADD COLUMN {field_name} TEXT"
            )
    uow.execute(
        "INSERT INTO player_data.attached_schema_migrations(version,name,checksum,applied_at) VALUES (?, ?, ?, ?)",
        (ATTACHED_PRESET_VERSION, ATTACHED_PRESET_NAME, checksum, applied_at),
    )
    return True


__all__ = [
    "ATTACHED_OPERATION_VERSION",
    "ATTACHED_PRESET_VERSION",
    "ATTACHED_SCHEMA_VERSION",
    "apply_attached_player_accessory",
    "apply_attached_player_accessory_operations",
    "apply_attached_player_accessory_presets",
    "ensure_attached_ledger",
]
