from __future__ import annotations

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


ATTACHED_SCHEMA_VERSION = "accessory_package.player_data.001"


def apply_attached_player_accessory(uow: AttachedDatabaseUnitOfWork) -> None:
    """Create the legacy-compatible attached table inside an explicit UoW.

    The caller must perform backup/reconcile checks before invoking this
    function. It is intentionally not part of the single-database migration
    catalog until attached schema history has its own durable ledger.
    """
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_data.player_accessory "
        "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA player_data.table_info(player_accessory)").fetchall()}
    if "equipped" not in columns:
        uow.execute("ALTER TABLE player_data.player_accessory ADD COLUMN equipped TEXT")
    if "bag" not in columns:
        uow.execute("ALTER TABLE player_data.player_accessory ADD COLUMN bag TEXT")


__all__ = ["ATTACHED_SCHEMA_VERSION", "apply_attached_player_accessory"]
