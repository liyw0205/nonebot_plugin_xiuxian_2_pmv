from ...infrastructure.database import DatabaseUnitOfWork


def apply_sect(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO sect_feature_migrations(version) VALUES ('sect.001')")


def apply_sect_rename(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_rename_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_name TEXT NOT NULL,new_name TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_sect", "apply_sect_rename"]
