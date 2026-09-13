from ...infrastructure.database import DatabaseUnitOfWork


def apply_sect(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO sect_feature_migrations(version) VALUES ('sect.001')")


__all__ = ["apply_sect"]
