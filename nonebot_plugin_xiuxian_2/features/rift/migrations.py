from ...infrastructure.database import DatabaseUnitOfWork


def apply_rift(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS rift_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO rift_feature_migrations(version) VALUES ('rift.001')")


__all__ = ["apply_rift"]
