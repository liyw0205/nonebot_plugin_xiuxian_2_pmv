from ...infrastructure.database import DatabaseUnitOfWork


def apply_status(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS status_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO status_feature_migrations(version) VALUES ('legacy.status.001')")


__all__ = ["apply_status"]
