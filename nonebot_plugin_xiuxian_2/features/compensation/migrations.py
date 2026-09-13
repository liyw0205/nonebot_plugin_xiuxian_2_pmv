from ...infrastructure.database import DatabaseUnitOfWork


def apply_compensation(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS compensation_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO compensation_feature_migrations(version) VALUES ('legacy.compensation.001')")


__all__ = ["apply_compensation"]
