from ...infrastructure.database import DatabaseUnitOfWork


def apply_base(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS base_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO base_feature_migrations(version) VALUES ('base.001')")


__all__ = ["apply_base"]
