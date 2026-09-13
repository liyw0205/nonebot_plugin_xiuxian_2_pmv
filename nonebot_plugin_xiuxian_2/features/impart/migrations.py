from ...infrastructure.database import DatabaseUnitOfWork


def apply_impart(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS impart_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO impart_feature_migrations(version) VALUES ('legacy.impart.001')")


__all__ = ["apply_impart"]
