from ...infrastructure.database import DatabaseUnitOfWork


def apply_puppet(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS puppet_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO puppet_feature_migrations(version) VALUES ('puppet.001')")


__all__ = ["apply_puppet"]
