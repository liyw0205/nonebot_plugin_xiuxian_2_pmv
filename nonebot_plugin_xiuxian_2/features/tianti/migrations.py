from ...infrastructure.database import DatabaseUnitOfWork


def apply_tianti(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tianti_feature_migrations(version) VALUES ('legacy.tianti.001')")


__all__ = ["apply_tianti"]
