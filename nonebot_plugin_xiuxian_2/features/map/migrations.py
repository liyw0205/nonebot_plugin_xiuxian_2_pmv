from ...infrastructure.database import DatabaseUnitOfWork


def apply_map(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO map_feature_migrations(version) VALUES ('map.001')")


__all__ = ["apply_map"]
