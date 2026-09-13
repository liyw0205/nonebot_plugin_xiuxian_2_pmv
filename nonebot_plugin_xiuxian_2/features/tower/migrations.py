from ...infrastructure.database import DatabaseUnitOfWork


def apply_tower(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tower_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tower_feature_migrations(version) VALUES ('tower.001')")


__all__ = ["apply_tower"]
