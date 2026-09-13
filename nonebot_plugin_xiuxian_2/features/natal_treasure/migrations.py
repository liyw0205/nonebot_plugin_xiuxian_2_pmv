from ...infrastructure.database import DatabaseUnitOfWork


def apply_natal_treasure(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS natal_treasure_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO natal_treasure_feature_migrations(version) VALUES ('natal_treasure.001')")


__all__ = ["apply_natal_treasure"]
