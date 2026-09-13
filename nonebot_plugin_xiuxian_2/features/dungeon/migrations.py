from ...infrastructure.database import DatabaseUnitOfWork


def apply_dungeon(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO dungeon_feature_migrations(version) VALUES ('dungeon.001')")


__all__ = ["apply_dungeon"]
