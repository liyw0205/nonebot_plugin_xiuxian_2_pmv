from ...infrastructure.database import DatabaseUnitOfWork


def apply_arena(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO arena_feature_migrations(version) VALUES ('arena.001')")


__all__ = ["apply_arena"]
