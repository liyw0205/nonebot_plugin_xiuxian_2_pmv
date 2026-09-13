from ...infrastructure.database import DatabaseUnitOfWork


def apply_past_life(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS past_life_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO past_life_feature_migrations(version) VALUES ('legacy.past_life.001')")


__all__ = ["apply_past_life"]
