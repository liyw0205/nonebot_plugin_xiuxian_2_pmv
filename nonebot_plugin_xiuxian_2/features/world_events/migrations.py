from ...infrastructure.database import DatabaseUnitOfWork


def apply_world_events(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS world_events_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO world_events_feature_migrations(version) VALUES ('world_events.001')")


__all__ = ["apply_world_events"]
