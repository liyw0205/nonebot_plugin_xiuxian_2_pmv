from ...infrastructure.database import DatabaseUnitOfWork


def apply_activity(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS activity_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO activity_feature_migrations(version) VALUES ('legacy.activity.001')")


__all__ = ["apply_activity"]
