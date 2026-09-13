from ...infrastructure.database import DatabaseUnitOfWork


def apply_entertainment(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS entertainment_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO entertainment_feature_migrations(version) VALUES ('legacy.entertainment.001')")


__all__ = ["apply_entertainment"]
