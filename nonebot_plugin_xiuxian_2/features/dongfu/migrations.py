from ...infrastructure.database import DatabaseUnitOfWork


def apply_dongfu(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dongfu_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO dongfu_feature_migrations(version) VALUES ('legacy.dongfu.001')")


__all__ = ["apply_dongfu"]
