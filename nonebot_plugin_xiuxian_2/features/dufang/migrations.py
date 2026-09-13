from ...infrastructure.database import DatabaseUnitOfWork


def apply_dufang(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dufang_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO dufang_feature_migrations(version) VALUES ('legacy.dufang.001')")


__all__ = ["apply_dufang"]
