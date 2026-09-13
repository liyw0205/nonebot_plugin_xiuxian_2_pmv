from ...infrastructure.database import DatabaseUnitOfWork


def apply_info(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS info_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO info_feature_migrations(version) VALUES ('legacy.info.001')")


__all__ = ["apply_info"]
