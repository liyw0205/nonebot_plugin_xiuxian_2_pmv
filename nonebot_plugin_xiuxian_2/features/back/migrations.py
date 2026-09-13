from ...infrastructure.database import DatabaseUnitOfWork


def apply_back(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS back_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO back_feature_migrations(version) VALUES ('back.001')")


__all__ = ["apply_back"]
