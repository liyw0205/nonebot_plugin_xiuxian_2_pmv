from ...infrastructure.database import DatabaseUnitOfWork


def apply_work(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS work_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO work_feature_migrations(version) VALUES ('work.001')")


__all__ = ["apply_work"]
