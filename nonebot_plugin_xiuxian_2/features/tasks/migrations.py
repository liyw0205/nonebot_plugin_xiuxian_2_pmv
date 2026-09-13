from ...infrastructure.database import DatabaseUnitOfWork


def apply_tasks(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tasks_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tasks_feature_migrations(version) VALUES ('legacy.tasks.001')")


__all__ = ["apply_tasks"]
