from ...infrastructure.database import DatabaseUnitOfWork


def apply_title(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS title_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO title_feature_migrations(version) VALUES ('title.001')")


__all__ = ["apply_title"]
