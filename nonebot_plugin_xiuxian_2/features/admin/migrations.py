from ...infrastructure.database import DatabaseUnitOfWork


def apply_admin(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS admin_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO admin_feature_migrations(version) VALUES ('legacy.admin.001')")


__all__ = ["apply_admin"]
