from ...infrastructure.database import DatabaseUnitOfWork


def apply_lunhui(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS lunhui_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO lunhui_feature_migrations(version) VALUES ('legacy.lunhui.001')")


__all__ = ["apply_lunhui"]
