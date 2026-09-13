from ...infrastructure.database import DatabaseUnitOfWork


def apply_admin_asset(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS admin_asset_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO admin_asset_feature_migrations(version) VALUES ('admin_asset.001')")


__all__ = ["apply_admin_asset"]
