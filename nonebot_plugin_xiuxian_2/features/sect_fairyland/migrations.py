from ...infrastructure.database import DatabaseUnitOfWork


def apply_sect_fairyland(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_fairyland_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "INSERT OR IGNORE INTO sect_fairyland_feature_migrations(version) VALUES ('sect_fairyland.001')"
    )


__all__ = ["apply_sect_fairyland"]
