from ...infrastructure.database import DatabaseUnitOfWork


def apply_beg(uow: DatabaseUnitOfWork) -> None:
    from .repository import BegRepository

    BegRepository().ensure_schema(uow)
    uow.execute("CREATE TABLE IF NOT EXISTS beg_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO beg_feature_migrations(version) VALUES ('beg.001')")
    # Keep the historical marker visible for installations upgraded during
    # the compatibility release.
    uow.execute("INSERT OR IGNORE INTO beg_feature_migrations(version) VALUES ('legacy.beg.001')")


__all__ = ["apply_beg"]
