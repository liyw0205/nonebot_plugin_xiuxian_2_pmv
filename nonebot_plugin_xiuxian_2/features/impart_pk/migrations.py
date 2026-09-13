from ...infrastructure.database import DatabaseUnitOfWork


def apply_impart_pk(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS impart_pk_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO impart_pk_feature_migrations(version) VALUES ('legacy.impart_pk.001')")


__all__ = ["apply_impart_pk"]
