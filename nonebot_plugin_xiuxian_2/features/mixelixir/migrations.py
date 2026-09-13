from ...infrastructure.database import DatabaseUnitOfWork


def apply_mixelixir(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO mixelixir_feature_migrations(version) VALUES ('mixelixir.001')")


__all__ = ["apply_mixelixir"]
