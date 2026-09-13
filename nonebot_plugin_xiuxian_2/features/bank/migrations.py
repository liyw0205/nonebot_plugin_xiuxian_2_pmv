from ...infrastructure.database import DatabaseUnitOfWork


def apply_bank(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS bank_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO bank_feature_migrations(version) VALUES ('bank.001')")


__all__ = ["apply_bank"]
