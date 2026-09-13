from ...infrastructure.database import DatabaseUnitOfWork


def apply_tianti_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_settlement_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tianti_settlement_feature_migrations(version) VALUES ('tianti_settlement.001')")


__all__ = ["apply_tianti_settlement"]
