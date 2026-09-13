from ...infrastructure.database import DatabaseUnitOfWork


def apply_trade(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS trade_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO trade_feature_migrations(version) VALUES ('trade.001')")


__all__ = ["apply_trade"]
