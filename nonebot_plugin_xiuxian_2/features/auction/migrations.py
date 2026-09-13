from ...infrastructure.database import DatabaseUnitOfWork


def apply_auction(uow: DatabaseUnitOfWork) -> None:
    # The legacy repository owns its auction tables.  The common ledger is
    # created by the runtime database phase; this migration is an explicit
    # marker for the new application boundary.
    uow.execute("CREATE TABLE IF NOT EXISTS auction_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO auction_feature_migrations(version) VALUES ('auction.001')")


__all__ = ["apply_auction"]
