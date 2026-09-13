from ...infrastructure.database import DatabaseUnitOfWork


def apply_combat_settlement(uow: DatabaseUnitOfWork) -> None:
    # The legacy attached transaction creates its operation table lazily; this
    # marker lets migration and recovery tooling prove the feature was loaded.
    uow.execute("CREATE TABLE IF NOT EXISTS combat_settlement_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO combat_settlement_feature_migrations(version) VALUES ('combat_settlement.001')")


__all__ = ["apply_combat_settlement"]
