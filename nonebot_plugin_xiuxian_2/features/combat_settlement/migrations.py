from ...infrastructure.database import DatabaseUnitOfWork


def apply_combat_settlement(uow: DatabaseUnitOfWork) -> None:
    # The legacy attached transaction creates its operation table lazily; this
    # marker lets migration and recovery tooling prove the feature was loaded.
    uow.execute("CREATE TABLE IF NOT EXISTS combat_settlement_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO combat_settlement_feature_migrations(version) VALUES ('combat_settlement.001')")


def apply_combat_settlement_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_combat_settlement_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
        "rewards TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_dao_battle_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_dao_battle_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_combat_settlement", "apply_combat_settlement_operations", "apply_dao_battle_operations"]
