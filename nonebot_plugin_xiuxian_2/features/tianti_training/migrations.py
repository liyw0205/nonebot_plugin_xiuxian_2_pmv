from ...infrastructure.database import DatabaseUnitOfWork


def apply_tianti_training(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_training_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "INSERT OR IGNORE INTO tianti_training_feature_migrations(version) VALUES ('tianti_training.001')"
    )


def apply_tianti_training_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_stone_training_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, "
        "requested_stone INTEGER NOT NULL, stone_cost INTEGER NOT NULL, "
        "hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_tianti_training", "apply_tianti_training_operations"]
