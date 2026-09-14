from ...infrastructure.database import DatabaseUnitOfWork


def apply_map(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO map_feature_migrations(version) VALUES ('map.001')")


def apply_map_movement(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_movement_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stamina INTEGER NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_map", "apply_map_movement"]
