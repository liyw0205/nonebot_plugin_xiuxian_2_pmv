from ...infrastructure.database import DatabaseUnitOfWork


def apply_fusion(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS fusion_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO fusion_feature_migrations(version) VALUES ('legacy.fusion.001')")


def apply_fusion_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS fusion_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, successful INTEGER NOT NULL, "
        "protected INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS fusion_batch_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, outcomes TEXT NOT NULL, "
        "successful_count INTEGER NOT NULL, failed_count INTEGER NOT NULL, "
        "protected_count INTEGER NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_fusion", "apply_fusion_operations"]
