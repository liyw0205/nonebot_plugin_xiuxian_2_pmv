from ...infrastructure.database import DatabaseUnitOfWork


def apply_tianti_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_settlement_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tianti_settlement_feature_migrations(version) VALUES ('tianti_settlement.001')")


def apply_tianti_settlement_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_settlement_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, sect_level INTEGER NOT NULL, "
        "result_status TEXT NOT NULL, detail_json TEXT NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_tianti_settlement", "apply_tianti_settlement_operations"]
