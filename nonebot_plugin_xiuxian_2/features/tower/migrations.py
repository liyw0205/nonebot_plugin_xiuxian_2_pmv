from ...infrastructure.database import DatabaseUnitOfWork


def apply_tower(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tower_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO tower_feature_migrations(version) VALUES ('tower.001')")


def apply_tower_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tower_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,score INTEGER NOT NULL,purchased INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_tower_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tower_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_tower", "apply_tower_purchase", "apply_tower_settlement"]
