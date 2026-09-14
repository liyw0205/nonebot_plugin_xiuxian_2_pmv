from ...infrastructure.database import DatabaseUnitOfWork


def apply_boss(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS boss_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO boss_feature_migrations(version) VALUES ('boss.001')")


def apply_boss_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS boss_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,integral INTEGER NOT NULL,purchased INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_boss_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS world_boss_battle_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,boss_hp INTEGER NOT NULL,stamina INTEGER NOT NULL,battle_count INTEGER NOT NULL,stone INTEGER NOT NULL,exp INTEGER NOT NULL,integral INTEGER NOT NULL,activity_lines TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_boss", "apply_boss_purchase", "apply_boss_settlement"]
