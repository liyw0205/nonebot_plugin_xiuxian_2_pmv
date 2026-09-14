from ...infrastructure.database import DatabaseUnitOfWork


def apply_arena(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO arena_feature_migrations(version) VALUES ('arena.001')")


def apply_arena_challenge_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_challenge_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,amount INTEGER NOT NULL,cost INTEGER NOT NULL,stone INTEGER NOT NULL,bought INTEGER NOT NULL,extra INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_arena", "apply_arena_challenge_purchase"]
