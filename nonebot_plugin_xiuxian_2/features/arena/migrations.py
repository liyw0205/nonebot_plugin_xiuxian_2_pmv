from ...infrastructure.database import DatabaseUnitOfWork


def apply_arena(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO arena_feature_migrations(version) VALUES ('arena.001')")


def apply_arena_challenge_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_challenge_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,amount INTEGER NOT NULL,cost INTEGER NOT NULL,stone INTEGER NOT NULL,bought INTEGER NOT NULL,extra INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_arena_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,honor_points INTEGER NOT NULL,purchased INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_arena_challenge_ticket(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_challenge_ticket_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tickets INTEGER NOT NULL,item_remaining INTEGER NOT NULL,challenges_used INTEGER NOT NULL,challenges_remaining INTEGER NOT NULL,challenge_cap INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_arena", "apply_arena_challenge_purchase", "apply_arena_challenge_ticket", "apply_arena_purchase"]
