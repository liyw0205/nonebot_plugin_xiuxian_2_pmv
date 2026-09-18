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


def apply_arena_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS arena_challenge_settlement_operations(operation_id TEXT PRIMARY KEY,challenger_id TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_arena_weekly_rank_reduction(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_operations("
        "business_week TEXT PRIMARY KEY,reduce_steps INTEGER NOT NULL,total INTEGER NOT NULL,"
        "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
        "skipped INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
        "status TEXT NOT NULL DEFAULT 'running',last_error TEXT NOT NULL DEFAULT '',"
        "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_targets("
        "business_week TEXT NOT NULL,user_id TEXT NOT NULL,ordinal INTEGER NOT NULL,"
        "previous_score INTEGER NOT NULL,previous_rank TEXT NOT NULL,previous_win_streak INTEGER NOT NULL,"
        "target_score INTEGER NOT NULL,target_rank TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "error_text TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,"
        "PRIMARY KEY(business_week,user_id))"
    )


__all__ = ["apply_arena", "apply_arena_challenge_purchase", "apply_arena_challenge_ticket", "apply_arena_purchase", "apply_arena_settlement", "apply_arena_weekly_rank_reduction"]
