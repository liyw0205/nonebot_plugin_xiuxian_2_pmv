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


def apply_arena_season_reward(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS arena_season_reward_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,season_key TEXT NOT NULL,"
        "user_id TEXT NOT NULL,honor INTEGER NOT NULL,honor_points INTEGER NOT NULL,"
        "total_honor_earned INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "UNIQUE(season_key,user_id))"
    )


def apply_arena_daily_reward_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS arena ("
        "user_id TEXT PRIMARY KEY,score INTEGER DEFAULT 1000,rank TEXT DEFAULT '青铜',"
        "honor_points INTEGER DEFAULT 0,total_honor_earned INTEGER DEFAULT 0,"
        "daily_challenges_used INTEGER DEFAULT 0,daily_extra_challenges INTEGER DEFAULT 0,"
        "daily_challenge_buys INTEGER DEFAULT 0,last_reset_date TEXT DEFAULT '',"
        "last_buy_date TEXT DEFAULT '')"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(arena)").fetchall()}
    for name, definition in {
        "score": "INTEGER DEFAULT 1000",
        "rank": "TEXT DEFAULT '青铜'",
        "honor_points": "INTEGER DEFAULT 0",
        "total_honor_earned": "INTEGER DEFAULT 0",
        "daily_challenges_used": "INTEGER DEFAULT 0",
        "daily_extra_challenges": "INTEGER DEFAULT 0",
        "daily_challenge_buys": "INTEGER DEFAULT 0",
        "last_reset_date": "TEXT DEFAULT ''",
        "last_buy_date": "TEXT DEFAULT ''",
    }.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE arena ADD COLUMN "{name}" {definition}')


def apply_arena_state(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS arena ("
        "user_id TEXT PRIMARY KEY,score INTEGER DEFAULT 1000,total_wins INTEGER DEFAULT 0,"
        "total_losses INTEGER DEFAULT 0,daily_challenges_used INTEGER DEFAULT 0,"
        "daily_extra_challenges INTEGER DEFAULT 0,daily_challenge_buys INTEGER DEFAULT 0,"
        "last_reset_date TEXT DEFAULT '',last_buy_date TEXT DEFAULT '',"
        "last_challenge_time TEXT DEFAULT '',win_streak INTEGER DEFAULT 0,"
        "max_win_streak INTEGER DEFAULT 0,rank TEXT DEFAULT '青铜',honor_points INTEGER DEFAULT 0,"
        "total_honor_earned INTEGER DEFAULT 0,weekly_purchases TEXT DEFAULT NULL)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(arena)").fetchall()}
    definitions = {
        "score": "INTEGER DEFAULT 1000", "total_wins": "INTEGER DEFAULT 0",
        "total_losses": "INTEGER DEFAULT 0", "daily_challenges_used": "INTEGER DEFAULT 0",
        "daily_extra_challenges": "INTEGER DEFAULT 0", "daily_challenge_buys": "INTEGER DEFAULT 0",
        "last_reset_date": "TEXT DEFAULT ''", "last_buy_date": "TEXT DEFAULT ''",
        "last_challenge_time": "TEXT DEFAULT ''", "win_streak": "INTEGER DEFAULT 0",
        "max_win_streak": "INTEGER DEFAULT 0", "rank": "TEXT DEFAULT '青铜'",
        "honor_points": "INTEGER DEFAULT 0", "total_honor_earned": "INTEGER DEFAULT 0",
        "weekly_purchases": "TEXT DEFAULT NULL",
    }
    for name, definition in definitions.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE arena ADD COLUMN "{name}" {definition}')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS arena_state_operations ("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,"
        "period_key TEXT NOT NULL,snapshot TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_arena", "apply_arena_challenge_purchase", "apply_arena_challenge_ticket", "apply_arena_daily_reward_player", "apply_arena_purchase", "apply_arena_season_reward", "apply_arena_settlement", "apply_arena_state", "apply_arena_weekly_rank_reduction"]
