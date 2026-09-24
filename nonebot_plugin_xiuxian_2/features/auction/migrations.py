from ...infrastructure.database import DatabaseUnitOfWork


def apply_auction(uow: DatabaseUnitOfWork) -> None:
    # The legacy repository owns its auction tables.  The common ledger is
    # created by the runtime database phase; this migration is an explicit
    # marker for the new application boundary.
    uow.execute("CREATE TABLE IF NOT EXISTS auction_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO auction_feature_migrations(version) VALUES ('auction.001')")


def apply_auction_settlement(uow: DatabaseUnitOfWork) -> None:
    """Own the session tables used by the feature settlement repository."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS auction_sessions ("
        "session_id TEXT PRIMARY KEY,status TEXT NOT NULL,start_time REAL NOT NULL,"
        "end_time REAL NOT NULL,items_count INTEGER NOT NULL,start_operation_id TEXT NOT NULL UNIQUE,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,finish_operation_id TEXT,settled_at REAL)"
    )
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(auction_sessions)").fetchall()}
    if "finish_operation_id" not in columns:
        uow.execute("ALTER TABLE auction_sessions ADD COLUMN finish_operation_id TEXT")
    if "settled_at" not in columns:
        uow.execute("ALTER TABLE auction_sessions ADD COLUMN settled_at REAL")
    uow.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS auction_one_active_session "
        "ON auction_sessions(status) WHERE status='active'"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS auction_session_operations ("
        "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
        "result TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS auction_current ("
        "id TEXT PRIMARY KEY,item_id INTEGER NOT NULL,name TEXT NOT NULL,start_price INTEGER NOT NULL,"
        "current_price INTEGER NOT NULL,seller_id TEXT NOT NULL,seller_name TEXT NOT NULL,"
        "bids TEXT DEFAULT '{}',bid_times TEXT DEFAULT '{}',is_system INTEGER DEFAULT 0,last_bid_time REAL DEFAULT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS auction_history ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,auction_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "item_name TEXT NOT NULL,start_price INTEGER NOT NULL,final_price INTEGER,seller_id TEXT NOT NULL,"
        "seller_name TEXT NOT NULL,winner_id TEXT,winner_name TEXT,status TEXT NOT NULL,fee INTEGER,"
        "seller_earnings INTEGER,start_time REAL NOT NULL,end_time REAL NOT NULL)"
    )
    uow.execute("CREATE TABLE IF NOT EXISTS auction_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO auction_feature_migrations(version) VALUES ('auction.002')")


def apply_auction_queue_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS auction_queue_operations ("
        "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,user_id TEXT NOT NULL,"
        "item_id INTEGER NOT NULL,item_name TEXT NOT NULL,start_price INTEGER NOT NULL,"
        "user_name TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_auction_player_queue(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS auction_player_upload ("
        "user_id TEXT NOT NULL,item_id INTEGER NOT NULL,item_name TEXT NOT NULL,"
        "start_price INTEGER NOT NULL,user_name TEXT NOT NULL,PRIMARY KEY(user_id,item_id))"
    )


__all__ = [
    "apply_auction",
    "apply_auction_settlement",
    "apply_auction_queue_operations",
    "apply_auction_player_queue",
]
