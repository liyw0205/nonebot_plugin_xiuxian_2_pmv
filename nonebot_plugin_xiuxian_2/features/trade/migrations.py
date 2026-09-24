from ...infrastructure.database import DatabaseUnitOfWork


def apply_trade(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS trade_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO trade_feature_migrations(version) VALUES ('trade.001')")


def apply_trade_guishi_deposit(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS trade_guishi_deposit_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,"
        "amount INTEGER NOT NULL,stored_balance INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_guishi_schema(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS guishi_info ("
        "user_id TEXT PRIMARY KEY,stored_stone INTEGER DEFAULT 0,items TEXT DEFAULT '{}')"
    )


def apply_trade_guishi_withdraw(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS trade_guishi_withdraw_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,"
        "amount INTEGER NOT NULL,fee INTEGER NOT NULL,actual_amount INTEGER NOT NULL,"
        "stored_balance INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_guishi_qiugou(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS guishi_order_create_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,order_id TEXT NOT NULL,"
        "order_type TEXT NOT NULL,amount INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_guishi_order_cancel(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS guishi_order_cancel_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,order_id TEXT NOT NULL,"
        "order_type TEXT NOT NULL,user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
        "item_name TEXT NOT NULL,goods_type TEXT NOT NULL,"
        "refunded_quantity INTEGER NOT NULL,refunded_stone INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_guishi_match(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS guishi_match_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_guishi_expired_cleanup(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS guishi_expired_order_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,order_id TEXT NOT NULL,"
        "user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,item_name TEXT NOT NULL,"
        "goods_type TEXT NOT NULL,refunded_quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_guishi_take_item(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS guishi_take_item_operations ("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
        "item_name TEXT NOT NULL,goods_type TEXT NOT NULL,quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_trade_xianshi_listing(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_listing_operations ("
        "operation_id TEXT PRIMARY KEY,seller_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
        "name TEXT NOT NULL,goods_type TEXT NOT NULL,price INTEGER NOT NULL,"
        "requested_quantity INTEGER NOT NULL,listed_quantity INTEGER NOT NULL,"
        "fee_charged INTEGER NOT NULL,stamina_cost INTEGER NOT NULL DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row[1])
        for row in uow.execute("PRAGMA table_info(xianshi_listing_operations)").fetchall()
    }
    if "stamina_cost" not in columns:
        uow.execute(
            "ALTER TABLE xianshi_listing_operations "
            "ADD COLUMN stamina_cost INTEGER NOT NULL DEFAULT 0"
        )
    uow.execute("CREATE TABLE IF NOT EXISTS trade_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO trade_feature_migrations(version) VALUES ('trade.010')")


def apply_trade_xianshi_plan_listing(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_plan_listing_operations ("
        "operation_id TEXT PRIMARY KEY,seller_id TEXT NOT NULL,listing_plan TEXT NOT NULL,"
        "listed_quantity INTEGER NOT NULL,fee_charged INTEGER NOT NULL,"
        "stamina_cost INTEGER NOT NULL DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row[1])
        for row in uow.execute(
            "PRAGMA table_info(xianshi_plan_listing_operations)"
        ).fetchall()
    }
    if "stamina_cost" not in columns:
        uow.execute(
            "ALTER TABLE xianshi_plan_listing_operations "
            "ADD COLUMN stamina_cost INTEGER NOT NULL DEFAULT 0"
        )
    uow.execute("CREATE TABLE IF NOT EXISTS trade_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO trade_feature_migrations(version) VALUES ('trade.011')")


def apply_trade_xianshi_removal(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_removal_operations ("
        "operation_id TEXT PRIMARY KEY,listing_id TEXT NOT NULL,seller_id TEXT NOT NULL,"
        "goods_id INTEGER NOT NULL,name TEXT NOT NULL,goods_type TEXT NOT NULL,"
        "refunded_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_clear_operations ("
        "operation_id TEXT PRIMARY KEY,listing_count INTEGER NOT NULL,"
        "refunded_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_name_removal_operations ("
        "operation_id TEXT PRIMARY KEY,seller_id TEXT NOT NULL,item_name TEXT NOT NULL,"
        "requested_quantity INTEGER NOT NULL,removed_quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute("CREATE TABLE IF NOT EXISTS trade_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO trade_feature_migrations(version) VALUES ('trade.012')")


def apply_trade_xianshi_purchase(uow: DatabaseUnitOfWork) -> None:
    """Prepare Xianshi purchase tables before request-time transactions."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_item ("
        "id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,name TEXT,type TEXT,"
        "price INTEGER,quantity INTEGER)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_operations ("
        "operation_id TEXT PRIMARY KEY,listing_id TEXT NOT NULL,buyer_id TEXT NOT NULL,"
        "seller_id TEXT NOT NULL,goods_id INTEGER NOT NULL,name TEXT NOT NULL,"
        "goods_type TEXT NOT NULL,quantity INTEGER NOT NULL,total_cost INTEGER NOT NULL,"
        "stamina_operation_id TEXT NOT NULL DEFAULT '',stamina_cost INTEGER NOT NULL DEFAULT 0,"
        "stamina_charged INTEGER NOT NULL DEFAULT 0,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row[1])
        for row in uow.execute("PRAGMA table_info(xianshi_operations)").fetchall()
    }
    for name, definition in (
        ("stamina_operation_id", "TEXT NOT NULL DEFAULT ''"),
        ("stamina_cost", "INTEGER NOT NULL DEFAULT 0"),
        ("stamina_charged", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if name not in columns:
            uow.execute(f"ALTER TABLE xianshi_operations ADD COLUMN {name} {definition}")
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xianshi_stamina_operations ("
        "operation_id TEXT PRIMARY KEY,buyer_id TEXT NOT NULL,stamina_cost INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute("CREATE TABLE IF NOT EXISTS trade_feature_migrations(version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO trade_feature_migrations(version) VALUES ('trade.013')")


__all__ = [
    "apply_trade",
    "apply_trade_guishi_deposit",
    "apply_trade_guishi_schema",
    "apply_trade_guishi_withdraw",
    "apply_trade_guishi_qiugou",
    "apply_trade_guishi_order_cancel",
    "apply_trade_guishi_match",
    "apply_trade_guishi_expired_cleanup",
    "apply_trade_guishi_take_item",
    "apply_trade_xianshi_listing",
    "apply_trade_xianshi_plan_listing",
    "apply_trade_xianshi_removal",
    "apply_trade_xianshi_purchase",
]
