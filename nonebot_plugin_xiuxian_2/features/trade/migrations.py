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


__all__ = [
    "apply_trade",
    "apply_trade_guishi_deposit",
    "apply_trade_guishi_schema",
    "apply_trade_guishi_withdraw",
]
