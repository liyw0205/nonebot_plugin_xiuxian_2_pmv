from ...infrastructure.database import DatabaseUnitOfWork


def apply_back(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS back_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO back_feature_migrations(version) VALUES ('back.001')")


def apply_alchemy(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS alchemy_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,"
        "reward_stone INTEGER NOT NULL,consumed TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_unbind(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS unbind_item_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,charm_item_id INTEGER NOT NULL,"
        "target_item_id INTEGER NOT NULL,quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_cultivation_item(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS cultivation_item_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,exp_gain INTEGER NOT NULL,hp_gain INTEGER NOT NULL,"
        "mp_gain INTEGER NOT NULL,atk_gain INTEGER NOT NULL,power_multiplier REAL NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_alchemy", "apply_back", "apply_cultivation_item", "apply_unbind"]
