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


def apply_skill_learning(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS skill_learning_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,skill_item_id INTEGER NOT NULL,"
        "skill_type TEXT NOT NULL,previous_item_id INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_lottery_talisman(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS lottery_talisman_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,talisman_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,rewards_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_stone_reward(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS stone_item_reward_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,reward_type TEXT NOT NULL,"
        "item_id INTEGER NOT NULL,quantity INTEGER NOT NULL,rewards_json TEXT NOT NULL,"
        "total_stone INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_three_cultivation_pill(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS three_cultivation_pill_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,requested_exp INTEGER NOT NULL,exp_gain INTEGER NOT NULL,"
        "hp_before INTEGER NOT NULL,hp_after INTEGER NOT NULL,mp_before INTEGER NOT NULL,"
        "mp_after INTEGER NOT NULL,power_multiplier REAL NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_breakthrough_rate_item(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS breakthrough_rate_item_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,rate_gain INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_recovery_item(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS recovery_item_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,mode TEXT NOT NULL,hp_before INTEGER NOT NULL,"
        "hp_after INTEGER NOT NULL,mp_before INTEGER NOT NULL,mp_after INTEGER NOT NULL,"
        "stamina_before INTEGER NOT NULL,stamina_after INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_alchemy", "apply_back", "apply_breakthrough_rate_item", "apply_cultivation_item", "apply_lottery_talisman", "apply_recovery_item", "apply_skill_learning", "apply_stone_reward", "apply_three_cultivation_pill", "apply_unbind"]
