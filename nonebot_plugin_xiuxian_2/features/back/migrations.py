from ...infrastructure.database import DatabaseUnitOfWork


def apply_back(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS back_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO back_feature_migrations(version) VALUES ('back.001')")


def apply_backpack_repair(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS backpack_repair_tasks("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "catalog_json TEXT NOT NULL,max_goods_num INTEGER NOT NULL,"
        "targets_json TEXT NOT NULL,"
        "next_index INTEGER NOT NULL DEFAULT 0,total INTEGER NOT NULL,"
        "quantity_fixed INTEGER NOT NULL DEFAULT 0,"
        "bind_fixed INTEGER NOT NULL DEFAULT 0,"
        "name_fixed INTEGER NOT NULL DEFAULT 0,"
        "equipment_fixed INTEGER NOT NULL DEFAULT 0,"
        "missing_definitions INTEGER NOT NULL DEFAULT 0,"
        "details_json TEXT NOT NULL DEFAULT '[]',"
        "status TEXT NOT NULL DEFAULT 'running',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_pet_egg_use(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS batch_pet_egg_use_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,pets_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_item_use(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS back_item_use_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,"
        "item_id INTEGER NOT NULL,quantity INTEGER NOT NULL,item_remaining INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_accessory_affix_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS accessory_transaction_operations("
        "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


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


def apply_permanent_atk_item(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS permanent_atk_item_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,item_id INTEGER NOT NULL,"
        "quantity INTEGER NOT NULL,atk_gain INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_blessed_flag_replace(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS blessed_flag_replace_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_equipment(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS equipment_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
        "action TEXT NOT NULL,previous_id INTEGER NOT NULL DEFAULT 0,"
        "payload TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(equipment_operations)")}
    if "payload" not in columns:
        uow.execute("ALTER TABLE equipment_operations ADD COLUMN payload TEXT NOT NULL DEFAULT ''")


__all__ = ["apply_accessory_affix_operations", "apply_alchemy", "apply_back", "apply_backpack_repair", "apply_blessed_flag_replace", "apply_breakthrough_rate_item", "apply_cultivation_item", "apply_equipment", "apply_item_use", "apply_lottery_talisman", "apply_permanent_atk_item", "apply_pet_egg_use", "apply_recovery_item", "apply_skill_learning", "apply_stone_reward", "apply_three_cultivation_pill", "apply_unbind"]
