from ...infrastructure.database import DatabaseUnitOfWork


def apply_tianti_player_info(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_info (user_id TEXT PRIMARY KEY)")
    fields = (
        "tianti_level", "tianti_hp", "last_settle_time", "medicine_last_time",
        "medicine_end_time", "medicine_effect", "medicine_name", "opened_qiaoxue",
        "opened_qiaoxue_detail", "qiaoxue_stage_opened",
    )
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(tianti_info)")}
    for field in fields:
        if field not in columns:
            uow.execute(f'ALTER TABLE tianti_info ADD COLUMN "{field}" TEXT')


def apply_tianti_breakthrough_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_breakthrough_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cultivation_rank INTEGER NOT NULL, "
        "roll_success INTEGER NOT NULL, old_level TEXT NOT NULL, new_level TEXT NOT NULL, "
        "hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, success INTEGER NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_tianti_training(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_training_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "INSERT OR IGNORE INTO tianti_training_feature_migrations(version) VALUES ('tianti_training.001')"
    )


def apply_tianti_training_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_stone_training_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, "
        "requested_stone INTEGER NOT NULL, stone_cost INTEGER NOT NULL, "
        "hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_tianti_qiaoxue_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_qiaoxue_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, roll INTEGER NOT NULL, "
        "qiaoxue_json TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
        "opened_count INTEGER NOT NULL, unlock_limit INTEGER NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_tianti_medicine_bath_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_medicine_bath_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, "
        "result_json TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_tianti_item_reward_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS tianti_item_reward_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, item_id INTEGER NOT NULL, "
        "quantity INTEGER NOT NULL, minutes INTEGER NOT NULL, detail_json TEXT NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_tianti_breakthrough_operations", "apply_tianti_item_reward_operations", "apply_tianti_medicine_bath_operations", "apply_tianti_player_info", "apply_tianti_qiaoxue_operations", "apply_tianti_training", "apply_tianti_training_operations"]
