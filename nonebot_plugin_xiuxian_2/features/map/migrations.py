from ...infrastructure.database import DatabaseUnitOfWork


def apply_map(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO map_feature_migrations(version) VALUES ('map.001')")


def apply_map_movement(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_movement_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stamina INTEGER NOT NULL, "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_map_home_return(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_home_return_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, result_status TEXT NOT NULL, "
        "realm TEXT NOT NULL DEFAULT '', heaven TEXT NOT NULL DEFAULT '', "
        "node_id TEXT NOT NULL DEFAULT '', node_name TEXT NOT NULL DEFAULT '', "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_map_interactive_start(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_interactive_start_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, result_status TEXT NOT NULL, "
        "stamina INTEGER NOT NULL DEFAULT 0, action_json TEXT NOT NULL DEFAULT '{}', "
        "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_map_interactive_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_interactive_actions (user_id TEXT PRIMARY KEY, action_id TEXT NOT NULL UNIQUE, action_type TEXT NOT NULL, status TEXT NOT NULL, state_json TEXT NOT NULL, settlement_json TEXT NOT NULL DEFAULT '', ready_at TEXT NOT NULL, expires_at TEXT NOT NULL, cooldown_seconds INTEGER NOT NULL, updated_at TEXT NOT NULL)")
    uow.execute("CREATE TABLE IF NOT EXISTS map_interactive_terminal_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, result_status TEXT NOT NULL, action_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")
    uow.execute("CREATE TABLE IF NOT EXISTS map_cooldown (user_id TEXT PRIMARY KEY, gather_cd_until TEXT DEFAULT NULL)")


def apply_map_resource_reward(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS map_resource_reward_operations ("
        "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stone INTEGER NOT NULL, "
        "rewards TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_map", "apply_map_home_return", "apply_map_interactive_player", "apply_map_interactive_start", "apply_map_movement", "apply_map_resource_reward"]
