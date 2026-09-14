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


def apply_map_explore_start(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_explore_start_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stamina INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_map_explore_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_explore_status (user_id TEXT PRIMARY KEY,running INTEGER NOT NULL DEFAULT 0,node_type TEXT NOT NULL DEFAULT '',node_name TEXT NOT NULL DEFAULT '',start_time TEXT NOT NULL DEFAULT '',duration_min INTEGER NOT NULL DEFAULT 0,settlement TEXT NOT NULL DEFAULT '',max_duration_min INTEGER NOT NULL DEFAULT 0,interval_min INTEGER NOT NULL DEFAULT 0)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(map_explore_status)")}
    if "settlement" not in columns:
        uow.execute("ALTER TABLE map_explore_status ADD COLUMN settlement TEXT DEFAULT ''")
    cooldown = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(map_cooldown)")}
    if "explore_start_cd_until" not in cooldown:
        uow.execute("ALTER TABLE map_cooldown ADD COLUMN explore_start_cd_until TEXT DEFAULT NULL")


def apply_map_explore_settlement(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_explore_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,rewards TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_map_mission_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_mission_claim_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,rewards TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_map_seed_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_seed_purchase_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,stone INTEGER NOT NULL,inventory INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_map_dongfu_build(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_dongfu_build_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_map_dongfu_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER NOT NULL DEFAULT 0,realm TEXT,heaven TEXT,node_id TEXT,node_name TEXT,node_type TEXT)")
    columns={str(row['name']) for row in uow.query_all('PRAGMA table_info(dongfu_status)')}
    for name in ('realm','heaven','node_id','node_name','node_type'):
        if name not in columns:uow.execute(f'ALTER TABLE dongfu_status ADD COLUMN "{name}" TEXT')


def apply_map_combat_start(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_combat_start_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,task_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_map_combat_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_combat_settlement (user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL DEFAULT '')")
    columns={str(row['name']) for row in uow.query_all('PRAGMA table_info(map_cooldown)')}
    if 'combat_cd_until' not in columns:uow.execute('ALTER TABLE map_cooldown ADD COLUMN combat_cd_until TEXT DEFAULT NULL')


def apply_map_combat_plan(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS map_combat_plan_operations (operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,task_id TEXT NOT NULL,payload TEXT NOT NULL,snapshot TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")

__all__ = ["apply_map", "apply_map_combat_plan", "apply_map_combat_player", "apply_map_combat_start", "apply_map_dongfu_build", "apply_map_dongfu_player", "apply_map_explore_player", "apply_map_explore_settlement", "apply_map_explore_start", "apply_map_home_return", "apply_map_interactive_player", "apply_map_interactive_start", "apply_map_mission_claim", "apply_map_movement", "apply_map_resource_reward", "apply_map_seed_purchase"]
