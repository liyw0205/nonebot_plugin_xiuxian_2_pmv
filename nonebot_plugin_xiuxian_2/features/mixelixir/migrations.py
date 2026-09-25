from ...infrastructure.database import DatabaseUnitOfWork


def apply_mixelixir(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS mixelixir_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO mixelixir_feature_migrations(version) VALUES ('mixelixir.001')")


def apply_mixelixir_refine_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS mixelixir_refine_tasks ("
        "task_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,recipe_set_id TEXT NOT NULL,"
        "recipe_key TEXT NOT NULL DEFAULT '',status TEXT NOT NULL,materials_json TEXT NOT NULL,"
        "reward_id INTEGER NOT NULL DEFAULT 0,reward_name TEXT NOT NULL DEFAULT '',"
        "reward_quantity INTEGER NOT NULL,expected_mix_state TEXT NOT NULL DEFAULT '{}',"
        "updated_mix_state TEXT NOT NULL DEFAULT '{}',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "claimed_at TIMESTAMP)"
    )
    columns = {
        str(row["name"])
        for row in uow.query_all("PRAGMA table_info(mixelixir_refine_tasks)")
    }
    for name, definition in (
        ("recipe_key", "TEXT NOT NULL DEFAULT ''"),
        ("reward_id", "INTEGER NOT NULL DEFAULT 0"),
        ("reward_name", "TEXT NOT NULL DEFAULT ''"),
        ("expected_mix_state", "TEXT NOT NULL DEFAULT '{}'"),
        ("updated_mix_state", "TEXT NOT NULL DEFAULT '{}'"),
    ):
        if name not in columns:
            uow.execute(f'ALTER TABLE mixelixir_refine_tasks ADD COLUMN "{name}" {definition}')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS mixelixir_refine_cost_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS mixelixir_refine_reward_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_id TEXT NOT NULL,"
        "reward_id INTEGER NOT NULL,reward_name TEXT NOT NULL,reward_quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_mixelixir_refine_claim_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics (user_id TEXT PRIMARY KEY)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
    if "炼丹次数" not in columns:
        uow.execute('ALTER TABLE statistics ADD COLUMN "炼丹次数" INTEGER DEFAULT NULL')


__all__ = [
    "apply_mixelixir",
    "apply_mixelixir_refine_claim",
    "apply_mixelixir_refine_claim_player",
]
