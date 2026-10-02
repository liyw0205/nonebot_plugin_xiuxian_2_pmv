from ...infrastructure.database import DatabaseUnitOfWork


def apply_base(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS base_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO base_feature_migrations(version) VALUES ('base.001')")


def apply_base_player_rename_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_rename_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,rename_type TEXT NOT NULL,"
        "new_name TEXT NOT NULL,previous_name TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,payload TEXT)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("player_rename_operations")')
    }
    required = {"operation_id", "user_id", "rename_type", "new_name", "previous_name"}
    if not required.issubset(columns):
        raise RuntimeError("player_rename_operations has an unsupported legacy schema")
    if "payload" not in columns:
        uow.execute("ALTER TABLE player_rename_operations ADD COLUMN payload TEXT")


def apply_base_root_reroll_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS player_root_reroll_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("player_root_reroll_operations")')
    }
    required = {"operation_id", "user_id", "payload", "result_json", "created_at"}
    if not required.issubset(columns):
        raise RuntimeError("player_root_reroll_operations has an unsupported schema")


def apply_base_direct_breakthrough_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS direct_breakthrough_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,outcome TEXT NOT NULL,"
        "from_level TEXT NOT NULL,to_level TEXT NOT NULL,exp_loss INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,payload TEXT NOT NULL DEFAULT '')"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("direct_breakthrough_operations")')
    }
    required = {
        "operation_id", "user_id", "outcome", "from_level", "to_level", "exp_loss",
        "created_at",
    }
    if not required.issubset(columns):
        raise RuntimeError("direct_breakthrough_operations has an unsupported legacy schema")
    if "payload" not in columns:
        uow.execute(
            "ALTER TABLE direct_breakthrough_operations ADD COLUMN payload TEXT NOT NULL DEFAULT ''"
        )


def apply_base_stone_contest_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS stone_contest_operations("
        "operation_id TEXT PRIMARY KEY,payer_id TEXT NOT NULL,receiver_id TEXT NOT NULL,"
        "requested_amount INTEGER NOT NULL,transferred_amount INTEGER NOT NULL,"
        "payer_balance INTEGER NOT NULL,operation_type TEXT NOT NULL DEFAULT 'transfer',"
        "thief_id TEXT NOT NULL DEFAULT '',victim_id TEXT NOT NULL DEFAULT '',"
        "outcome TEXT NOT NULL DEFAULT '',penalty_amount INTEGER NOT NULL DEFAULT 0,"
        "stamina_cost INTEGER NOT NULL DEFAULT 0,thief_stamina INTEGER NOT NULL DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    additions = {
        "operation_type": "TEXT NOT NULL DEFAULT 'transfer'",
        "thief_id": "TEXT NOT NULL DEFAULT ''",
        "victim_id": "TEXT NOT NULL DEFAULT ''",
        "outcome": "TEXT NOT NULL DEFAULT ''",
        "penalty_amount": "INTEGER NOT NULL DEFAULT 0",
        "stamina_cost": "INTEGER NOT NULL DEFAULT 0",
        "thief_stamina": "INTEGER NOT NULL DEFAULT 0",
    }
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("stone_contest_operations")')
    }
    required = {
        "operation_id", "payer_id", "receiver_id", "requested_amount",
        "transferred_amount", "payer_balance",
    }
    if not required.issubset(columns):
        raise RuntimeError("stone_contest_operations has an unsupported legacy schema")
    for name, definition in additions.items():
        if name not in columns:
            uow.execute(
                f"ALTER TABLE stone_contest_operations ADD COLUMN {name} {definition}"
            )


def apply_base_stone_robbery_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS stone_robbery_operations("
        "operation_id TEXT PRIMARY KEY,robber_id TEXT NOT NULL,victim_id TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("stone_robbery_operations")')
    }
    if not {"operation_id", "robber_id", "victim_id", "result_json"}.issubset(columns):
        raise RuntimeError("stone_robbery_operations has an unsupported legacy schema")


def apply_base_stone_robbery_player_statistics(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("statistics")')
    }
    for column in ("抢灵石成功", "抢灵石失败"):
        if column.casefold() not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{column}" INTEGER DEFAULT 0')


def apply_base_xiangyuan(uow: DatabaseUnitOfWork) -> None:
    """Create the game-owned xiangyuan projection during startup migration."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_groups ("
        "group_id TEXT PRIMARY KEY,next_gift_id INTEGER NOT NULL DEFAULT 1,"
        "legacy_imported INTEGER NOT NULL DEFAULT 0)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_gifts ("
        "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,giver_id TEXT NOT NULL,"
        "giver_name TEXT NOT NULL,stone_amount INTEGER NOT NULL,remaining_stone INTEGER NOT NULL,"
        "receiver_count INTEGER NOT NULL,received INTEGER NOT NULL DEFAULT 0,create_time TEXT NOT NULL,"
        "PRIMARY KEY(group_id,gift_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_gift_items ("
        "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,goods_id INTEGER NOT NULL,"
        "goods_name TEXT NOT NULL,goods_type TEXT NOT NULL,quantity INTEGER NOT NULL,"
        "PRIMARY KEY(group_id,gift_id,goods_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_receivers ("
        "group_id TEXT NOT NULL,gift_id INTEGER NOT NULL,user_id TEXT NOT NULL,"
        "stone INTEGER NOT NULL,items TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(group_id,gift_id,user_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_create_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gift_id INTEGER NOT NULL,"
        "send_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_claim_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_base_xiangyuan_player(uow: DatabaseUnitOfWork) -> None:
    """Create the player-owned daily xiangyuan counters during startup."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS xiangyuan_limit ("
        "user_id TEXT PRIMARY KEY,send_count INTEGER NOT NULL DEFAULT 0,"
        "receive_count INTEGER NOT NULL DEFAULT 0,last_reset_date TEXT NOT NULL DEFAULT '')"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("xiangyuan_limit")')
    }
    for name, definition in (
        ("send_count", "INTEGER NOT NULL DEFAULT 0"),
        ("receive_count", "INTEGER NOT NULL DEFAULT 0"),
        ("last_reset_date", "TEXT NOT NULL DEFAULT ''"),
    ):
        if name not in columns:
            uow.execute(f'ALTER TABLE xiangyuan_limit ADD COLUMN "{name}" {definition}')


__all__ = [
    "apply_base",
    "apply_base_player_rename_operations",
    "apply_base_root_reroll_operations",
    "apply_base_stone_contest_operations",
    "apply_base_stone_robbery_operations",
    "apply_base_stone_robbery_player_statistics",
    "apply_base_xiangyuan",
    "apply_base_xiangyuan_player",
    "apply_base_direct_breakthrough_operations",
]
