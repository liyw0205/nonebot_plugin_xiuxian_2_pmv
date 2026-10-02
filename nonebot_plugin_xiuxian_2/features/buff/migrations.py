from ...infrastructure.database import DatabaseUnitOfWork


def apply_buff(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS buff_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO buff_feature_migrations(version) VALUES ('buff.001')")


def apply_partner_token_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS partner_token_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tokens INTEGER NOT NULL,"
        "used_count INTEGER NOT NULL,item_remaining INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_partner_token_usage(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS partner_two_exp_usage("
        "user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL DEFAULT 0,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_partner_cultivation_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS partner_cultivation_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,exp_1 INTEGER NOT NULL,"
        "exp_2 INTEGER NOT NULL,used_count INTEGER NOT NULL,affection_1 INTEGER NOT NULL,"
        "affection_2 INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_partner_cultivation_player_schema(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS partner(user_id TEXT PRIMARY KEY)")
    partner_columns = {
        str(row["name"]) for row in uow.query_all("PRAGMA table_info(partner)")
    }
    for name, data_type in (
        ("partner_id", "TEXT"),
        ("bind_time", "TEXT"),
        ("affection", "INTEGER DEFAULT 0"),
    ):
        if name not in partner_columns:
            uow.execute(f'ALTER TABLE partner ADD COLUMN "{name}" {data_type}')

    uow.execute("CREATE TABLE IF NOT EXISTS status(user_id TEXT PRIMARY KEY)")
    status_columns = {
        str(row["name"]) for row in uow.query_all("PRAGMA table_info(status)")
    }
    if "two_exp_protect" not in status_columns:
        uow.execute("ALTER TABLE status ADD COLUMN two_exp_protect TEXT")

    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    statistics_columns = {
        str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")
    }
    if "双修次数" not in statistics_columns:
        uow.execute('ALTER TABLE statistics ADD COLUMN "双修次数" INTEGER DEFAULT 0')

    uow.execute(
        "CREATE TABLE IF NOT EXISTS partner_cultivation_invites("
        "invite_id TEXT PRIMARY KEY,inviter_id TEXT NOT NULL,target_id TEXT NOT NULL,"
        "count INTEGER NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL,"
        "expires_at REAL NOT NULL,resolved_at REAL)"
    )
    uow.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS partner_invite_pending_inviter "
        "ON partner_cultivation_invites(inviter_id) WHERE status='pending'"
    )
    uow.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS partner_invite_pending_target "
        "ON partner_cultivation_invites(target_id) WHERE status='pending'"
    )


def apply_normal_pvp_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS normal_pvp_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_normal_pvp_player_statistics(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
    for name in ("切磋胜利", "切磋失败"):
        if name not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{name}" INTEGER DEFAULT 0')


def apply_closing_settlement_game(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS closing_settlement_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(closing_settlement_operations)")}
    additions = {
        "exp_time": "INTEGER NOT NULL DEFAULT 0",
        "effects_event_id": "TEXT",
        "occurred_at": "TEXT",
    }
    for field, definition in additions.items():
        if field not in columns:
            uow.execute(f'ALTER TABLE closing_settlement_operations ADD COLUMN "{field}" {definition}')


def apply_closing_effects_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
    for name in ("闭关时长", "闭关修为", "闭关灵石消耗"):
        if name not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{name}" INTEGER DEFAULT 0')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS closing_statistics_events("
        "event_id TEXT NOT NULL,event_key TEXT NOT NULL,user_id TEXT NOT NULL,"
        "increment INTEGER NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(event_id,event_key))"
    )


__all__ = [
    "apply_buff",
    "apply_partner_token_operations",
    "apply_partner_token_usage",
    "apply_partner_cultivation_operations",
    "apply_partner_cultivation_player_schema",
    "apply_normal_pvp_operations",
    "apply_normal_pvp_player_statistics",
    "apply_closing_settlement_game",
    "apply_closing_effects_player",
]
