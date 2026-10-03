from ...infrastructure.database import DatabaseUnitOfWork


def _add_missing_columns(
    uow: DatabaseUnitOfWork, table: str, definitions: dict[str, str]
) -> None:
    columns = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
    for name, definition in definitions.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')


def apply_dungeon(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO dungeon_feature_migrations(version) VALUES ('dungeon.001')")


def apply_dungeon_purchase(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL DEFAULT 'applied',quantity INTEGER NOT NULL DEFAULT 0,cost INTEGER NOT NULL DEFAULT 0,stone INTEGER NOT NULL DEFAULT 0,inventory INTEGER NOT NULL DEFAULT 0,response TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_dungeon_session(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_session_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,dungeon_status TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_dungeon_explore(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_explore_operations(operation_id TEXT PRIMARY KEY,request_identity TEXT NOT NULL,phase TEXT NOT NULL,prepared_json TEXT NOT NULL DEFAULT '{}',result_status TEXT NOT NULL DEFAULT '',result_json TEXT NOT NULL DEFAULT '{}',current_layer INTEGER NOT NULL DEFAULT 0,dungeon_status TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)")


def apply_dungeon_team(uow: DatabaseUnitOfWork) -> None:
    apply_dungeon_team_schema(uow)
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_team_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,team_id TEXT NOT NULL,result_json TEXT NOT NULL DEFAULT '',action TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    uow.execute("CREATE TABLE IF NOT EXISTS dungeon_team_invites(invite_id TEXT PRIMARY KEY,team_id TEXT NOT NULL,inviter_id TEXT NOT NULL,invitee_id TEXT NOT NULL,group_id TEXT NOT NULL,expires_at REAL NOT NULL,consumed_at TIMESTAMP DEFAULT NULL,status TEXT NOT NULL DEFAULT 'pending',created_at REAL NOT NULL DEFAULT 0,resolved_operation_id TEXT DEFAULT NULL)")
    uow.execute("CREATE INDEX IF NOT EXISTS idx_dungeon_team_invites_pending ON dungeon_team_invites(invitee_id,status,expires_at)")
    uow.execute("CREATE TABLE IF NOT EXISTS team_cd(user_id TEXT PRIMARY KEY,join_cd_until TEXT DEFAULT '',had_first_join INTEGER DEFAULT 0)")


def apply_dungeon_team_schema(uow: DatabaseUnitOfWork) -> None:
    """Prepare the player-owned team projection and all durable receipts."""
    uow.execute("CREATE TABLE IF NOT EXISTS teams(user_id TEXT PRIMARY KEY)")
    _add_missing_columns(
        uow,
        "teams",
        {
            "team_id": "TEXT DEFAULT NULL",
            "team_name": "TEXT DEFAULT NULL",
            "group_id": "TEXT DEFAULT NULL",
            "leader": "TEXT DEFAULT NULL",
            "members": "TEXT DEFAULT NULL",
            "create_time": "TEXT DEFAULT NULL",
            "max_members": "INTEGER DEFAULT 4",
            "description": "TEXT DEFAULT NULL",
            "version": "INTEGER NOT NULL DEFAULT 0",
        },
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dungeon_team_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL DEFAULT '',"
        "result_status TEXT NOT NULL DEFAULT '',team_id TEXT NOT NULL DEFAULT '',"
        "result_json TEXT NOT NULL DEFAULT '{}',action TEXT NOT NULL DEFAULT '',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    _add_missing_columns(
        uow,
        "dungeon_team_operations",
        {
            "payload": "TEXT NOT NULL DEFAULT ''",
            "result_status": "TEXT NOT NULL DEFAULT ''",
            "team_id": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "action": "TEXT NOT NULL DEFAULT ''",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dungeon_team_invites("
        "invite_id TEXT PRIMARY KEY,team_id TEXT NOT NULL DEFAULT '',"
        "inviter_id TEXT NOT NULL DEFAULT '',invitee_id TEXT NOT NULL DEFAULT '',"
        "group_id TEXT NOT NULL DEFAULT '',expires_at REAL NOT NULL DEFAULT 0,"
        "consumed_at TIMESTAMP DEFAULT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "created_at REAL NOT NULL DEFAULT 0,resolved_operation_id TEXT DEFAULT NULL)"
    )
    _add_missing_columns(
        uow,
        "dungeon_team_invites",
        {
            "team_id": "TEXT NOT NULL DEFAULT ''",
            "inviter_id": "TEXT NOT NULL DEFAULT ''",
            "invitee_id": "TEXT NOT NULL DEFAULT ''",
            "group_id": "TEXT NOT NULL DEFAULT ''",
            "expires_at": "REAL NOT NULL DEFAULT 0",
            "consumed_at": "TIMESTAMP DEFAULT NULL",
            "status": "TEXT NOT NULL DEFAULT 'pending'",
            "created_at": "REAL NOT NULL DEFAULT 0",
            "resolved_operation_id": "TEXT DEFAULT NULL",
        },
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS idx_dungeon_team_invites_pending "
        "ON dungeon_team_invites(invitee_id,status,expires_at)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS team_cd("
        "user_id TEXT PRIMARY KEY,join_cd_until TEXT DEFAULT '',"
        "had_first_join INTEGER DEFAULT 0)"
    )
    _add_missing_columns(
        uow,
        "team_cd",
        {"join_cd_until": "TEXT DEFAULT ''", "had_first_join": "INTEGER DEFAULT 0"},
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dungeon_team_exit_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL DEFAULT '',"
        "result_json TEXT NOT NULL DEFAULT '{}',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    _add_missing_columns(
        uow,
        "dungeon_team_exit_operations",
        {
            "payload": "TEXT NOT NULL DEFAULT ''",
            "result_json": "TEXT NOT NULL DEFAULT '{}'",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
    )


__all__ = [
    "apply_dungeon",
    "apply_dungeon_explore",
    "apply_dungeon_purchase",
    "apply_dungeon_session",
    "apply_dungeon_team",
    "apply_dungeon_team_schema",
]
