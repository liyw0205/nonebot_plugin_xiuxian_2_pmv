from ...infrastructure.database import DatabaseUnitOfWork


def apply_dufang(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS dufang_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO dufang_feature_migrations(version) VALUES ('legacy.dufang.001')")


def apply_dufang_share(uow: DatabaseUnitOfWork) -> None:
    """Prepare frozen sharing batches and their durable per-recipient progress."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_share_operations("
        "operation_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,event_type TEXT NOT NULL,"
        "event_title TEXT NOT NULL,event_description TEXT NOT NULL,effect_amount INTEGER NOT NULL,"
        "bonus_percent INTEGER NOT NULL,total INTEGER NOT NULL,completed INTEGER NOT NULL DEFAULT 0,"
        "total_amount INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'running',"
        "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_share_progress("
        "operation_id TEXT NOT NULL,ordinal INTEGER NOT NULL,target_id TEXT NOT NULL,"
        "target_name TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "actual_amount INTEGER NOT NULL DEFAULT 0,wallet_stone INTEGER NOT NULL DEFAULT 0,"
        "reason TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,"
        "PRIMARY KEY(operation_id,target_id),UNIQUE(operation_id,ordinal))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS dufang_share_progress_pending_idx "
        "ON dufang_share_progress(operation_id,status,ordinal)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS economy_log("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
        "source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,"
        "exp_delta INTEGER NOT NULL DEFAULT 0,sect_contribution_delta INTEGER NOT NULL DEFAULT 0,"
        "sect_scale_delta INTEGER NOT NULL DEFAULT 0,sect_materials_delta INTEGER NOT NULL DEFAULT 0,"
        "item_delta TEXT NOT NULL DEFAULT '[]',detail TEXT NOT NULL DEFAULT '{}',"
        "trace_id TEXT,created_at TEXT NOT NULL)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("economy_log")')
    }
    definitions = {
        "user_id": "TEXT",
        "sect_id": "INTEGER",
        "source": "TEXT NOT NULL DEFAULT ''",
        "action": "TEXT NOT NULL DEFAULT ''",
        "stone_delta": "INTEGER NOT NULL DEFAULT 0",
        "exp_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_contribution_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_scale_delta": "INTEGER NOT NULL DEFAULT 0",
        "sect_materials_delta": "INTEGER NOT NULL DEFAULT 0",
        "item_delta": "TEXT NOT NULL DEFAULT '[]'",
        "detail": "TEXT NOT NULL DEFAULT '{}'",
        "trace_id": "TEXT",
        "created_at": "TEXT NOT NULL DEFAULT ''",
    }
    for name, definition in definitions.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE economy_log ADD COLUMN "{name}" {definition}')


def apply_dufang_share_player(uow: DatabaseUnitOfWork) -> None:
    """Create or extend the player-owned unseal statistics projection."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS unseal_data("
        "user_id TEXT PRIMARY KEY,count INTEGER,total_cost INTEGER,profit INTEGER,loss INTEGER,"
        "shared_profit INTEGER,shared_loss INTEGER,received_profit INTEGER,received_loss INTEGER,"
        "last_update TEXT)"
    )
    columns = {
        str(row["name"]).casefold(): int(row["pk"])
        for row in uow.query_all('PRAGMA table_info("unseal_data")')
    }
    if columns.get("user_id", 0) != 1:
        raise RuntimeError("unseal_data has an unsupported key schema")
    definitions = {
        "count": "INTEGER",
        "total_cost": "INTEGER",
        "profit": "INTEGER",
        "loss": "INTEGER",
        "shared_profit": "INTEGER",
        "shared_loss": "INTEGER",
        "received_profit": "INTEGER",
        "received_loss": "INTEGER",
        "last_update": "TEXT",
    }
    for name, definition in definitions.items():
        if name not in columns:
            uow.execute(f'ALTER TABLE unseal_data ADD COLUMN "{name}" {definition}')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_share_player_receipts("
        "operation_id TEXT NOT NULL,target_id TEXT NOT NULL,source_id TEXT NOT NULL,"
        "event_type TEXT NOT NULL,amount INTEGER NOT NULL,updated_at TEXT NOT NULL,"
        "PRIMARY KEY(operation_id,target_id))"
    )


__all__ = ["apply_dufang", "apply_dufang_share", "apply_dufang_share_player"]
