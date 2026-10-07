import json

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


def apply_dufang_bet_payout(uow: DatabaseUnitOfWork) -> None:
    """Prepare bet and payout receipts before request handlers use them."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_bets("
        "bet_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,cost INTEGER NOT NULL,"
        "status TEXT NOT NULL,placed_at TEXT NOT NULL,settled_at TEXT)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_bet_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,cost INTEGER NOT NULL,"
        "wallet_stone INTEGER NOT NULL,bet_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_payout_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wallet_stone INTEGER NOT NULL,"
        "gain INTEGER NOT NULL,loss INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    required = {
        "dufang_bets": ({"bet_id", "user_id", "cost", "status", "placed_at", "settled_at"}, "bet_id"),
        "dufang_bet_operations": (
            {"operation_id", "payload", "cost", "wallet_stone", "bet_id", "created_at"},
            "operation_id",
        ),
        "dufang_payout_operations": (
            {"operation_id", "payload", "wallet_stone", "gain", "loss", "created_at"},
            "operation_id",
        ),
    }
    for table, (columns, key_column) in required.items():
        rows = uow.query_all(f'PRAGMA table_info("{table}")')
        actual = {str(row["name"]).casefold() for row in rows}
        primary_key = next(
            (int(row["pk"]) for row in rows if str(row["name"]).casefold() == key_column),
            0,
        )
        if not columns.issubset(actual) or primary_key != 1:
            raise RuntimeError(f"unsupported existing dufang schema: {table}")


def apply_dufang_resolution(uow: DatabaseUnitOfWork) -> None:
    """Persist the accepted random plan and cross-database projection work."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_bet_resolutions("
        "operation_id TEXT PRIMARY KEY,plan_json TEXT NOT NULL,created_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_player_outbox("
        "event_id TEXT PRIMARY KEY,operation_id TEXT NOT NULL,event_type TEXT NOT NULL,"
        "payload_json TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "created_at TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "UNIQUE(operation_id,event_type))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS dufang_player_outbox_pending_idx "
        "ON dufang_player_outbox(status,created_at,event_id)"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS dufang_bets_user_idx "
        "ON dufang_bets(user_id,placed_at,bet_id)"
    )
    required = {
        "dufang_bet_resolutions": {"operation_id", "plan_json", "created_at"},
        "dufang_player_outbox": {
            "event_id", "operation_id", "event_type", "payload_json", "status", "created_at", "updated_at",
        },
    }
    for table, columns in required.items():
        rows = uow.query_all(f'PRAGMA table_info("{table}")')
        actual = {str(row["name"]).casefold() for row in rows}
        key = "operation_id" if table == "dufang_bet_resolutions" else "event_id"
        primary_key = next(
            (int(row["pk"]) for row in rows if str(row["name"]).casefold() == key),
            0,
        )
        if not columns.issubset(actual) or primary_key != 1:
            raise RuntimeError(f"unsupported existing dufang schema: {table}")


def apply_dufang_player_receipts(uow: DatabaseUnitOfWork) -> None:
    """Prepare idempotency receipts for game-to-player stat projections."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_player_operation_receipts("
        "operation_id TEXT NOT NULL,event_type TEXT NOT NULL,payload_json TEXT NOT NULL,"
        "created_at TEXT NOT NULL,PRIMARY KEY(operation_id,event_type))"
    )
    rows = uow.query_all('PRAGMA table_info("dufang_player_operation_receipts")')
    actual = {str(row["name"]).casefold() for row in rows}
    primary_keys = {
        str(row["name"]).casefold(): int(row["pk"])
        for row in rows
    }
    if not {"operation_id", "event_type", "payload_json", "created_at"}.issubset(actual):
        raise RuntimeError("unsupported existing dufang player receipt schema")
    if primary_keys.get("operation_id") != 1 or primary_keys.get("event_type") != 2:
        raise RuntimeError("unsupported existing dufang player receipt key")


def apply_dufang_sharing_preferences(uow: DatabaseUnitOfWork) -> None:
    """Move the old global sharing list into a feature-owned player table."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS dufang_sharing_preferences("
        "user_id TEXT PRIMARY KEY,enabled_at TEXT NOT NULL)"
    )
    tables = {
        str(row[0]).casefold()
        for row in uow.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "global" not in tables:
        return
    columns = {
        str(row[1]).casefold()
        for row in uow.execute('PRAGMA table_info("global")').fetchall()
    }
    if not {"user_id", "unseal_sharing"}.issubset(columns):
        return
    row = uow.query_one('SELECT unseal_sharing FROM "global" WHERE user_id=?', ("global",))
    if row is None or row["unseal_sharing"] is None:
        return
    raw = row["unseal_sharing"]
    users = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(users, dict):
        users = users.get("users", [])
    if not isinstance(users, list):
        raise RuntimeError("unsupported legacy dufang sharing list")
    normalized = tuple(dict.fromkeys(str(user_id).strip() for user_id in users if str(user_id).strip()))
    uow.executemany(
        "INSERT OR IGNORE INTO dufang_sharing_preferences(user_id,enabled_at) VALUES(?,CURRENT_TIMESTAMP)",
        ((user_id,) for user_id in normalized),
    )


__all__ = [
    "apply_dufang",
    "apply_dufang_bet_payout",
    "apply_dufang_player_receipts",
    "apply_dufang_resolution",
    "apply_dufang_share",
    "apply_dufang_share_player",
    "apply_dufang_sharing_preferences",
]
