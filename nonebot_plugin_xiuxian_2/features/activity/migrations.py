import shutil
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


def apply_activity(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS activity_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO activity_feature_migrations(version) VALUES ('legacy.activity.001')")


_ACTIVITY_STATE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS activity_user (user_id TEXT PRIMARY KEY,sign_days INTEGER NOT NULL DEFAULT 0,last_sign_date TEXT DEFAULT '',total_sign_days INTEGER NOT NULL DEFAULT 0,create_time TEXT DEFAULT '',update_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_sign_log (id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT NOT NULL,sign_date TEXT NOT NULL,day_index INTEGER NOT NULL DEFAULT 0,reward TEXT DEFAULT '',milestone_reward TEXT DEFAULT '',reward_status TEXT DEFAULT '',reward_message TEXT DEFAULT '',create_time TEXT DEFAULT '',finish_time TEXT DEFAULT '',UNIQUE(user_id,sign_date))",
    "CREATE TABLE IF NOT EXISTS activity_collect_inventory (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,word_char TEXT NOT NULL,count INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,word_char))",
    "CREATE TABLE IF NOT EXISTS activity_collect_claim (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,phrase TEXT NOT NULL,count INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,phrase))",
    "CREATE TABLE IF NOT EXISTS activity_collect_drop_log (id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT NOT NULL,user_id TEXT NOT NULL,event_key TEXT NOT NULL,word_char TEXT NOT NULL,drop_date TEXT DEFAULT '',create_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_collect_pity_state (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,event_key TEXT NOT NULL,miss_count INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,event_key))",
    "CREATE TABLE IF NOT EXISTS activity_point_balance (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,points INTEGER NOT NULL DEFAULT 0,total_points INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id))",
    "CREATE TABLE IF NOT EXISTS activity_point_event_log (id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT NOT NULL,user_id TEXT NOT NULL,event_key TEXT NOT NULL,points INTEGER NOT NULL DEFAULT 0,record_date TEXT DEFAULT '',create_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_point_purchase (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,item_key TEXT NOT NULL,count INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,item_key))",
    "CREATE TABLE IF NOT EXISTS activity_task_progress (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,scope_type TEXT NOT NULL,scope_key TEXT NOT NULL,task_key TEXT NOT NULL,progress INTEGER NOT NULL DEFAULT 0,target INTEGER NOT NULL DEFAULT 1,claimed INTEGER NOT NULL DEFAULT 0,claim_time TEXT DEFAULT '',update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,scope_type,scope_key,task_key))",
    "CREATE TABLE IF NOT EXISTS activity_task_claim_log (id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT NOT NULL,user_id TEXT NOT NULL,scope_type TEXT NOT NULL,scope_key TEXT NOT NULL,task_key TEXT NOT NULL,reward TEXT DEFAULT '',create_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_pass_balance (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,exp INTEGER NOT NULL DEFAULT 0,total_exp INTEGER NOT NULL DEFAULT 0,level INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id))",
    "CREATE TABLE IF NOT EXISTS activity_pass_event_log (id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT NOT NULL,user_id TEXT NOT NULL,event_key TEXT NOT NULL,exp INTEGER NOT NULL DEFAULT 0,record_date TEXT DEFAULT '',create_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_pass_reward_claim (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,level INTEGER NOT NULL,create_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,level))",
    "CREATE TABLE IF NOT EXISTS activity_item_inventory (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,item_id TEXT NOT NULL,count INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,item_id))",
    "CREATE TABLE IF NOT EXISTS activity_boss_state (activity_key TEXT PRIMARY KEY,hp_left INTEGER NOT NULL,max_hp INTEGER NOT NULL,update_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_boss_damage (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,total_damage INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id))",
    "CREATE TABLE IF NOT EXISTS activity_boss_fight_log (id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT NOT NULL,user_id TEXT NOT NULL,damage INTEGER NOT NULL DEFAULT 0,fight_date TEXT DEFAULT '',source TEXT DEFAULT '',create_time TEXT DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS activity_boss_milestone (activity_key TEXT NOT NULL,milestone_key TEXT NOT NULL,unlocked_time TEXT DEFAULT '',PRIMARY KEY(activity_key,milestone_key))",
    "CREATE TABLE IF NOT EXISTS activity_boss_milestone_claim (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,milestone_key TEXT NOT NULL,create_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,milestone_key))",
    "CREATE TABLE IF NOT EXISTS activity_boss_rank_claim (activity_key TEXT NOT NULL,user_id TEXT NOT NULL,tier_key TEXT NOT NULL,create_time TEXT DEFAULT '',PRIMARY KEY(activity_key,user_id,tier_key))",
    "CREATE TABLE IF NOT EXISTS activity_sign_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,sign_days INTEGER NOT NULL,total_sign_days INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS activity_point_purchase_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,cost INTEGER NOT NULL,points INTEGER NOT NULL,personal_count INTEGER NOT NULL,total_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS activity_collect_exchange_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE TABLE IF NOT EXISTS activity_boss_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,damage INTEGER NOT NULL,hp_left INTEGER NOT NULL,max_hp INTEGER NOT NULL,fight_count INTEGER NOT NULL,inventory INTEGER,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    "CREATE INDEX IF NOT EXISTS idx_activity_point_event_daily ON activity_point_event_log(activity_key,user_id,event_key,record_date)",
    "CREATE INDEX IF NOT EXISTS idx_activity_pass_event_daily ON activity_pass_event_log(activity_key,user_id,event_key,record_date)",
    "CREATE INDEX IF NOT EXISTS idx_activity_collect_drop_daily ON activity_collect_drop_log(activity_key,user_id,drop_date)",
    "CREATE INDEX IF NOT EXISTS idx_activity_boss_fight_daily ON activity_boss_fight_log(activity_key,user_id,fight_date,source)",
)

_ACTIVITY_STATE_TABLES = (
    ("activity_user", ("user_id", "sign_days", "last_sign_date", "total_sign_days", "create_time", "update_time"), ("user_id",), {"sign_days": 0, "last_sign_date": "", "total_sign_days": "sign_days", "create_time": "", "update_time": ""}),
    ("activity_sign_log", ("id", "user_id", "sign_date", "day_index", "reward", "milestone_reward", "reward_status", "reward_message", "create_time", "finish_time"), ("user_id", "sign_date"), {"reward": "", "milestone_reward": "", "reward_status": "", "reward_message": "", "create_time": "", "finish_time": ""}),
    ("activity_collect_inventory", ("activity_key", "user_id", "word_char", "count", "update_time"), ("activity_key", "user_id", "word_char"), {"count": 0, "update_time": ""}),
    ("activity_collect_claim", ("activity_key", "user_id", "phrase", "count", "update_time"), ("activity_key", "user_id", "phrase"), {"count": 0, "update_time": ""}),
    ("activity_collect_drop_log", ("id", "activity_key", "user_id", "event_key", "word_char", "drop_date", "create_time"), ("id",), {"drop_date": "", "create_time": ""}),
    ("activity_collect_pity_state", ("activity_key", "user_id", "event_key", "miss_count", "update_time"), ("activity_key", "user_id", "event_key"), {"miss_count": 0, "update_time": ""}),
    ("activity_point_balance", ("activity_key", "user_id", "points", "total_points", "update_time"), ("activity_key", "user_id"), {"points": 0, "total_points": 0, "update_time": ""}),
    ("activity_point_event_log", ("id", "activity_key", "user_id", "event_key", "points", "record_date", "create_time"), ("id",), {"points": 0, "record_date": "", "create_time": ""}),
    ("activity_point_purchase", ("activity_key", "user_id", "item_key", "count", "update_time"), ("activity_key", "user_id", "item_key"), {"count": 0, "update_time": ""}),
    ("activity_task_progress", ("activity_key", "user_id", "scope_type", "scope_key", "task_key", "progress", "target", "claimed", "claim_time", "update_time"), ("activity_key", "user_id", "scope_type", "scope_key", "task_key"), {"progress": 0, "target": 1, "claimed": 0, "claim_time": "", "update_time": ""}),
    ("activity_task_claim_log", ("id", "activity_key", "user_id", "scope_type", "scope_key", "task_key", "reward", "create_time"), ("id",), {"reward": "", "create_time": ""}),
    ("activity_pass_balance", ("activity_key", "user_id", "exp", "total_exp", "level", "update_time"), ("activity_key", "user_id"), {"exp": 0, "total_exp": 0, "level": 0, "update_time": ""}),
    ("activity_pass_event_log", ("id", "activity_key", "user_id", "event_key", "exp", "record_date", "create_time"), ("id",), {"exp": 0, "record_date": "", "create_time": ""}),
    ("activity_pass_reward_claim", ("activity_key", "user_id", "level", "create_time"), ("activity_key", "user_id", "level"), {"create_time": ""}),
    ("activity_item_inventory", ("activity_key", "user_id", "item_id", "count", "update_time"), ("activity_key", "user_id", "item_id"), {"count": 0, "update_time": ""}),
    ("activity_boss_state", ("activity_key", "hp_left", "max_hp", "update_time"), ("activity_key",), {"update_time": ""}),
    ("activity_boss_damage", ("activity_key", "user_id", "total_damage", "update_time"), ("activity_key", "user_id"), {"total_damage": 0, "update_time": ""}),
    ("activity_boss_fight_log", ("id", "activity_key", "user_id", "damage", "fight_date", "source", "create_time"), ("id",), {"damage": 0, "fight_date": "", "source": "", "create_time": ""}),
    ("activity_boss_milestone", ("activity_key", "milestone_key", "unlocked_time"), ("activity_key", "milestone_key"), {"unlocked_time": ""}),
    ("activity_boss_milestone_claim", ("activity_key", "user_id", "milestone_key", "create_time"), ("activity_key", "user_id", "milestone_key"), {"create_time": ""}),
    ("activity_boss_rank_claim", ("activity_key", "user_id", "tier_key", "create_time"), ("activity_key", "user_id", "tier_key"), {"create_time": ""}),
    ("activity_sign_settlement_operations", ("operation_id", "payload", "sign_days", "total_sign_days", "created_at"), ("operation_id",), {"created_at": "CURRENT_TIMESTAMP"}),
    ("activity_point_purchase_operations", ("operation_id", "payload", "quantity", "cost", "points", "personal_count", "total_count", "created_at"), ("operation_id",), {"created_at": "CURRENT_TIMESTAMP"}),
    ("activity_collect_exchange_operations", ("operation_id", "payload", "result_json", "created_at"), ("operation_id",), {"created_at": "CURRENT_TIMESTAMP"}),
    ("activity_boss_settlement_operations", ("operation_id", "payload", "damage", "hp_left", "max_hp", "fight_count", "inventory", "created_at"), ("operation_id",), {"inventory": None, "created_at": "CURRENT_TIMESTAMP"}),
)


def _assert_activity_migration_space(uow: DatabaseUnitOfWork, legacy_database: Path) -> None:
    source_size = legacy_database.stat().st_size
    wal_path = legacy_database.with_name(f"{legacy_database.name}-wal")
    if wal_path.is_file():
        source_size += wal_path.stat().st_size
    required = source_size * 2 + 128 * 1024 * 1024
    available = shutil.disk_usage(uow.database.parent).free
    if available < required:
        raise RuntimeError(
            f"activity_state migration needs about {required} bytes free; found {available}"
        )


def apply_activity_state_schema(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if legacy_database.is_file():
        _assert_activity_migration_space(uow, legacy_database)
    for statement in _ACTIVITY_STATE_SCHEMA:
        uow.execute(statement)


def _copy_activity_table(uow: DatabaseUnitOfWork, legacy: DatabaseUnitOfWork, table: str, columns: tuple[str, ...], keys: tuple[str, ...], defaults: dict[str, object]) -> int:
    legacy_columns = {str(row["name"]) for row in legacy.query_all(f"PRAGMA table_info({table})")}
    if not legacy_columns:
        return 0
    missing = [column for column in keys if column not in legacy_columns]
    missing.extend(
        column for column in columns
        if column not in legacy_columns and column not in defaults
    )
    if missing:
        raise RuntimeError(f"legacy activity schema incomplete: {table} missing {','.join(dict.fromkeys(missing))}")

    source_columns = tuple(column for column in columns if column in legacy_columns)
    source_select_columns = ",".join(source_columns)
    insert_sql = f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})"
    key_where = " AND ".join(f"{key}=?" for key in keys)
    existing_sql = f"SELECT {','.join(columns)} FROM {table} WHERE {key_where}"
    last_rowid = 0
    source_rows = 0
    while True:
        rows = legacy.query_all(
            f"SELECT rowid AS legacy_rowid,{source_select_columns} FROM {table} "
            "WHERE rowid>? ORDER BY rowid LIMIT 200",
            (last_rowid,),
        )
        if not rows:
            return source_rows
        for row in rows:
            source_rows += 1
            values = {}
            for column in columns:
                if column in legacy_columns:
                    values[column] = row[column]
                elif defaults[column] == "sign_days":
                    values[column] = row.get("sign_days", 0)
                elif defaults[column] == "CURRENT_TIMESTAMP":
                    values[column] = ""
                else:
                    values[column] = defaults[column]
            identity = tuple(values[key] for key in keys)
            existing = uow.query_one(existing_sql, identity)
            current = tuple(existing[column] for column in columns) if existing is not None else None
            imported = tuple(values[column] for column in columns)
            if current is not None:
                if current != imported:
                    raise RuntimeError(f"activity state migration conflict: {table} {identity!r}")
            else:
                uow.execute(insert_sql, imported)
        last_rowid = int(rows[-1]["legacy_rowid"])


def apply_activity_state_legacy(uow: DatabaseUnitOfWork) -> None:
    legacy_database = uow.database.parent / "activity" / "activity.db"
    if not legacy_database.is_file():
        return
    _assert_activity_migration_space(uow, legacy_database)
    source_rows: dict[str, int] = {}
    with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
        for table, columns, keys, defaults in _ACTIVITY_STATE_TABLES:
            source_rows[table] = _copy_activity_table(uow, legacy, table, columns, keys, defaults)
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_state_migration_audit "
        "(table_name TEXT PRIMARY KEY,source_rows INTEGER NOT NULL,applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    for table, count in source_rows.items():
        uow.execute(
            "INSERT INTO activity_state_migration_audit(table_name,source_rows) VALUES(?,?) "
            "ON CONFLICT(table_name) DO UPDATE SET source_rows=excluded.source_rows",
            (table, count),
        )


__all__ = ["apply_activity", "apply_activity_state_schema", "apply_activity_state_legacy"]
