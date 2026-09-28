from ...infrastructure.database import DatabaseUnitOfWork


def apply_world_events(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS world_events_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO world_events_feature_migrations(version) VALUES ('world_events.001')")


def apply_world_events_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS world_event_state ("
        "user_id TEXT PRIMARY KEY,active INTEGER,status TEXT,event_id TEXT,event_type TEXT,"
        "name TEXT,period TEXT,manual INTEGER,bosses TEXT,participants TEXT,claimed TEXT,"
        "started_at TEXT,ends_at TEXT,last_result TEXT)"
    )
    state_columns = {
        "active": "INTEGER",
        "status": "TEXT",
        "event_id": "TEXT",
        "event_type": "TEXT",
        "name": "TEXT",
        "period": "TEXT",
        "manual": "INTEGER",
        "bosses": "TEXT",
        "participants": "TEXT",
        "claimed": "TEXT",
        "started_at": "TEXT",
        "ends_at": "TEXT",
        "last_result": "TEXT",
    }
    existing_state_columns = {
        str(row[1]) for row in uow.execute("PRAGMA table_info(world_event_state)").fetchall()
    }
    for name, data_type in state_columns.items():
        if name not in existing_state_columns:
            uow.execute(f'ALTER TABLE world_event_state ADD COLUMN "{name}" {data_type}')

    uow.execute(
        "CREATE TABLE IF NOT EXISTS demon_attack_settlement_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TEXT NOT NULL)"
    )
    uow.execute("CREATE TABLE IF NOT EXISTS statistics (user_id TEXT PRIMARY KEY)")
    stat_columns = {
        str(row[1]) for row in uow.execute("PRAGMA table_info(statistics)").fetchall()
    }
    for name in ("魔修入侵参与", "魔修入侵伤害", "魔修入侵击退"):
        if name not in stat_columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{name}" INTEGER')


def apply_world_events_claim(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS demon_claim_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "stone INTEGER NOT NULL DEFAULT 0,exp INTEGER NOT NULL DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    columns = {
        str(row[1]) for row in uow.execute("PRAGMA table_info(demon_claim_operations)").fetchall()
    }
    for name in ("stone", "exp"):
        if name not in columns:
            uow.execute(
                f'ALTER TABLE demon_claim_operations ADD COLUMN "{name}" INTEGER NOT NULL DEFAULT 0'
            )


def apply_world_events_lifecycle(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS demon_event_lifecycle_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TEXT NOT NULL)"
    )


def apply_world_events_wave_refresh(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS demon_wave_refresh_operations ("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TEXT NOT NULL)"
    )


__all__ = [
    "apply_world_events",
    "apply_world_events_player",
    "apply_world_events_claim",
    "apply_world_events_lifecycle",
    "apply_world_events_wave_refresh",
]
