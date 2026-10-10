from ...infrastructure.database import DatabaseUnitOfWork

MIGRATION_VERSION = "game_events.001"


def apply_game_event_statistics_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
    for name in ("宠物游历领取", "宠物游历次数", "宠物游历时长", "地图委托完成"):
        if name not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{name}" INTEGER DEFAULT 0')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS game_event_statistics_events("
        "event_id TEXT NOT NULL,event_key TEXT NOT NULL,user_id TEXT NOT NULL,"
        "increment INTEGER NOT NULL,created_at TEXT NOT NULL,"
        "PRIMARY KEY(event_id,event_key))"
    )


MIGRATIONS = (MIGRATION_VERSION,)


__all__ = ["MIGRATION_VERSION", "MIGRATIONS", "apply_game_event_statistics_player"]
