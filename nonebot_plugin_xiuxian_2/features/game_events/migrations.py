from ...infrastructure.database import DatabaseUnitOfWork

MIGRATION_VERSION = "game_events.001"
DONGFU_HARVEST_STATISTICS_MIGRATION_VERSION = "game_events.002"


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


def apply_dongfu_harvest_statistics_player(uow: DatabaseUnitOfWork) -> None:
    """Add the harvest projection to player databases upgraded from .001."""
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(statistics)")}
    if "洞府收获" not in columns:
        uow.execute('ALTER TABLE statistics ADD COLUMN "洞府收获" INTEGER DEFAULT 0')


MIGRATIONS = (MIGRATION_VERSION, DONGFU_HARVEST_STATISTICS_MIGRATION_VERSION)


__all__ = [
    "DONGFU_HARVEST_STATISTICS_MIGRATION_VERSION",
    "MIGRATION_VERSION",
    "MIGRATIONS",
    "apply_dongfu_harvest_statistics_player",
    "apply_game_event_statistics_player",
]
