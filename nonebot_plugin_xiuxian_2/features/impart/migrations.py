from ...infrastructure.database import DatabaseUnitOfWork


PRAYER_STATISTICS_COLUMNS = ("祈愿石使用", "传承新卡", "传承重复卡")


def apply_impart(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS impart_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO impart_feature_migrations(version) VALUES ('legacy.impart.001')")


def apply_impart_prayer_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_prayer_operations("
        "operation_id TEXT PRIMARY KEY,identity_json TEXT NOT NULL,cards_json TEXT NOT NULL,"
        "new_cards_json TEXT NOT NULL,card_counts_json TEXT NOT NULL,item_remaining INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_impart_prayer_player_statistics(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(statistics)").fetchall()}
    for column in PRAYER_STATISTICS_COLUMNS:
        if column not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{column}" INTEGER DEFAULT 0')


__all__ = [
    "PRAYER_STATISTICS_COLUMNS",
    "apply_impart",
    "apply_impart_prayer_operations",
    "apply_impart_prayer_player_statistics",
]
