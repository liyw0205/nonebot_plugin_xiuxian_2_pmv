from ...infrastructure.database import DatabaseUnitOfWork


PRAYER_STATISTICS_COLUMNS = ("祈愿石使用", "传承新卡", "传承重复卡")
LOVE_SAND_STATISTICS_COLUMNS = ("思恋流沙使用", "思恋结晶获取")
DRAW_STATISTICS_COLUMNS = (
    "传承抽卡",
    "传承抽卡次数",
    "传承抽卡灵石消耗",
    "传承祈愿",
    "传承祈愿次数",
    "思恋结晶消耗",
    "虚神界时间获取",
    "传承保底次数",
    "传承新卡",
    "传承重复卡",
)


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


def apply_impart_love_sand_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS love_sand_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,"
        "stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)"
    )


def apply_impart_love_sand_player_statistics(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(statistics)").fetchall()}
    for column in LOVE_SAND_STATISTICS_COLUMNS:
        if column not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{column}" INTEGER DEFAULT 0')


def apply_impart_draw_operations(uow: DatabaseUnitOfWork) -> None:
    """Create the paid-draw receipt schema during startup migrations."""

    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_draw_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wish INTEGER NOT NULL,"
        "draw_count INTEGER NOT NULL,cards_json TEXT NOT NULL)"
    )
    columns = {
        str(row[1])
        for row in uow.execute("PRAGMA table_info(impart_draw_operations)").fetchall()
    }
    if "cards_json" not in columns:
        uow.execute(
            "ALTER TABLE impart_draw_operations ADD COLUMN cards_json TEXT DEFAULT '[]'"
        )


def apply_impart_crystal_draw_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_crystal_draw_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,wish INTEGER NOT NULL,"
        "stone_num INTEGER NOT NULL,exp_day INTEGER NOT NULL,cards_json TEXT NOT NULL)"
    )


def apply_impart_card_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_card_compose_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_card_disassemble_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "card_quantity INTEGER NOT NULL,stone_quantity INTEGER NOT NULL)"
    )


def apply_impart_crystal_and_card_operations(uow: DatabaseUnitOfWork) -> None:
    apply_impart_crystal_draw_operations(uow)
    apply_impart_card_operations(uow)


def apply_impart_draw_player_statistics(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(statistics)").fetchall()}
    for column in DRAW_STATISTICS_COLUMNS:
        if column not in columns:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{column}" INTEGER DEFAULT 0')


__all__ = [
    "LOVE_SAND_STATISTICS_COLUMNS",
    "DRAW_STATISTICS_COLUMNS",
    "PRAYER_STATISTICS_COLUMNS",
    "apply_impart",
    "apply_impart_love_sand_operations",
    "apply_impart_love_sand_player_statistics",
    "apply_impart_prayer_operations",
    "apply_impart_prayer_player_statistics",
    "apply_impart_draw_operations",
    "apply_impart_crystal_draw_operations",
    "apply_impart_card_operations",
    "apply_impart_crystal_and_card_operations",
    "apply_impart_draw_player_statistics",
]
