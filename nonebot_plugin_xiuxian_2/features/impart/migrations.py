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


def apply_impart_base_schema(uow: DatabaseUnitOfWork) -> None:
    """Own the legacy base schema at startup, preserving duplicate legacy rows."""
    uow.execute("CREATE TABLE IF NOT EXISTS xiuxian_impart(id INTEGER PRIMARY KEY,user_id TEXT DEFAULT 0)")
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(xiuxian_impart)").fetchall()}
    for column in (
        "impart_hp_per", "impart_atk_per", "impart_mp_per", "impart_exp_up", "boss_atk",
        "impart_know_per", "impart_burst_per", "impart_mix_per", "impart_reap_per",
        "impart_two_exp", "stone_num", "impart_lv", "impart_num", "exp_day", "wish",
    ):
        if column not in columns:
            uow.execute(f'ALTER TABLE xiuxian_impart ADD COLUMN "{column}" INTEGER DEFAULT 0')
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_cards(user_id TEXT NOT NULL,card_name TEXT NOT NULL,"
        "quantity INTEGER DEFAULT 1,PRIMARY KEY(user_id,card_name))"
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


def _apply_exact_integer_columns(uow: DatabaseUnitOfWork, table: str, fields: tuple[str, ...]) -> None:
    present = uow.query_one(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    if present is None:
        return
    columns = {str(row[1]) for row in uow.execute(f'PRAGMA table_info("{table}")').fetchall()}
    for field in fields:
        exact_field = f"{field}_exact"
        state_field = f"{field}_precision_state"
        if field not in columns:
            continue
        if exact_field not in columns:
            uow.execute(f'ALTER TABLE "{table}" ADD COLUMN "{exact_field}" TEXT')
            columns.add(exact_field)
        if state_field not in columns:
            uow.execute(
                f'ALTER TABLE "{table}" ADD COLUMN "{state_field}" '
                "TEXT NOT NULL DEFAULT 'exact'"
            )
            columns.add(state_field)
        uow.execute(
            f'UPDATE "{table}" SET "{exact_field}"='
            f'xiuxian_crystal_exact_text("{field}",typeof("{field}")), '
            f'"{state_field}"=xiuxian_crystal_precision_state("{field}",typeof("{field}")) '
            f'WHERE "{exact_field}" IS NULL AND "{state_field}"=\'exact\''
        )


def apply_impart_exact_crystal_game_receipts(uow: DatabaseUnitOfWork) -> None:
    _apply_exact_integer_columns(uow, "love_sand_operations", ("stone_num",))
    _apply_exact_integer_columns(
        uow, "admin_impart_stone_operations",
        ("previous_stone", "final_stone", "applied_delta"),
    )


def apply_impart_exact_crystal_balances(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS impart_legacy_crystal_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL)"
    )
    _apply_exact_integer_columns(uow, "xiuxian_impart", ("stone_num",))
    _apply_exact_integer_columns(uow, "impart_crystal_draw_operations", ("stone_num",))
    _apply_exact_integer_columns(uow, "impart_card_disassemble_operations", ("stone_quantity",))
    columns = {
        str(row[1])
        for row in uow.execute('PRAGMA table_info("xiuxian_impart")').fetchall()
    }
    if not {"stone_num_exact", "stone_num_precision_state"}.issubset(columns):
        return
    uow.execute("DROP TRIGGER IF EXISTS impart_crystal_exact_insert")
    uow.execute("DROP TRIGGER IF EXISTS impart_crystal_exact_legacy_update")
    # Older application builds updated only the INTEGER-affinity column. Never
    # let such a write leave a stale exact shadow that would look authoritative.
    uow.execute(
        "CREATE TRIGGER IF NOT EXISTS impart_crystal_exact_write_guard "
        "BEFORE UPDATE OF stone_num ON xiuxian_impart "
        "WHEN NEW.stone_num IS NOT OLD.stone_num "
        "AND NEW.stone_num_exact IS OLD.stone_num_exact BEGIN "
        "SELECT RAISE(ABORT,'crystal balance write requires exact shadow'); END"
    )


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
    "apply_impart_exact_crystal_game_receipts",
    "apply_impart_exact_crystal_balances",
    "apply_impart_draw_player_statistics",
]
