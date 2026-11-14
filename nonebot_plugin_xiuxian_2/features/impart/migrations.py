from datetime import datetime, timezone
import json

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

IMPART_DUPLICATE_ARCHIVE_TABLE = "xiuxian_impart_duplicate_archive"
IMPART_DUPLICATE_RECONCILIATION_VERSION = "impart.012"
_IMPART_DUPLICATE_ARCHIVE_FIELDS = {
    "__xiuxian_archive_source_rowid",
    "__xiuxian_archive_effective_rowid",
    "__xiuxian_archive_classification",
    "__xiuxian_archive_conflicting_fields",
    "__xiuxian_archive_migration",
    "__xiuxian_archive_created_at",
    "__xiuxian_archive_restored_at",
}


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


def apply_impart_duplicate_row_reconciliation(uow: DatabaseUnitOfWork) -> dict[str, int]:
    """Archive duplicate legacy rows, retaining the earliest row as active state.

    The archive is a same-database recovery copy written in the same startup
    transaction as the active-row deletes and unique index. Divergent snapshots
    are never merged; their differing fields are recorded for later review.
    """
    table = uow.query_one(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='xiuxian_impart'"
    )
    if table is None:
        return {"archived": 0, "conflicts": 0}
    schema = str(table.get("sql") or "")
    if "WITHOUT ROWID" in schema.upper():
        raise RuntimeError("cannot reconcile xiuxian_impart: stable rowid is unavailable")

    table_info = uow.execute('PRAGMA table_xinfo("xiuxian_impart")').fetchall()
    unsupported_hidden = [str(row[1]) for row in table_info if int(row[6] or 0) == 1]
    if unsupported_hidden:
        raise RuntimeError(
            "cannot reconcile xiuxian_impart: hidden virtual-table columns are unsupported: "
            + ",".join(sorted(unsupported_hidden))
        )
    source_columns = [str(row[1]) for row in table_info]
    if "user_id" not in source_columns:
        raise RuntimeError("cannot reconcile xiuxian_impart: user_id column is unavailable")
    normalized_columns = {column.casefold() for column in source_columns}
    if "__xiuxian_reconcile_rowid" in normalized_columns:
        raise RuntimeError("cannot reconcile xiuxian_impart: reserved rowid alias already exists")
    reserved_columns = {column.casefold() for column in _IMPART_DUPLICATE_ARCHIVE_FIELDS}
    collisions = normalized_columns.intersection(reserved_columns)
    if collisions:
        raise RuntimeError(
            "cannot reconcile xiuxian_impart: reserved archive columns already exist: "
            + ",".join(sorted(collisions))
        )

    archive_exists = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
        (IMPART_DUPLICATE_ARCHIVE_TABLE,),
    )
    if archive_exists is None:
        uow.execute(
            f'CREATE TABLE "{IMPART_DUPLICATE_ARCHIVE_TABLE}" AS '
            'SELECT rowid AS "__xiuxian_archive_source_rowid",* '
            'FROM "xiuxian_impart" WHERE 0'
        )

    archive_columns = {
        str(row[1])
        for row in uow.execute(
            f'PRAGMA table_info("{IMPART_DUPLICATE_ARCHIVE_TABLE}")'
        ).fetchall()
    }
    metadata_columns = (
        ("__xiuxian_archive_effective_rowid", "INTEGER"),
        ("__xiuxian_archive_classification", "TEXT"),
        ("__xiuxian_archive_conflicting_fields", "TEXT"),
        ("__xiuxian_archive_migration", "TEXT"),
        ("__xiuxian_archive_created_at", "TEXT"),
        ("__xiuxian_archive_restored_at", "TEXT"),
    )
    for name, declaration in metadata_columns:
        if name not in archive_columns:
            uow.execute(
                f'ALTER TABLE "{IMPART_DUPLICATE_ARCHIVE_TABLE}" '
                f'ADD COLUMN "{name}" {declaration}'
            )
            archive_columns.add(name)

    def quote(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    archive_insert_columns = [
        "__xiuxian_archive_source_rowid",
        *source_columns,
        *(name for name, _declaration in metadata_columns[:-1]),
    ]
    insert_sql = (
        f'INSERT INTO "{IMPART_DUPLICATE_ARCHIVE_TABLE}"('
        + ",".join(quote(column) for column in archive_insert_columns)
        + ") VALUES("
        + ",".join("?" for _ in archive_insert_columns)
        + ")"
    )
    primary_key_columns = {
        str(row[1]) for row in table_info if int(row[5] or 0) > 0
    }
    comparable_columns = [
        column for column in source_columns if column not in primary_key_columns
    ]
    groups = uow.query_all(
        'SELECT user_id,MIN(rowid) AS effective_rowid FROM "xiuxian_impart" '
        "WHERE user_id IS NOT NULL AND length(trim(CAST(user_id AS TEXT)))>0 "
        'GROUP BY user_id HAVING COUNT(*)>1 ORDER BY MIN(rowid)'
    )
    archived = conflicts = 0
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for group in groups:
        rows = uow.query_all(
            'SELECT rowid AS "__xiuxian_reconcile_rowid",* FROM "xiuxian_impart" '
            'WHERE user_id IS ? ORDER BY rowid',
            (group["user_id"],),
        )
        if len(rows) < 2:
            continue
        effective = rows[0]
        effective_rowid = int(effective["__xiuxian_reconcile_rowid"])
        for duplicate in rows[1:]:
            conflicting_fields = [
                column
                for column in comparable_columns
                if type(duplicate[column]) is not type(effective[column])
                or duplicate[column] != effective[column]
            ]
            classification = "divergent_history" if conflicting_fields else "identical_copy"
            source_rowid = int(duplicate["__xiuxian_reconcile_rowid"])
            uow.execute(
                insert_sql,
                (
                    source_rowid,
                    *(duplicate[column] for column in source_columns),
                    effective_rowid,
                    classification,
                    json.dumps(conflicting_fields, ensure_ascii=False, separators=(",", ":")),
                    IMPART_DUPLICATE_RECONCILIATION_VERSION,
                    created_at,
                ),
            )
            deleted = uow.execute(
                'DELETE FROM "xiuxian_impart" WHERE rowid=? AND user_id IS ?',
                (source_rowid, group["user_id"]),
            )
            if deleted.rowcount != 1:
                raise RuntimeError(
                    "cannot reconcile xiuxian_impart: duplicate row changed during startup"
                )
            archived += 1
            conflicts += classification == "divergent_history"

    uow.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS xiuxian_impart_user_id_unique '
        'ON "xiuxian_impart"(user_id) '
        "WHERE user_id IS NOT NULL AND length(trim(CAST(user_id AS TEXT)))>0"
    )
    return {"archived": archived, "conflicts": conflicts}


def restore_impart_duplicate_archive(uow: DatabaseUnitOfWork) -> int:
    """Restore archived rows and unmark impart.012 inside the caller's transaction."""
    archive = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
        (IMPART_DUPLICATE_ARCHIVE_TABLE,),
    )
    if archive is None:
        return 0
    source_columns = [
        str(row[1]) for row in uow.execute('PRAGMA table_info("xiuxian_impart")').fetchall()
    ]
    archive_columns = {
        str(row[1])
        for row in uow.execute(
            f'PRAGMA table_info("{IMPART_DUPLICATE_ARCHIVE_TABLE}")'
        ).fetchall()
    }
    if not _IMPART_DUPLICATE_ARCHIVE_FIELDS.issubset(archive_columns):
        raise RuntimeError("cannot restore xiuxian_impart archive: archive metadata is incomplete")

    def quote(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    uow.execute("DROP INDEX IF EXISTS xiuxian_impart_user_id_unique")
    rows = uow.query_all(
        f'SELECT rowid AS "__archive_rowid",* FROM "{IMPART_DUPLICATE_ARCHIVE_TABLE}" '
        'WHERE "__xiuxian_archive_restored_at" IS NULL ORDER BY rowid'
    )
    restore_columns = [column for column in source_columns if column in archive_columns]
    info = uow.execute('PRAGMA table_info("xiuxian_impart")').fetchall()
    primary_key_columns = [row for row in info if int(row[5] or 0) > 0]
    integer_primary_key = len(primary_key_columns) == 1 and (
        str(primary_key_columns[0][2]).strip().upper() == "INTEGER"
    )
    insert_columns = restore_columns if integer_primary_key else ["rowid", *restore_columns]
    insert_sql = (
        'INSERT INTO "xiuxian_impart"('
        + ",".join(quote(column) for column in insert_columns)
        + ") VALUES("
        + ",".join("?" for _ in insert_columns)
        + ")"
    )
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for row in rows:
        source_rowid = int(row["__xiuxian_archive_source_rowid"])
        if uow.query_one("SELECT 1 FROM xiuxian_impart WHERE rowid=?", (source_rowid,)):
            raise RuntimeError(
                f"cannot restore xiuxian_impart archive rowid={source_rowid}: rowid is occupied"
            )
        values = [row[column] for column in restore_columns]
        if not integer_primary_key:
            values.insert(0, source_rowid)
        uow.execute(insert_sql, values)
        uow.execute(
            f'UPDATE "{IMPART_DUPLICATE_ARCHIVE_TABLE}" '
            'SET "__xiuxian_archive_restored_at"=? WHERE rowid=?',
            (created_at, row["__archive_rowid"]),
        )
    migrations = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    )
    if migrations is not None:
        uow.execute(
            "DELETE FROM schema_migrations WHERE version=?",
            (IMPART_DUPLICATE_RECONCILIATION_VERSION,),
        )
    return len(rows)


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
