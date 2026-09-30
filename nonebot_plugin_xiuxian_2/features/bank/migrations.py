import shutil
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


def apply_bank(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS bank_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO bank_feature_migrations(version) VALUES ('bank.001')")


def apply_bank_accounts(uow: DatabaseUnitOfWork) -> None:
    """Create the new game-db-owned first-use bank projection."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS bank_accounts ("
        "user_id TEXT PRIMARY KEY, saved_stone INTEGER NOT NULL, "
        "bank_level TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS bank_account_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, payload TEXT NOT NULL, "
        "deposited INTEGER NOT NULL, interest INTEGER NOT NULL, wallet_after INTEGER NOT NULL, "
        "saved_after INTEGER NOT NULL, created_at TEXT NOT NULL)"
    )


def _assert_legacy_account_migration_space(
    uow: DatabaseUnitOfWork, *, row_count: int, payload_bytes: int
) -> None:
    # Estimate only the imported table, not the potentially much larger player DB.
    required = 8 * 1024 * 1024 + row_count * 512 + payload_bytes * 4
    available = shutil.disk_usage(uow.database.parent).free
    if available < required:
        raise RuntimeError(
            f"bank account migration needs about {required} bytes free; found {available}"
        )


def apply_bank_legacy_accounts(uow: DatabaseUnitOfWork) -> None:
    """Import the legacy player projection without replacing newer game state."""
    legacy_database = uow.database.parent / "player.db"
    source_rows = imported_rows = retained_rows = 0
    if legacy_database.is_file():
        with DatabaseUnitOfWork(legacy_database, read_only=True) as legacy:
            table = legacy.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='bankinfo'"
            )
            if table is not None:
                columns = {
                    str(row["name"]).casefold()
                    for row in legacy.query_all('PRAGMA table_info("bankinfo")')
                }
                required = {"user_id", "savestone", "savetime", "banklevel"}
                missing = sorted(required - columns)
                if missing:
                    raise RuntimeError(
                        f"legacy bankinfo schema incomplete: missing {','.join(missing)}"
                    )

                size = legacy.query_one(
                    'SELECT COUNT(*) AS row_count, '
                    "COALESCE(SUM(length(CAST(COALESCE(user_id,'') AS BLOB)) + "
                    "length(CAST(COALESCE(savestone,0) AS BLOB)) + "
                    "length(CAST(COALESCE(savetime,'') AS BLOB)) + "
                    "length(CAST(COALESCE(banklevel,'1') AS BLOB))),0) AS payload_bytes "
                    'FROM "bankinfo"'
                )
                _assert_legacy_account_migration_space(
                    uow,
                    row_count=int(size["row_count"]),
                    payload_bytes=int(size["payload_bytes"]),
                )

                last_rowid = 0
                while True:
                    rows = legacy.query_all(
                        "SELECT rowid AS legacy_rowid,user_id,savestone,savetime,banklevel "
                        'FROM "bankinfo" WHERE rowid>? ORDER BY rowid LIMIT 200',
                        (last_rowid,),
                    )
                    if not rows:
                        break
                    for row in rows:
                        source_rows += 1
                        user_id = str(row["user_id"] or "").strip()
                        if not user_id:
                            raise RuntimeError("legacy bankinfo row has an empty user_id")
                        saved_stone = int(row["savestone"] or 0)
                        bank_level = str(row["banklevel"] or "1")
                        updated_at = str(row["savetime"] or "")
                        existing = uow.query_one(
                            "SELECT saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id=?",
                            (user_id,),
                        )
                        imported = (saved_stone, bank_level, updated_at)
                        if existing is None:
                            uow.execute(
                                "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) "
                                "VALUES(?,?,?,?)",
                                (user_id, *imported),
                            )
                            imported_rows += 1
                        elif (
                            int(existing["saved_stone"]),
                            str(existing["bank_level"]),
                            str(existing["updated_at"]),
                        ) == imported:
                            retained_rows += 1
                        elif uow.query_one(
                            "SELECT 1 AS present FROM bank_account_operations WHERE user_id=? LIMIT 1",
                            (user_id,),
                        ) is not None:
                            # A game-db receipt proves this account has advanced past its legacy snapshot.
                            retained_rows += 1
                        else:
                            raise RuntimeError(f"bank account migration conflict: {user_id}")
                    last_rowid = int(rows[-1]["legacy_rowid"])

    uow.execute(
        "CREATE TABLE IF NOT EXISTS bank_account_legacy_migration_audit "
        "(source_table TEXT PRIMARY KEY,source_rows INTEGER NOT NULL,"
        "imported_rows INTEGER NOT NULL,retained_rows INTEGER NOT NULL,"
        "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "INSERT INTO bank_account_legacy_migration_audit"
        "(source_table,source_rows,imported_rows,retained_rows) VALUES(?,?,?,?) "
        "ON CONFLICT(source_table) DO UPDATE SET source_rows=excluded.source_rows,"
        "imported_rows=excluded.imported_rows,retained_rows=excluded.retained_rows,"
        "applied_at=CURRENT_TIMESTAMP",
        ("player.bankinfo", source_rows, imported_rows, retained_rows),
    )


__all__ = ["apply_bank", "apply_bank_accounts", "apply_bank_legacy_accounts"]
