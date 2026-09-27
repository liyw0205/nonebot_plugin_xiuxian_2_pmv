from ...infrastructure.database import DatabaseUnitOfWork


def apply_sect_fairyland(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS sect_fairyland_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "INSERT OR IGNORE INTO sect_fairyland_feature_migrations(version) VALUES ('sect_fairyland.001')"
    )


def apply_sect_fairyland_player(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS sect_fairyland_claim_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id TEXT NOT NULL,"
        "claim_day TEXT NOT NULL,level INTEGER NOT NULL,minutes INTEGER NOT NULL,"
        "detail_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS sect_fairyland_claim_days("
        "user_id TEXT NOT NULL,sect_id TEXT NOT NULL,claim_day TEXT NOT NULL,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(user_id,sect_id))"
    )

    old_table = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='sect_fairyland_claim'"
    )
    if old_table is None:
        return
    for row in uow.query_all("PRAGMA table_info(sect_fairyland_claim)"):
        field = str(row["name"])
        if not field.startswith("last_claim_"):
            continue
        sect_id = field.removeprefix("last_claim_")
        if not sect_id:
            continue
        quoted = '"' + field.replace('"', '""') + '"'
        uow.execute(
            "INSERT OR IGNORE INTO sect_fairyland_claim_days(user_id,sect_id,claim_day) "
            f"SELECT user_id,?,CAST({quoted} AS TEXT) FROM sect_fairyland_claim "
            f"WHERE {quoted} IS NOT NULL AND CAST({quoted} AS TEXT)<>''",
            (sect_id,),
        )


__all__ = ["apply_sect_fairyland", "apply_sect_fairyland_player"]
