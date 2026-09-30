from ...infrastructure.database import DatabaseUnitOfWork


def apply_admin_asset(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS admin_asset_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO admin_asset_feature_migrations(version) VALUES ('admin_asset.001')")


def apply_admin_stone_adjustment(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_stone_adjustment_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,previous_stone INTEGER NOT NULL,"
        "final_stone INTEGER NOT NULL,applied_delta INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS economy_log("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,source TEXT NOT NULL,"
        "action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,"
        "sect_contribution_delta INTEGER NOT NULL DEFAULT 0,sect_scale_delta INTEGER NOT NULL DEFAULT 0,"
        "sect_materials_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT '[]',"
        "detail TEXT NOT NULL DEFAULT '{}',trace_id TEXT,created_at TEXT NOT NULL)"
    )
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA table_info("economy_log")')
    }
    if "trace_id" not in columns:
        uow.execute("ALTER TABLE economy_log ADD COLUMN trace_id TEXT")


__all__ = ["apply_admin_asset", "apply_admin_stone_adjustment"]
