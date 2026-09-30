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


def apply_admin_stone_batch(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_stone_batch_operations("
        "operation_id TEXT PRIMARY KEY,operator_id TEXT NOT NULL,"
        "requested_delta INTEGER NOT NULL,payload TEXT NOT NULL,total INTEGER NOT NULL,"
        "completed INTEGER NOT NULL DEFAULT 0,applied_delta REAL NOT NULL DEFAULT 0,"
        "affected_users INTEGER NOT NULL DEFAULT 0,skipped_users INTEGER NOT NULL DEFAULT 0,"
        "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_stone_batch_running_idx "
        "ON admin_stone_batch_operations(operator_id,requested_delta,status,created_at)"
    )
    uow.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS admin_stone_batch_single_running_idx "
        "ON admin_stone_batch_operations(operator_id,requested_delta) WHERE status='running'"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_stone_batch_progress("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
        "previous_stone REAL,final_stone REAL,applied_delta REAL NOT NULL DEFAULT 0,"
        "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(operation_id,user_id),"
        "FOREIGN KEY(operation_id) REFERENCES admin_stone_batch_operations(operation_id))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_stone_batch_progress_pending_idx "
        "ON admin_stone_batch_progress(operation_id,status,user_id)"
    )


def apply_admin_exp_adjustment(uow: DatabaseUnitOfWork) -> None:
    # economy_log and its trace_id column are owned by admin_asset.002.
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_exp_adjustment_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "previous_exp INTEGER NOT NULL,final_exp INTEGER NOT NULL,"
        "applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_admin_item_destroy(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_item_destroy_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "previous_quantity INTEGER NOT NULL,final_quantity INTEGER NOT NULL,"
        "removed_quantity INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_admin_item_grant(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_item_grant_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,user_id TEXT NOT NULL,"
        "item_id INTEGER NOT NULL,previous_quantity INTEGER NOT NULL,"
        "final_quantity INTEGER NOT NULL,granted_quantity INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_admin_realm_changes(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_level_change_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_root_change_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_admin_impart_stone_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_impart_stone_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "previous_stone INTEGER NOT NULL,final_stone INTEGER NOT NULL,"
        "applied_delta INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_admin_accessory_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_accessory_operations("
        "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
        "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_admin_accessory_batch(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_accessory_batch_operations("
        "operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,"
        "total INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'running',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_accessory_batch_running_idx "
        "ON admin_accessory_batch_operations(action,status,created_at)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_accessory_batch_progress("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
        "affected_quantity INTEGER NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(operation_id,user_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_accessory_batch_targets("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "affected_quantity INTEGER NOT NULL DEFAULT 0,result_json TEXT NOT NULL DEFAULT '{}',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(operation_id,user_id),"
        "FOREIGN KEY(operation_id) REFERENCES admin_accessory_batch_operations(operation_id))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_accessory_batch_targets_pending_idx "
        "ON admin_accessory_batch_targets(operation_id,status,user_id)"
    )


def apply_admin_impart_stone_batch(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,total INTEGER NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'running',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_impart_stone_batch_running_idx "
        "ON admin_impart_stone_batch_operations(status,created_at)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_progress("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL,"
        "applied_delta INTEGER NOT NULL,result_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(operation_id,user_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS admin_impart_stone_batch_targets("
        "operation_id TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "applied_delta INTEGER NOT NULL DEFAULT 0,result_json TEXT NOT NULL DEFAULT '{}',"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(operation_id,user_id),"
        "FOREIGN KEY(operation_id) REFERENCES admin_impart_stone_batch_operations(operation_id))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS admin_impart_stone_batch_targets_pending_idx "
        "ON admin_impart_stone_batch_targets(operation_id,status,user_id)"
    )


__all__ = [
    "apply_admin_asset",
    "apply_admin_accessory_operations",
    "apply_admin_accessory_batch",
    "apply_admin_exp_adjustment",
    "apply_admin_item_destroy",
    "apply_admin_item_grant",
    "apply_admin_impart_stone_operations",
    "apply_admin_impart_stone_batch",
    "apply_admin_realm_changes",
    "apply_admin_stone_adjustment",
    "apply_admin_stone_batch",
]
