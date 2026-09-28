from ...infrastructure.database import DatabaseUnitOfWork


def apply_work(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS work_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO work_feature_migrations(version) VALUES ('work.001')")


def apply_work_daily_refresh_reset(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_operations("
        "business_date TEXT PRIMARY KEY,reset_count INTEGER NOT NULL,total INTEGER NOT NULL,"
        "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
        "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_targets("
        "business_date TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
        "previous_count INTEGER,final_count INTEGER,updated_at TEXT NOT NULL,"
        "PRIMARY KEY(business_date,user_id))"
    )


def apply_work_item_use(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_item_use_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,action TEXT NOT NULL,"
        "item_remaining INTEGER NOT NULL,result_snapshot TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_work_offer_snapshots(uow: DatabaseUnitOfWork) -> None:
    """Ensure the legacy-compatible offer projection without replacing rows."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
        "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )


def apply_work_refresh_operations(uow: DatabaseUnitOfWork) -> None:
    """Pre-create the refresh ledger while preserving existing receipts."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_refresh_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
        "offer_snapshot TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_work_abort_cleanup(uow: DatabaseUnitOfWork) -> None:
    """Prepare abort/reset snapshots and replay receipts before requests."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_active_snapshots("
        "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_abort_cleanup_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reason TEXT NOT NULL,"
        "penalty INTEGER NOT NULL,stone_remaining INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_work_claim_operations(uow: DatabaseUnitOfWork) -> None:
    """Prepare claim replay receipts before request handling."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS work_claim_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,"
        "started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = [
    "apply_work",
    "apply_work_abort_cleanup",
    "apply_work_claim_operations",
    "apply_work_daily_refresh_reset",
    "apply_work_item_use",
    "apply_work_offer_snapshots",
    "apply_work_refresh_operations",
]
