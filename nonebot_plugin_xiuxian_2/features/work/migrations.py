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


__all__ = ["apply_work", "apply_work_daily_refresh_reset"]
