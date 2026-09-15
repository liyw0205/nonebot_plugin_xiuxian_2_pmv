from ...infrastructure.database import DatabaseUnitOfWork


def apply_title(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS title_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO title_feature_migrations(version) VALUES ('title.001')")


def apply_title_schema(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS title(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(title)").fetchall()}
    if "unlocked" not in columns:
        uow.execute("ALTER TABLE title ADD COLUMN unlocked TEXT")
    if "equipped" not in columns:
        uow.execute("ALTER TABLE title ADD COLUMN equipped TEXT")
    uow.execute("CREATE TABLE IF NOT EXISTS title_transaction_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,title_id TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")


__all__ = ["apply_title", "apply_title_schema"]
