from ...infrastructure.database import DatabaseUnitOfWork


def apply_puppet(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS puppet_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO puppet_feature_migrations(version) VALUES ('puppet.001')")


def apply_puppet_status(uow: DatabaseUnitOfWork) -> None:
    columns = {
        str(row["name"]).casefold()
        for row in uow.query_all('PRAGMA main.table_info("user_xiuxian")')
    }
    if not columns:
        return
    if "puppet_status" not in columns:
        uow.execute("ALTER TABLE user_xiuxian ADD COLUMN puppet_status INTEGER DEFAULT 0")


def apply_puppet_operations(uow: DatabaseUnitOfWork) -> None:
    """Startup migration for the puppet operation ledgers.

    Both tables used to be created lazily inside the request transaction.  They
    are part of the startup schema now so request paths never execute DDL; the
    statements stay ``IF NOT EXISTS`` so databases that already took the lazy
    path keep every existing receipt row.
    """
    uow.execute("CREATE TABLE IF NOT EXISTS puppet_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "CREATE TABLE IF NOT EXISTS puppet_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,action TEXT NOT NULL,"
        "previous_level INTEGER NOT NULL,current_level INTEGER NOT NULL,"
        "stone_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS puppet_harvest_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,"
        "rewards_json TEXT NOT NULL,stone_cost INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute("INSERT OR IGNORE INTO puppet_feature_migrations(version) VALUES ('puppet.003')")


__all__ = ["apply_puppet", "apply_puppet_operations", "apply_puppet_status"]
