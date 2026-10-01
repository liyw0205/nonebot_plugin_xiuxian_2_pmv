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


__all__ = ["apply_puppet", "apply_puppet_status"]
