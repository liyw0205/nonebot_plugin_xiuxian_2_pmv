from ...infrastructure.database import DatabaseUnitOfWork


def apply_buff(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS buff_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO buff_feature_migrations(version) VALUES ('buff.001')")


__all__ = ["apply_buff"]
