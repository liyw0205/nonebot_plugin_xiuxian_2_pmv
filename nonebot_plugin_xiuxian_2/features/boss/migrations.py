from ...infrastructure.database import DatabaseUnitOfWork


def apply_boss(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS boss_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO boss_feature_migrations(version) VALUES ('boss.001')")


__all__ = ["apply_boss"]
