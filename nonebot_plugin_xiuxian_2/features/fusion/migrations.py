from ...infrastructure.database import DatabaseUnitOfWork


def apply_fusion(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS fusion_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO fusion_feature_migrations(version) VALUES ('legacy.fusion.001')")


__all__ = ["apply_fusion"]
