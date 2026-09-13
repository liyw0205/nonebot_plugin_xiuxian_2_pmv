from ...infrastructure.database import DatabaseUnitOfWork


def apply_tianti_training(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS tianti_training_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "INSERT OR IGNORE INTO tianti_training_feature_migrations(version) VALUES ('tianti_training.001')"
    )


__all__ = ["apply_tianti_training"]
