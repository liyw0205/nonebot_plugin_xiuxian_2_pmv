from ...infrastructure.database import DatabaseUnitOfWork


def apply_training(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS training_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO training_feature_migrations(version) VALUES ('legacy.training.001')")


__all__ = ["apply_training"]
