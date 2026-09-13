from ...infrastructure.database import DatabaseUnitOfWork


def apply_pet(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS pet_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO pet_feature_migrations(version) VALUES ('pet.001')")


__all__ = ["apply_pet"]
