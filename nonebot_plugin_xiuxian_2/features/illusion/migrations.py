from ...infrastructure.database import DatabaseUnitOfWork
from .repository import IllusionRepository


def apply_illusion(uow: DatabaseUnitOfWork) -> None:
    IllusionRepository().ensure_schema(uow)
    uow.execute("CREATE TABLE IF NOT EXISTS illusion_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute(
        "INSERT OR IGNORE INTO illusion_feature_migrations(version) VALUES (?)",
        ("illusion.001",),
    )
    # Keep the historical marker visible for upgraded installations and the
    # one-release compatibility migration in the legacy catalog.
    uow.execute(
        "INSERT OR IGNORE INTO illusion_feature_migrations(version) VALUES (?)",
        ("legacy.illusion.001",),
    )


def apply_illusion_state(uow: DatabaseUnitOfWork) -> None:
    IllusionRepository().ensure_schema(uow)
    uow.execute(
        "INSERT OR IGNORE INTO illusion_feature_migrations(version) VALUES (?)",
        ("illusion.002",),
    )


__all__ = ["apply_illusion", "apply_illusion_state"]
