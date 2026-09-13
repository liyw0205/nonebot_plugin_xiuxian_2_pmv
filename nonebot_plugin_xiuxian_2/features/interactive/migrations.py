from ...infrastructure.database import DatabaseUnitOfWork
from .repository import InteractiveRepository

MIGRATION_VERSION = "interactive.001"


def apply_interactive(uow: DatabaseUnitOfWork) -> None:
    InteractiveRepository().ensure_schema(uow)
    uow.execute("CREATE TABLE IF NOT EXISTS interactive_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO interactive_feature_migrations(version) VALUES (?)", (MIGRATION_VERSION,))


__all__ = ["MIGRATION_VERSION", "apply_interactive"]
