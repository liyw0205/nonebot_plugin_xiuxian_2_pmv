from ...infrastructure.database import DatabaseUnitOfWork


def apply_simulator(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS simulator_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO simulator_feature_migrations(version) VALUES ('legacy.simulator.001')")


__all__ = ["apply_simulator"]
