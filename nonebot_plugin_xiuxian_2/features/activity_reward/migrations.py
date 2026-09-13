from ...infrastructure.database import DatabaseUnitOfWork


def apply_activity_reward(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS activity_reward_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO activity_reward_feature_migrations(version) VALUES ('activity_reward.001')")


__all__ = ["apply_activity_reward"]
