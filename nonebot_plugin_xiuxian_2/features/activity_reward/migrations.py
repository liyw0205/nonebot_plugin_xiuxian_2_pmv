from ...infrastructure.database import DatabaseUnitOfWork


def apply_activity_reward(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS activity_reward_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO activity_reward_feature_migrations(version) VALUES ('activity_reward.001')")


def apply_activity_claim_all(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_claim_all_operations("
        "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,"
        "result_json TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS activity_claim_all_steps("
        "operation_id TEXT NOT NULL,step_name TEXT NOT NULL,ordinal INTEGER NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,"
        "ok INTEGER,result_text TEXT NOT NULL DEFAULT '',error_text TEXT NOT NULL DEFAULT '',"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,step_name))"
    )


__all__ = ["apply_activity_claim_all", "apply_activity_reward"]
