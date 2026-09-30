from ...infrastructure.database import DatabaseUnitOfWork


def apply_compensation(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS compensation_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO compensation_feature_migrations(version) VALUES ('legacy.compensation.001')")


def apply_compensation_reward_claim_schema(uow: DatabaseUnitOfWork) -> None:
    """Prepare the claim ledger before any reward request can run."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS reward_claims("
        "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,user_id TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(reward_type,record_id,user_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS reward_claim_counters("
        "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,"
        "baseline_count INTEGER NOT NULL DEFAULT 0,"
        "PRIMARY KEY(reward_type,record_id))"
    )


__all__ = ["apply_compensation", "apply_compensation_reward_claim_schema"]
