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


def apply_compensation_invitation_reward_schema(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_invites("
        "inviter_id TEXT NOT NULL,invited_id TEXT NOT NULL,source TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(inviter_id,invited_id))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_claims("
        "user_id TEXT NOT NULL,threshold INTEGER NOT NULL,source TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "PRIMARY KEY(user_id,threshold))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
        "thresholds_json TEXT NOT NULL,invitation_count INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_compensation_invitation_definition_schema(uow: DatabaseUnitOfWork) -> None:
    """Prepare the invitation reward catalog before admin writes can run."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS invitation_reward_definitions("
        "threshold INTEGER PRIMARY KEY,rewards_json TEXT NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = [
    "apply_compensation",
    "apply_compensation_reward_claim_schema",
    "apply_compensation_invitation_reward_schema",
    "apply_compensation_invitation_definition_schema",
]
