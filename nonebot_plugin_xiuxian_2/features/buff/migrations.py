from ...infrastructure.database import DatabaseUnitOfWork


def apply_buff(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS buff_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO buff_feature_migrations(version) VALUES ('buff.001')")


def apply_partner_token_operations(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS partner_token_operations("
        "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,used_tokens INTEGER NOT NULL,"
        "used_count INTEGER NOT NULL,item_remaining INTEGER NOT NULL,"
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


def apply_partner_token_usage(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS partner_two_exp_usage("
        "user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL DEFAULT 0,"
        "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )


__all__ = ["apply_buff", "apply_partner_token_operations", "apply_partner_token_usage"]
