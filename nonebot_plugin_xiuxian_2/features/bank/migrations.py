from ...infrastructure.database import DatabaseUnitOfWork


def apply_bank(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS bank_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO bank_feature_migrations(version) VALUES ('bank.001')")


def apply_bank_accounts(uow: DatabaseUnitOfWork) -> None:
    """Create the new game-db-owned first-use bank projection."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS bank_accounts ("
        "user_id TEXT PRIMARY KEY, saved_stone INTEGER NOT NULL, "
        "bank_level TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS bank_account_operations ("
        "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, payload TEXT NOT NULL, "
        "deposited INTEGER NOT NULL, interest INTEGER NOT NULL, wallet_after INTEGER NOT NULL, "
        "saved_after INTEGER NOT NULL, created_at TEXT NOT NULL)"
    )


__all__ = ["apply_bank", "apply_bank_accounts"]
