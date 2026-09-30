from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository


class BankAccountImportApplication:
    """Import an explicit legacy snapshot without replacing game-owned state."""

    def __init__(self, database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.repository = repository or BankAccountRepository()

    def import_if_missing(
        self, *, user_id: str, saved_stone: int, bank_level: str, updated_at: str
    ) -> str:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        saved_stone = int(saved_stone)
        bank_level = str(bank_level).strip()
        if saved_stone < 0 or not bank_level:
            raise ValueError("legacy bank account values are invalid")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.repository.assert_schema_ready(uow)
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return "user_missing"
            if self.repository.existing_account(uow, user_id) is not None:
                return "existing"
            self.repository.create_account(
                uow,
                user_id=user_id,
                saved_stone=saved_stone,
                bank_level=bank_level,
                updated_at=str(updated_at),
            )
            return "imported"


__all__ = ["BankAccountImportApplication"]
