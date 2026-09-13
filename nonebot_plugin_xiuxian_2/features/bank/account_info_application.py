from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository


class BankAccountInfoApplication:
    def __init__(self, database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.repository = repository or BankAccountRepository()

    def get_info(self, *, user_id: str) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.database, immediate=False) as uow:
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            account = self.repository.existing_account(uow, user_id)
            if wallet is None or account is None:
                return {"status": "user_missing" if wallet is None else "account_missing", "user_id": user_id}
            return {
                "status": "ok",
                "user_id": user_id,
                "wallet_stone": int(wallet["stone"] or 0),
                "saved_stone": int(account["saved_stone"]),
                "bank_level": str(account["bank_level"]),
                "updated_at": str(account["updated_at"]),
            }


__all__ = ["BankAccountInfoApplication"]
