from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository
from .legacy_account_repository import BankLegacyAccountReadRepository


class BankAccountInfoApplication:
    def __init__(self, database: str | Path, *, player_database: str | Path | None = None, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.player_database = str(player_database or database)
        self.repository = repository or BankAccountRepository()
        self.legacy_account_reader = BankLegacyAccountReadRepository()

    def get_legacy_info(self, *, user_id: str, default_saved_at: str) -> dict[str, Any]:
        user_id = str(user_id)
        bank_data = None
        if Path(self.player_database).is_file():
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                bank_data = self.legacy_account_reader.read_fields(uow, user_id)
        if not bank_data:
            return {"savestone": 0, "savetime": str(default_saved_at), "banklevel": "1"}

        savestone = bank_data.get("savestone", 0)
        savetime = bank_data.get("savetime", str(default_saved_at))
        bank_level = str(bank_data.get("banklevel", "1"))
        try:
            savestone = int(savestone)
        except Exception:
            savestone = 0
        return {
            "savestone": savestone,
            "savetime": str(savetime),
            "banklevel": bank_level,
        }

    def get_info(self, *, user_id: str) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.database, immediate=False) as uow:
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            account = self.repository.existing_account(uow, user_id)
        if wallet is None:
            return {"status": "user_missing", "user_id": user_id}
        if account is None:
            from .account_bootstrap_application import BankAccountBootstrapApplication

            bootstrap = BankAccountBootstrapApplication(self.database, self.player_database, repository=self.repository)
            bootstrap_result = bootstrap.ensure_account(user_id=user_id, default_level="1")
            if bootstrap_result["status"] != "imported":
                return {"status": "account_missing", "user_id": user_id}
            with DatabaseUnitOfWork(self.database, immediate=False) as uow:
                wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
                account = self.repository.existing_account(uow, user_id)
        if wallet is None or account is None:
            return {"status": "account_missing", "user_id": user_id}
        return {
            "status": "ok",
            "user_id": user_id,
            "wallet_stone": int(wallet["stone"] or 0),
            "saved_stone": int(account["saved_stone"]),
            "bank_level": str(account["bank_level"]),
            "updated_at": str(account["updated_at"]),
        }


__all__ = ["BankAccountInfoApplication"]
