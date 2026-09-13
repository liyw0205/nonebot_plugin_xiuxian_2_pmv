from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository
from .interest_rules import decide_interest


class BankInterestApplication:
    def __init__(self, database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.repository = repository or BankAccountRepository()

    def settle_interest(self, *, operation_id: str, user_id: str, interest: int, bank_level: str, settled_at: str) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = json.dumps(["interest", user_id, int(interest), str(bank_level)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.repository.ensure_schema(uow)
            previous = self.repository.operation(uow, operation_id)
            if previous is not None:
                if previous["payload"] != payload:
                    return {"status": "operation_conflict", "operation_id": operation_id}
                return {"status": "duplicate", "operation_id": operation_id, "interest": previous["interest"], "wallet_stone": previous["wallet_after"], "saved_stone": previous["saved_after"]}
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            account = self.repository.account(uow, user_id)
            if wallet is None or account is None:
                return {"status": "user_missing", "operation_id": operation_id}
            if str(account["bank_level"]) != str(bank_level):
                return {"status": "state_changed", "operation_id": operation_id}
            decision = decide_interest(wallet=int(wallet["stone"] or 0), interest=int(interest), settled_at=str(settled_at))
            self.repository.save_interest(uow, operation_id=operation_id, user_id=user_id, payload=payload, interest=int(interest), decision=decision, bank_level=str(bank_level), saved_stone=int(account["saved_stone"]), settled_at=str(settled_at))
            return {"status": "applied", "operation_id": operation_id, "interest": int(interest), "wallet_stone": decision.wallet_after, "saved_stone": int(account["saved_stone"])}


__all__ = ["BankInterestApplication"]
