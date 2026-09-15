from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository
from .withdrawal_rules import decide_withdraw


class BankWithdrawalApplication:
    def __init__(self, database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.repository = repository or BankAccountRepository()

    def withdraw(self, *, operation_id: str, user_id: str, amount: int, interest: int, bank_level: str, settled_at: str) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = json.dumps(["withdraw", user_id, int(amount), int(interest), str(bank_level)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = self.repository.operation(uow, operation_id)
            if previous is not None:
                if previous["payload"] != payload:
                    return {"status": "operation_conflict", "operation_id": operation_id}
                return {"status": "duplicate", "operation_id": operation_id, "withdrawn": abs(int(previous["deposited"])), "interest": previous["interest"], "wallet_stone": previous["wallet_after"], "saved_stone": previous["saved_after"]}
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            account = self.repository.account(uow, user_id)
            if wallet is None or account is None:
                return {"status": "user_missing", "operation_id": operation_id}
            try:
                decision = decide_withdraw(wallet=int(wallet["stone"] or 0), saved=int(account["saved_stone"]), amount=int(amount), interest=int(interest))
            except ValueError as exc:
                if str(exc) == "saved_stone_insufficient":
                    return {"status": str(exc), "operation_id": operation_id}
                raise
            self.repository.save_withdrawal(uow, operation_id=operation_id, user_id=user_id, payload=payload, amount=int(amount), decision=decision, bank_level=str(bank_level), settled_at=str(settled_at))
            return {"status": "applied", "operation_id": operation_id, "withdrawn": int(amount), "interest": decision.interest, "wallet_stone": decision.wallet_after, "saved_stone": decision.saved_after}


__all__ = ["BankWithdrawalApplication"]
