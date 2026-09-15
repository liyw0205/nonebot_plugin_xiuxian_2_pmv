from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository
from .rules import decide_deposit


class BankDepositApplication:
    """First-use bank deposit path backed by new game-db-owned tables."""

    def __init__(self, database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.repository = repository or BankAccountRepository()

    def deposit(self, *, operation_id: str, user_id: str, amount: int, interest: int, limit: int, bank_level: str, settled_at: str) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = json.dumps([user_id, int(amount), int(interest), int(limit), str(bank_level)], separators=(",", ":"), sort_keys=False)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = self.repository.operation(uow, operation_id)
            if previous is not None:
                if previous["payload"] != payload:
                    return {"status": "operation_conflict", "operation_id": operation_id}
                return {"status": "duplicate", "operation_id": operation_id, "deposited": previous["deposited"], "interest": previous["interest"], "wallet_stone": previous["wallet_after"], "saved_stone": previous["saved_after"]}
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if wallet is None:
                return {"status": "user_missing", "operation_id": operation_id}
            account = self.repository.account(uow, user_id)
            saved = 0 if account is None else int(account["saved_stone"])
            try:
                decision = decide_deposit(wallet=int(wallet["stone"] or 0), saved=saved, amount=int(amount), interest=int(interest), limit=int(limit))
            except ValueError as exc:
                if str(exc) in {"stone_insufficient", "limit_exceeded"}:
                    return {"status": str(exc), "operation_id": operation_id}
                raise
            self.repository.save_deposit(uow, operation_id=operation_id, user_id=user_id, payload=payload, amount=int(amount), decision=decision, bank_level=str(bank_level), settled_at=str(settled_at))
            return {"status": "applied", "operation_id": operation_id, "deposited": int(amount), "interest": decision.interest, "wallet_stone": decision.wallet_after, "saved_stone": decision.saved_after}


__all__ = ["BankDepositApplication"]
