from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository
from .upgrade_rules import decide_upgrade


class BankUpgradeApplication:
    def __init__(self, database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.repository = repository or BankAccountRepository()

    def upgrade(self, *, operation_id: str, user_id: str, expected_level: str, next_level: str, cost: int, settled_at: str) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        payload = json.dumps(["upgrade", user_id, str(next_level), int(cost)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self.repository.ensure_schema(uow)
            previous = self.repository.operation(uow, operation_id)
            if previous is not None:
                if previous["payload"] != payload:
                    return {"status": "operation_conflict", "operation_id": operation_id}
                return {"status": "duplicate", "operation_id": operation_id, "cost": abs(int(previous["deposited"])), "wallet_stone": previous["wallet_after"], "bank_level": str(next_level)}
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            account = self.repository.account(uow, user_id)
            if wallet is None or account is None:
                return {"status": "user_missing", "operation_id": operation_id}
            try:
                decision = decide_upgrade(wallet=int(wallet["stone"] or 0), current_level=str(account["bank_level"]), expected_level=str(expected_level), next_level=str(next_level), cost=int(cost))
            except ValueError as exc:
                if str(exc) == "stone_insufficient":
                    return {"status": str(exc), "operation_id": operation_id}
                raise
            self.repository.save_upgrade(uow, operation_id=operation_id, user_id=user_id, payload=payload, amount=int(cost), expected_level=str(expected_level), decision=decision, settled_at=str(settled_at))
            return {"status": "applied", "operation_id": operation_id, "cost": int(cost), "wallet_stone": decision.wallet_after, "bank_level": decision.bank_level}


__all__ = ["BankUpgradeApplication"]
