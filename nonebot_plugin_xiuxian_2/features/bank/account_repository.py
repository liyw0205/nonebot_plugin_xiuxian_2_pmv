from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .rules import BankDepositDecision
from .withdrawal_rules import BankWithdrawalDecision


class BankAccountRepository:
    def ensure_schema(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS bank_accounts (user_id TEXT PRIMARY KEY, saved_stone INTEGER NOT NULL, bank_level TEXT NOT NULL, updated_at TEXT NOT NULL)")
        uow.execute("CREATE TABLE IF NOT EXISTS bank_account_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, payload TEXT NOT NULL, deposited INTEGER NOT NULL, interest INTEGER NOT NULL, wallet_after INTEGER NOT NULL, saved_after INTEGER NOT NULL, created_at TEXT NOT NULL)")

    def account(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any] | None:
        self.ensure_schema(uow)
        row = uow.query_one("SELECT user_id,saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id=?", (user_id,))
        return None if row is None else dict(row)

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> dict[str, Any] | None:
        self.ensure_schema(uow)
        row = uow.query_one("SELECT * FROM bank_account_operations WHERE operation_id=?", (operation_id,))
        return None if row is None else dict(row)

    def save_deposit(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, decision: BankDepositDecision, bank_level: str, settled_at: str) -> None:
        uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND stone>=?", (decision.wallet_after, user_id, amount))
        uow.execute("INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES (?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET saved_stone=excluded.saved_stone, bank_level=excluded.bank_level, updated_at=excluded.updated_at", (user_id, decision.saved_after, bank_level, settled_at))
        uow.execute("INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES (?,?,?,?,?,?,?,?)", (operation_id, user_id, payload, amount, decision.interest, decision.wallet_after, decision.saved_after, settled_at))

    def save_withdrawal(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, decision: BankWithdrawalDecision, bank_level: str, settled_at: str) -> None:
        uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=?", (decision.wallet_after, user_id))
        uow.execute("UPDATE bank_accounts SET saved_stone=?, bank_level=?, updated_at=? WHERE user_id=?", (decision.saved_after, bank_level, settled_at, user_id))
        uow.execute("INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES (?,?,?,?,?,?,?,?)", (operation_id, user_id, payload, -amount, decision.interest, decision.wallet_after, decision.saved_after, settled_at))


__all__ = ["BankAccountRepository"]
