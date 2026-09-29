from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .rules import BankDepositDecision
from .withdrawal_rules import BankWithdrawalDecision
from .upgrade_rules import BankUpgradeDecision
from .interest_rules import BankInterestDecision


class BankAccountRepository:
    def assert_schema_ready(self, uow: DatabaseUnitOfWork) -> None:
        required_columns = {
            "bank_accounts": {"user_id", "saved_stone", "bank_level", "updated_at"},
            "bank_account_operations": {
                "operation_id", "user_id", "payload", "deposited", "interest",
                "wallet_after", "saved_after", "created_at",
            },
        }
        for table, required in required_columns.items():
            columns = {str(row["name"]) for row in uow.query_all(f"PRAGMA table_info({table})")}
            if not required.issubset(columns):
                raise RuntimeError(f"bank.002 schema_missing: {table}")

    def account(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any] | None:
        return self.existing_account(uow, user_id)

    def existing_account(self, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any] | None:
        row = uow.query_one("SELECT user_id,saved_stone,bank_level,updated_at FROM bank_accounts WHERE user_id=?", (user_id,))
        return None if row is None else dict(row)

    def create_account(self, uow: DatabaseUnitOfWork, *, user_id: str, saved_stone: int, bank_level: str, updated_at: str) -> None:
        uow.execute(
            "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES(?,?,?,?)",
            (str(user_id), int(saved_stone), str(bank_level), str(updated_at)),
        )

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> dict[str, Any] | None:
        row = uow.query_one("SELECT * FROM bank_account_operations WHERE operation_id=?", (operation_id,))
        return None if row is None else dict(row)

    def save_deposit(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, decision: BankDepositDecision, bank_level: str, settled_at: str) -> None:
        changed = uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND stone>=?", (decision.wallet_after, user_id, amount))
        if changed.rowcount != 1:
            raise RuntimeError("wallet state changed during bank deposit")
        uow.execute("INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES (?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET saved_stone=excluded.saved_stone, bank_level=excluded.bank_level, updated_at=excluded.updated_at", (user_id, decision.saved_after, bank_level, settled_at))
        uow.execute("INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES (?,?,?,?,?,?,?,?)", (operation_id, user_id, payload, amount, decision.interest, decision.wallet_after, decision.saved_after, settled_at))

    def save_withdrawal(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, decision: BankWithdrawalDecision, bank_level: str, settled_at: str) -> None:
        changed = uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=?", (decision.wallet_after, user_id))
        if changed.rowcount != 1:
            raise RuntimeError("wallet state changed during bank withdrawal")
        updated = uow.execute("UPDATE bank_accounts SET saved_stone=?, bank_level=?, updated_at=? WHERE user_id=?", (decision.saved_after, bank_level, settled_at, user_id))
        if updated.rowcount != 1:
            raise RuntimeError("bank account state changed during bank withdrawal")
        uow.execute("INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES (?,?,?,?,?,?,?,?)", (operation_id, user_id, payload, -amount, decision.interest, decision.wallet_after, decision.saved_after, settled_at))

    def save_upgrade(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, expected_level: str, decision: BankUpgradeDecision, settled_at: str) -> None:
        changed = uow.execute(
            "UPDATE user_xiuxian SET stone=? WHERE user_id=? AND stone>=?",
            (decision.wallet_after, user_id, amount),
        )
        if changed.rowcount != 1:
            raise RuntimeError("wallet state changed during bank upgrade")
        upgraded = uow.execute(
            "UPDATE bank_accounts SET bank_level=? WHERE user_id=? AND bank_level=?",
            (decision.bank_level, user_id, expected_level),
        )
        if upgraded.rowcount != 1:
            raise RuntimeError("bank account state changed during bank upgrade")
        uow.execute(
            "INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (operation_id, user_id, payload, 0, 0, decision.wallet_after, 0, settled_at),
        )

    def save_interest(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, interest: int, decision: BankInterestDecision, bank_level: str, saved_stone: int, settled_at: str) -> None:
        changed = uow.execute(
            "UPDATE user_xiuxian SET stone=? WHERE user_id=?",
            (decision.wallet_after, user_id),
        )
        if changed.rowcount != 1:
            raise RuntimeError("wallet state changed during bank interest")
        updated = uow.execute(
            "UPDATE bank_accounts SET updated_at=?, bank_level=? WHERE user_id=?",
            (settled_at, bank_level, user_id),
        )
        if updated.rowcount != 1:
            raise RuntimeError("bank account state changed during bank interest")
        uow.execute(
            "INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (operation_id, user_id, payload, 0, interest, decision.wallet_after, saved_stone, settled_at),
        )


__all__ = ["BankAccountRepository"]
