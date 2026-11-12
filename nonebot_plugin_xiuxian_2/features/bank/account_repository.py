from __future__ import annotations

from ...core.errors import DomainError
from ...core.numeric import bind_sqlite_param, require_exact_integer

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
        if row is None:
            return None
        return {**row, "saved_stone": require_exact_integer(row["saved_stone"], field="saved_stone", subject=user_id)}

    def create_account(self, uow: DatabaseUnitOfWork, *, user_id: str, saved_stone: int, bank_level: str, updated_at: str) -> None:
        uow.execute(
            "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES(?,xiuxian_int_store(?,'saved_stone','actor'),?,?)",
            (str(user_id), bind_sqlite_param(int(saved_stone), exact=True), str(bank_level), str(updated_at)),
        )

    def operation(self, uow: DatabaseUnitOfWork, operation_id: str) -> dict[str, Any] | None:
        row = uow.query_one("SELECT * FROM bank_account_operations WHERE operation_id=?", (operation_id,))
        if row is None:
            return None
        result = dict(row)
        for field in ("deposited", "interest", "wallet_after", "saved_after"):
            result[field] = require_exact_integer(result[field], field=f"bank_receipt_{field}", subject=str(row["user_id"]))
        return result

    def _verify_balances(self, uow, user_id, wallet_after, saved_after=None):
        wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
        if wallet is None or require_exact_integer(wallet["stone"], field="stone", subject=user_id) != wallet_after:
            raise DomainError("stone_write_rejected", "bank wallet did not persist exactly", {"reason": "stone_write_rejected", "subject": user_id})
        if saved_after is not None:
            account = self.existing_account(uow, user_id)
            if account is None or account["saved_stone"] != saved_after:
                raise DomainError("bank_account_write_rejected", "bank balance did not persist exactly", {"reason": "bank_account_write_rejected", "subject": user_id})

    def _save_operation(self, uow, operation_id, user_id, payload, deposited, interest, wallet_after, saved_after, settled_at):
        changed = uow.execute(
            "INSERT INTO bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at) VALUES(?,?,?,?,xiuxian_int_store(?,'stone','actor'),?,?,?)",
            (operation_id, user_id, payload, *(bind_sqlite_param(int(value), exact=True) for value in (deposited, interest, wallet_after, saved_after)), settled_at),
        )
        if changed.rowcount != 1:
            raise DomainError("bank_receipt_write_rejected", "bank receipt write rejected", {"reason": "bank_receipt_write_rejected", "subject": user_id})

    def save_deposit(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, decision: BankDepositDecision, bank_level: str, settled_at: str) -> None:
        changed = uow.execute("UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')=0", (bind_sqlite_param(decision.wallet_after, exact=True), user_id, bind_sqlite_param(decision.wallet_after + amount - decision.interest, exact=True)))
        if changed.rowcount != 1:
            raise DomainError("stone_write_rejected", "wallet state changed during bank deposit")
        uow.execute("INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES(?,xiuxian_int_store(?,'saved_stone','actor'),?,?) ON CONFLICT(user_id) DO UPDATE SET saved_stone=excluded.saved_stone, bank_level=excluded.bank_level, updated_at=excluded.updated_at", (user_id, bind_sqlite_param(decision.saved_after, exact=True), bank_level, settled_at))
        self._verify_balances(uow, user_id, decision.wallet_after, decision.saved_after)
        self._save_operation(uow, operation_id, user_id, payload, amount, decision.interest, decision.wallet_after, decision.saved_after, settled_at)

    def save_withdrawal(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, decision: BankWithdrawalDecision, bank_level: str, settled_at: str) -> None:
        changed = uow.execute("UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')=0", (bind_sqlite_param(decision.wallet_after, exact=True), user_id, bind_sqlite_param(decision.wallet_after - amount - decision.interest, exact=True)))
        if changed.rowcount != 1:
            raise DomainError("stone_write_rejected", "wallet state changed during bank withdrawal")
        updated = uow.execute("UPDATE bank_accounts SET saved_stone=xiuxian_int_store(?,'saved_stone','actor'), bank_level=?, updated_at=? WHERE user_id=? AND xiuxian_int_cmp(saved_stone,?,1,'saved_stone','actor')=0", (bind_sqlite_param(decision.saved_after, exact=True), bank_level, settled_at, user_id, bind_sqlite_param(decision.saved_after + amount, exact=True)))
        if updated.rowcount != 1:
            raise DomainError("bank_account_write_rejected", "bank account state changed during bank withdrawal")
        self._verify_balances(uow, user_id, decision.wallet_after, decision.saved_after)
        self._save_operation(uow, operation_id, user_id, payload, -amount, decision.interest, decision.wallet_after, decision.saved_after, settled_at)

    def save_upgrade(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, amount: int, expected_level: str, decision: BankUpgradeDecision, settled_at: str) -> None:
        changed = uow.execute(
            "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')=0",
            (bind_sqlite_param(decision.wallet_after, exact=True), user_id, bind_sqlite_param(decision.wallet_after + amount, exact=True)),
        )
        if changed.rowcount != 1:
            raise DomainError("stone_write_rejected", "wallet state changed during bank upgrade")
        upgraded = uow.execute(
            "UPDATE bank_accounts SET bank_level=? WHERE user_id=? AND bank_level=?",
            (decision.bank_level, user_id, expected_level),
        )
        if upgraded.rowcount != 1:
            raise DomainError("bank_level_write_rejected", "bank account state changed during bank upgrade")
        self._verify_balances(uow, user_id, decision.wallet_after)
        self._save_operation(uow, operation_id, user_id, payload, 0, 0, decision.wallet_after, 0, settled_at)

    def save_interest(self, uow: DatabaseUnitOfWork, *, operation_id: str, user_id: str, payload: str, interest: int, decision: BankInterestDecision, bank_level: str, saved_stone: int, settled_at: str) -> None:
        changed = uow.execute(
            "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor') WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')=0",
            (bind_sqlite_param(decision.wallet_after, exact=True), user_id, bind_sqlite_param(decision.wallet_after - interest, exact=True)),
        )
        if changed.rowcount != 1:
            raise DomainError("stone_write_rejected", "wallet state changed during bank interest")
        updated = uow.execute(
            "UPDATE bank_accounts SET updated_at=?, bank_level=? WHERE user_id=?",
            (settled_at, bank_level, user_id),
        )
        if updated.rowcount != 1:
            raise DomainError("bank_account_write_rejected", "bank account state changed during bank interest")
        self._verify_balances(uow, user_id, decision.wallet_after, saved_stone)
        self._save_operation(uow, operation_id, user_id, payload, 0, interest, decision.wallet_after, saved_stone, settled_at)


__all__ = ["BankAccountRepository"]
