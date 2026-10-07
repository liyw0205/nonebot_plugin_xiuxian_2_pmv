from __future__ import annotations

from pathlib import Path

from .account_application import BankDepositApplication
from .account_info_application import BankAccountInfoApplication
from .account_interest_application import BankInterestApplication
from .account_upgrade_application import BankUpgradeApplication
from .account_withdrawal_application import BankWithdrawalApplication
from .command_receipt_repository import BankCommandReceiptRepository
from .clock import bank_clock
from .interest_rules import calculate_interest


class BankCommandApplication:
    _ACTIONS = {
        "存灵石": "deposit", "取灵石": "withdrawal", "升级会员": "upgrade",
        "信息": "info", "结算": "interest",
    }

    def __init__(self, database: str | Path, bank_levels, *, clock=None) -> None:
        self.database = Path(database)
        self.bank_levels = bank_levels
        self.clock = clock
        self.receipts = BankCommandReceiptRepository(database)
        self.info = BankAccountInfoApplication(database)
        self.deposits = BankDepositApplication(database)
        self.withdrawals = BankWithdrawalApplication(database)
        self.upgrades = BankUpgradeApplication(database)
        self.interest = BankInterestApplication(database)

    def execute(self, *, operation_id, user_id, mode, argument) -> dict:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        mode = str(mode).strip() if mode is not None else None
        action = "help" if not mode else self._ACTIONS.get(mode, "invalid")
        invalid = {"action": action, "status": "invalid_argument"}
        if not operation_id or not user_id or len(operation_id) > 255 or len(user_id) > 255:
            return invalid
        raw = "" if argument is None else str(argument)
        if len(raw) > 128:
            return invalid
        raw = raw.strip()
        if action == "help":
            return {"action": action, "status": "help"} if raw in {"", "帮助"} else invalid
        amount = None
        if action in {"deposit", "withdrawal"}:
            if not raw or len(raw) > 18 or not raw.isascii() or not raw.isdecimal():
                return invalid
            amount = int(raw)
            if amount <= 0:
                return invalid
        elif raw or action == "invalid":
            return invalid
        if action != "info":
            previous = self.receipts.find(operation_id=operation_id, user_id=user_id, action=action, amount=amount)
            if previous is not None:
                return previous
        try:
            account = self.info.get_info(user_id=user_id)
        except RuntimeError as exc:
            if "schema_missing" not in str(exc):
                raise
            return {"action": action, "status": "schema_missing"}
        if account["status"] not in {"ok", "account_missing"}:
            return {"action": action, **account}
        if action == "info":
            return {"action": action, **account}

        now = (self.clock or bank_clock()).now()
        settled_at = now.isoformat()
        missing = account["status"] == "account_missing"
        level = str(account["bank_level"])
        try:
            configuration = self.bank_levels[level]
            saved = int(account["saved_stone"])
            if saved < 0:
                raise ValueError("negative bank balance")
            saved_at = str(account["updated_at"]) if not missing else settled_at
            initial = {"saved_stone": saved, "bank_level": level, "updated_at": saved_at} if missing else None
            if action == "upgrade":
                if not level.isascii() or not level.isdecimal() or len(level) > 4:
                    raise ValueError("invalid bank level")
                next_level = str(int(level) + 1)
                if next_level not in self.bank_levels:
                    return {"action": action, "status": "max_level"}
                cost = int(configuration["levelup"])
                if cost < 0:
                    raise ValueError("invalid upgrade cost")
                result = self.upgrades.upgrade(
                    operation_id=operation_id, user_id=user_id, expected_level=level,
                    next_level=next_level, cost=cost, settled_at=settled_at, initial_account=initial,
                )
                return {"action": action, **result}
            accrued, hours = calculate_interest(
                saved_stone=saved, saved_at=saved_at, settled_at=now, rate=float(configuration["interest"]),
            )
            limit = int(configuration["savemax"])
            if limit < 0:
                raise ValueError("invalid bank limit")
        except (KeyError, TypeError, ValueError, OverflowError):
            return {"action": action, "status": "account_invalid"}
        snapshot = {
            "operation_id": operation_id, "user_id": user_id, "interest": accrued,
            "bank_level": level, "settled_at": settled_at,
            "expected_saved_stone": saved, "expected_saved_at": saved_at,
        }
        if action == "deposit":
            result = self.deposits.deposit(**snapshot, amount=amount, limit=limit, initial_account=initial)
        elif action == "withdrawal":
            if missing:
                return {"action": action, "status": "saved_stone_insufficient"}
            result = self.withdrawals.withdraw(**snapshot, amount=amount)
        else:
            result = self.interest.settle_interest(**snapshot, initial_account=initial)
        return {"action": action, "interest_hours": hours, **result}


__all__ = ["BankCommandApplication"]
