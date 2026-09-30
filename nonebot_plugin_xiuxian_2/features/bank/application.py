from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import BankDepositRequest, BankInterestRequest, BankUpgradeRequest, BankWithdrawalRequest
from .repository import BankRepository
from .account_repository import BankAccountRepository
from .account_application import BankDepositApplication
from .account_withdrawal_application import BankWithdrawalApplication
from .account_upgrade_application import BankUpgradeApplication
from .account_interest_application import BankInterestApplication


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class BankApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path | None = None, *, repository: BankRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.game_database = str(game_database)
        # Keep the old constructor argument for integrations; default requests never read player_db.
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.account_repository = BankAccountRepository()

    def _prepare_account(self, user_id: str) -> str:
        if self.repository is not None:
            return "existing"
        with DatabaseUnitOfWork(self.game_database, immediate=False) as uow:
            self.account_repository.assert_schema_ready(uow)
            wallet = uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,))
            if wallet is None:
                return "user_missing"
            if self.account_repository.existing_account(uow, user_id) is not None:
                return "existing"
        return "account_missing"

    @staticmethod
    def _setup_failure(status: str, operation_id: str) -> dict[str, str]:
        return {"status": "user_missing" if status == "user_missing" else "account_invalid", "operation_id": operation_id}

    def _execute(self, *, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any], call, messages: Mapping[str, str]) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        # Feature receipts make a retry safe if the process stopped
                        # after the account transaction but before ledger.finish().
                data = _data(call())
                status = str(data.get("status", "failed"))
                normalized = {"status": status, **{key: data.get(key) for key in ("deposited", "withdrawn", "interest", "wallet_stone", "saved_stone", "saved_at", "cost", "bank_level")}}
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(operation_id, action, data=normalized, granted={key: normalized[key] for key in ("interest", "deposited", "withdrawn") if normalized.get(key) is not None}, audit_category="bank")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, messages.get(status, "灵庄操作未完成。"), code=status, data=normalized, audit_category="bank")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def deposit(self, *, operation_id: str, user_id: str, amount: int, expected_saved_stone: int, expected_saved_at: str, bank_level: str, interest: int, settled_at: str, save_limit: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = BankDepositRequest(str(operation_id).strip(), str(user_id).strip(), int(amount), int(expected_saved_stone), str(expected_saved_at), str(bank_level), int(interest), str(settled_at), int(save_limit))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        if self.repository is None:
            def call():
                setup = self._prepare_account(request.user_id)
                if setup not in {"existing", "account_missing"}:
                    return self._setup_failure(setup, request.operation_id)
                initial_account = (
                    {"saved_stone": 0, "updated_at": request.expected_saved_at, "bank_level": "1"}
                    if setup == "account_missing"
                    else None
                )
                return BankDepositApplication(self.game_database).deposit(
                    operation_id=request.operation_id, user_id=request.user_id,
                    amount=request.amount, interest=request.interest, limit=request.save_limit,
                    bank_level=request.bank_level, settled_at=request.settled_at,
                    expected_saved_stone=request.expected_saved_stone,
                    expected_saved_at=request.expected_saved_at,
                    initial_account=initial_account,
                )
        else:
            repository = self.repository
            call = lambda: repository.deposit(request.operation_id, request.user_id, request.amount, request.expected_saved_stone, request.expected_saved_at, request.bank_level, request.interest, request.settled_at, request.save_limit)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="bank.deposit", payload=request.payload(), call=call, messages={"stone_insufficient": "灵石不足，存款未结算。", "limit_exceeded": "超过灵庄存储上限，存款未结算。", "state_changed": "灵庄操作失败：账户当前状态已更新。", "account_invalid": "灵庄历史账户数据无效，操作未处理，请联系管理员核查。", "user_missing": "未找到修仙数据。"})

    def withdraw(self, *, operation_id: str, user_id: str, amount: int, expected_saved_stone: int, expected_saved_at: str, bank_level: str, interest: int, settled_at: str) -> OperationOutcome[dict[str, Any]]:
        try:
            request = BankWithdrawalRequest(str(operation_id).strip(), str(user_id).strip(), int(amount), int(expected_saved_stone), str(expected_saved_at), str(bank_level), int(interest), str(settled_at))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        if self.repository is None:
            def call():
                setup = self._prepare_account(request.user_id)
                if setup not in {"existing", "account_missing"}:
                    return self._setup_failure(setup, request.operation_id)
                return BankWithdrawalApplication(self.game_database).withdraw(
                    operation_id=request.operation_id, user_id=request.user_id,
                    amount=request.amount, interest=request.interest,
                    bank_level=request.bank_level, settled_at=request.settled_at,
                    expected_saved_stone=request.expected_saved_stone,
                    expected_saved_at=request.expected_saved_at,
                )
        else:
            repository = self.repository
            call = lambda: repository.withdraw(request.operation_id, request.user_id, request.amount, request.expected_saved_stone, request.expected_saved_at, request.bank_level, request.interest, request.settled_at)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="bank.withdraw", payload=request.payload(), call=call, messages={"saved_stone_insufficient": "灵庄存款不足，取款未结算。", "state_changed": "灵庄操作失败：账户当前状态已更新。", "account_invalid": "灵庄历史账户数据无效，操作未处理，请联系管理员核查。", "user_missing": "未找到修仙数据。"})

    def upgrade(self, *, operation_id: str, user_id: str, expected_level: str, next_level: str, cost: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = BankUpgradeRequest(str(operation_id).strip(), str(user_id).strip(), str(expected_level), str(next_level), int(cost))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        if self.repository is None:
            def call():
                setup = self._prepare_account(request.user_id)
                if setup not in {"existing", "account_missing"}:
                    return self._setup_failure(setup, request.operation_id)
                initial_account = (
                    {"saved_stone": 0, "bank_level": "1", "updated_at": ""}
                    if setup == "account_missing"
                    else None
                )
                return BankUpgradeApplication(self.game_database).upgrade(
                    operation_id=request.operation_id, user_id=request.user_id,
                    expected_level=request.expected_level, next_level=request.next_level,
                    cost=request.cost, settled_at="", initial_account=initial_account,
                )
        else:
            repository = self.repository
            call = lambda: repository.upgrade(request.operation_id, request.user_id, request.expected_level, request.next_level, request.cost)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="bank.upgrade", payload=request.payload(), call=call, messages={"stone_insufficient": "灵石不足，会员升级未结算。", "state_changed": "灵庄会员升级失败：账户当前状态已更新。", "account_invalid": "灵庄历史账户数据无效，操作未处理，请联系管理员核查。", "user_missing": "未找到修仙数据。"})

    def settle_interest(self, *, operation_id: str, user_id: str, expected_saved_stone: int, expected_saved_at: str, bank_level: str, interest: int, settled_at: str) -> OperationOutcome[dict[str, Any]]:
        try:
            request = BankInterestRequest(str(operation_id).strip(), str(user_id).strip(), int(expected_saved_stone), str(expected_saved_at), str(bank_level), int(interest), str(settled_at))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        if self.repository is None:
            def call():
                setup = self._prepare_account(request.user_id)
                if setup not in {"existing", "account_missing"}:
                    return self._setup_failure(setup, request.operation_id)
                initial_account = (
                    {
                        "saved_stone": 0,
                        "bank_level": "1",
                        "updated_at": request.expected_saved_at,
                    }
                    if setup == "account_missing"
                    else None
                )
                return BankInterestApplication(self.game_database).settle_interest(
                    operation_id=request.operation_id, user_id=request.user_id,
                    interest=request.interest, bank_level=request.bank_level,
                    settled_at=request.settled_at,
                    expected_saved_stone=request.expected_saved_stone,
                    expected_saved_at=request.expected_saved_at,
                    initial_account=initial_account,
                )
        else:
            repository = self.repository
            call = lambda: repository.settle_interest(request.operation_id, request.user_id, request.expected_saved_stone, request.expected_saved_at, request.bank_level, request.interest, request.settled_at)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="bank.interest", payload=request.payload(), call=call, messages={"state_changed": "灵庄结息失败：账户当前状态已更新。", "account_invalid": "灵庄历史账户数据无效，操作未处理，请联系管理员核查。", "user_missing": "未找到修仙数据。"})

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "settle_interest"))
        return ReplyPlan(getattr(self, action)(**kwargs).data, reference=True)


__all__ = ["BankApplication"]
