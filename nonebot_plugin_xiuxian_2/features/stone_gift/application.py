from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from ...infrastructure.clock import SystemClock
from .domain import StoneGiftRecord, calculate_amounts, normalize_transfer_date, validate_daily_limit
from .repository import StoneGiftRepository
from .schemas import StoneGiftRequest


class StoneGiftApplication:
    action = "stone_gift.transfer"

    def __init__(
        self,
        database: str | Path,
        *,
        repository: StoneGiftRepository | None = None,
        ledger: OperationLedger | None = None,
        fee_rate: float = 0.1,
        clock: Any | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or StoneGiftRepository()
        self.ledger = ledger or OperationLedger()
        self.fee_rate = float(fee_rate)
        self.clock = clock or SystemClock()
        calculate_amounts(1, self.fee_rate)

    def _today(self) -> str:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if hasattr(value, "date"):
            return value.date().isoformat()
        raise TypeError("clock.now() must return a datetime")

    def lookup(self, operation_id: str, *, sender_id: str | None = None, recipient_id: str | None = None) -> StoneGiftRecord | None:
        with DatabaseUnitOfWork(self.database) as uow:
            record = self.repository.operation(uow, str(operation_id).strip())
        if record is None:
            return None
        if sender_id is not None and record.sender_id != str(sender_id):
            return None
        if recipient_id is not None and record.recipient_id != str(recipient_id):
            return None
        return record

    def resolve_user(self, identifier: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.database) as uow:
                return self.repository.find_user(uow, identifier)
        except Exception as exc:
            if exc.__class__.__name__ != "OperationalError":
                raise
            # A fresh installation may receive a command before the legacy
            # player schema has been imported; treat it as an unknown target.
            return None

    def transfer(
        self,
        *,
        operation_id: str,
        sender_id: str,
        recipient_id: str,
        gross_amount: int,
        fee_rate: float | None = None,
        transfer_date: str | None = None,
        send_limit: int | None = None,
        receive_limit: int | None = None,
        send_used: int | None = None,
        receive_used: int | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=sender_id):
            rate = self.fee_rate if fee_rate is None else float(fee_rate)
            request = StoneGiftRequest(str(operation_id), str(sender_id), str(recipient_id), int(gross_amount), rate)
            try:
                request.validate()
                net_amount, fee_amount = calculate_amounts(request.gross_amount, request.fee_rate)
                normalized_date = normalize_transfer_date(transfer_date or self._today())
                normalized_send_limit = validate_daily_limit(send_limit, "send_limit")
                normalized_receive_limit = validate_daily_limit(receive_limit, "receive_limit")
                normalized_send_used = validate_daily_limit(send_used, "send_used")
                normalized_receive_used = validate_daily_limit(receive_used, "receive_used")
            except (TypeError, ValueError) as exc:
                raise ValidationError(str(exc)) from exc
            payload = {
                "sender_id": request.sender_id,
                "recipient_id": request.recipient_id,
                "gross_amount": request.gross_amount,
                "fee_rate": request.fee_rate,
                "transfer_date": normalized_date,
                "send_limit": normalized_send_limit,
                "receive_limit": normalized_receive_limit,
                "send_used": normalized_send_used,
                "receive_used": normalized_receive_used,
            }
            try:
                # A transfer checks and increments daily limits alongside two
                # balances; take the SQLite write lock before reading them.
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    before = self.repository.snapshot(uow, (request.sender_id, request.recipient_id))
                    legacy = self.repository.operation(uow, request.operation_id)
                    if legacy is not None:
                        status = "transferred" if (legacy.sender_id == request.sender_id and legacy.recipient_id == request.recipient_id) else "state_changed"
                        if status == "state_changed":
                            outcome = OperationOutcome.rejected(
                                request.operation_id,
                                self.action,
                                "操作参与方已变化",
                                code="state_changed",
                                before=before,
                                after=before,
                                audit_category="stone_gift",
                            )
                        else:
                            outcome = OperationOutcome.applied(
                                request.operation_id,
                                self.action,
                                data={"stone_gift": legacy.to_dict()},
                                before=before,
                                after=before,
                                granted={"stone": legacy.net_amount},
                                audit_category="stone_gift",
                            )
                        self.ledger.finish(uow, outcome)
                        return outcome.replay() if outcome.ok else outcome
                    record = StoneGiftRecord(
                        request.operation_id,
                        request.sender_id,
                        request.recipient_id,
                        request.gross_amount,
                        net_amount,
                        fee_amount,
                    )
                    status = self.repository.transfer(
                        uow,
                        record,
                        transfer_date=normalized_date,
                        send_limit=normalized_send_limit,
                        receive_limit=normalized_receive_limit,
                        send_used=normalized_send_used,
                        receive_used=normalized_receive_used,
                    )
                    if status != "transferred":
                        messages = {
                            "recipient_missing": ("对方未踏入修仙界，不可赠送！", "recipient_missing"),
                            "stone_insufficient": ("灵石不足", "stone_insufficient"),
                            "state_changed": ("转账未结算：双方灵石刚被其他操作改动，请稍后重试。", "state_changed"),
                            "send_limit_reached": ("今日赠送额度不足", "send_limit_reached"),
                            "receive_limit_reached": ("对方今日可接收额度不足", "receive_limit_reached"),
                        }
                        message, code = messages.get(status, ("转账失败", "rejected"))
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            message,
                            code=code,
                            before=before,
                            after=before,
                            audit_category="stone_gift",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    after = self.repository.snapshot(uow, (request.sender_id, request.recipient_id))
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data={"stone_gift": record.to_dict()},
                        before=before,
                        after=after,
                        consumed={"sender_stone": request.gross_amount, "fee": fee_amount},
                        granted={"recipient_stone": net_amount},
                        audit_category="stone_gift",
                    )
                    self.ledger.finish(uow, outcome)
                    return outcome
            except DomainError:
                raise
            except Exception as exc:
                try:
                    self.ledger.record_failure(self.database, request.operation_id, self.action, payload, str(exc))
                except Exception:
                    pass
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        result = self.transfer(**kwargs)
        if result.message:
            content = result.message
        else:
            gift = (result.data or {}).get("stone_gift", {}) if isinstance(result.data, dict) else {}
            content = (
                f"共赠送{gift.get('gross_amount', 0)}枚灵石，"
                f"收取手续费{gift.get('fee_amount', 0)}枚"
            )
        return ReplyPlan(content, reference=True)


__all__ = ["StoneGiftApplication"]
