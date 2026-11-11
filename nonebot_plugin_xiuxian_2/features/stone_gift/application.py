from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, OperationConflictError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...core.player_reasons import reason_text
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.database.ledger import request_hash
from ...infrastructure.observability import trace_context
from ...infrastructure.clock import SystemClock
from .domain import StoneGiftRecord, calculate_amounts, daily_cap, normalize_transfer_date, validate_daily_limit
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

    def read_limits(self, sender: dict[str, Any], recipient: dict[str, Any]) -> dict[str, Any]:
        transfer_date = self._today()
        sender_id = str(sender["user_id"])
        recipient_id = str(recipient["user_id"])
        with DatabaseUnitOfWork(self.database) as uow:
            usage = self.repository.limits(uow, transfer_date, (sender_id, recipient_id))
        return {
            "transfer_date": transfer_date,
            "send_limit": daily_cap(str(sender.get("level", ""))),
            "receive_limit": daily_cap(str(recipient.get("level", ""))),
            "send_used": usage[sender_id]["sent"],
            "receive_used": usage[recipient_id]["received"],
        }

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
            identity = {key: payload[key] for key in ("sender_id", "recipient_id", "gross_amount")}
            try:
                # A transfer checks and increments daily limits alongside two
                # balances; take the SQLite write lock before reading them.
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.get(uow, request.operation_id, self.action)
                    legacy = self.repository.operation(uow, request.operation_id)
                    if legacy is not None:
                        if existing is not None:
                            # The ledger keeps the exact request identity. Do
                            # not compare a historical REAL projection to a
                            # Python integer: SQLite may round that projection
                            # beyond int64 while the request hash remains exact.
                            valid_hashes = {request_hash(identity), request_hash(payload)}
                            if existing.request_hash not in valid_hashes:
                                # A pre-ledger deployment could leave a
                                # committed legacy receipt beside a started
                                # row whose hash included policy snapshots.
                                # Recover that interrupted request from its
                                # exact small-value legacy identity; an
                                # applied row with a different hash remains a
                                # strict operation conflict.
                                legacy_identity = (
                                    legacy.sender_id,
                                    legacy.recipient_id,
                                    legacy.gross_amount,
                                )
                                request_identity = (
                                    request.sender_id,
                                    request.recipient_id,
                                    request.gross_amount,
                                )
                                if existing.status != "started" or legacy_identity != request_identity:
                                    raise OperationConflictError(request.operation_id, self.action)
                            previous = existing.outcome()
                            if previous is not None:
                                return previous.replay()
                        elif (legacy.sender_id, legacy.recipient_id, legacy.gross_amount) != (
                            request.sender_id, request.recipient_id, request.gross_amount
                        ):
                            raise OperationConflictError(request.operation_id, self.action)
                    # Historical refusal hashes included policy snapshots. Keep
                    # those hashes valid; new requests use only player inputs.
                    if existing is None or existing.request_hash != request_hash(payload):
                        payload = identity
                    if legacy is None or existing is None:
                        existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None and legacy is None:
                            return previous.replay()
                        if legacy is None:
                            raise ConflictError("操作正在处理中", reason="operation_pending")
                    before = self.repository.snapshot(uow, (request.sender_id, request.recipient_id))
                    if legacy is not None:
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
                        return outcome.replay()
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
                            "state_changed": ("接收方灵石写入未通过校验，请联系管理员核对账户记录。", "state_changed"),
                            "send_limit_reached": ("今日赠送额度不足", "send_limit_reached"),
                            "receive_limit_reached": ("对方今日可接收额度不足", "receive_limit_reached"),
                            "schema_missing": ("赠送数据尚未就绪，请检查启动迁移。", "schema_missing"),
                        }
                        message, code = messages.get(status, ("转账失败", "rejected"))
                        reason = getattr(status, "reason", "") or str(status)
                        subject = getattr(status, "subject", "") or {
                            "recipient_missing": "recipient", "receive_limit_reached": "recipient",
                            "send_limit_reached": "sender", "stone_insufficient": "sender",
                        }.get(str(status), "")
                        diagnostic = reason_text(reason)
                        if diagnostic:
                            label = {"sender": "赠送方", "recipient": "接收方"}.get(subject, "")
                            message = (label + "，" if label else "") + diagnostic
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            message,
                            code=code,
                            data={"status": str(status), "reason": reason, "subject": subject},
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
