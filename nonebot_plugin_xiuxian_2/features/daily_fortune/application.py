from __future__ import annotations

from datetime import date as date_type, datetime, timezone
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from .domain import draw_fortune
from .repository import DailyFortuneRepository
from .schemas import DailyFortune, DailyFortuneRequest


class DailyFortuneApplication:
    action = "daily_fortune.claim"

    def __init__(
        self,
        database: str,
        *,
        repository: DailyFortuneRepository | None = None,
        ledger: OperationLedger | None = None,
        clock: Any | None = None,
        random_source: Any | None = None,
    ) -> None:
        self.database = database
        self.repository = repository or DailyFortuneRepository()
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()
        self.random = random_source or SystemRandom()

    def _date(self, explicit: str | None) -> str:
        if explicit:
            try:
                return datetime.strptime(explicit, "%Y-%m-%d").date().isoformat()
            except ValueError as exc:
                raise ValidationError("日期格式必须为 YYYY-MM-DD") from exc
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if isinstance(value, date_type) and not isinstance(value, datetime):
            return value.isoformat()
        if hasattr(value, "date"):
            return value.date().isoformat()
        raise TypeError("clock.now() must return a date or datetime")

    def _now(self) -> datetime:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if isinstance(value, datetime):
            return value
        if isinstance(value, date_type):
            return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
        raise TypeError("clock.now() must return a date or datetime")

    def claim(self, *, user_id: str, operation_id: str, date: str | None = None) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            return self._claim(user_id=user_id, operation_id=operation_id, date=date)

    def _claim(self, *, user_id: str, operation_id: str, date: str | None = None) -> OperationOutcome[dict[str, Any]]:
        request = DailyFortuneRequest(user_id, operation_id, date)
        try:
            request.validate()
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        fortune_date = self._date(request.date)
        payload = {"user_id": request.user_id, "date": fortune_date}
        try:
            with DatabaseUnitOfWork(self.database) as uow:
                existing_operation = self.ledger.begin(uow, request.operation_id, self.action, payload)
                if existing_operation is not None:
                    previous = existing_operation.outcome()
                    if previous is not None:
                        return previous.replay()
                    raise ConflictError("操作正在处理中")
                existing = self.repository.get(uow, request.user_id, fortune_date)
                if existing is not None:
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        "今日运势已经领取",
                        code="already_claimed",
                        data={"fortune": dict(existing)},
                        audit_category="daily_fortune",
                    )
                    self.ledger.finish(uow, outcome)
                    return outcome
                value = draw_fortune(self.random)
                now = self._now().isoformat()
                row = {
                    "user_id": request.user_id,
                    "fortune_date": fortune_date,
                    "score": value.score,
                    "title": value.title,
                    "message": value.message,
                    "operation_id": request.operation_id,
                    "created_at": now,
                }
                self.repository.insert(uow, row)
                fortune = DailyFortune(request.user_id, fortune_date, value.score, value.title, value.message)
                outcome = OperationOutcome.applied(
                    request.operation_id,
                    self.action,
                    data={"fortune": fortune.to_dict()},
                    after=fortune.to_dict(),
                    audit_category="daily_fortune",
                    occurred_at=now,
                )
                self.ledger.finish(uow, outcome)
                return outcome
        except DomainError:
            raise
        except Exception as exc:
            # The business UoW has already rolled back.  Record the failure in
            # a fresh transaction so reconcile can distinguish it from a
            # request that never reached the application.
            try:
                self.ledger.record_failure(self.database, request.operation_id, self.action, payload, str(exc))
            except Exception:
                # A storage outage must not replace the original business
                # error; the outer runtime health check will report it.
                pass
            raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        result = self.claim(**kwargs)
        if result.message:
            content = result.message
        else:
            fortune = (result.data or {}).get("fortune", {}) if isinstance(result.data, dict) else {}
            content = (
                "✨ 今日运势 ✨\n"
                f"运势：{fortune.get('title', '')} {fortune.get('score', '')}\n"
                f"签文：{fortune.get('message', '')}"
            )
        return ReplyPlan(content, reference=True)


__all__ = ["DailyFortuneApplication"]
