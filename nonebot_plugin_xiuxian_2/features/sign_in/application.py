from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from .domain import SignInRecord, validate_limits
from .effects import NullSignInEffects, SignInEffects
from .repository import SignInRepository
from .schemas import SignInRequest


class SignInApplication:
    action = "sign_in.claim"

    def __init__(
        self,
        database: str | Path,
        *,
        repository: SignInRepository | None = None,
        ledger: OperationLedger | None = None,
        random_source: Any | None = None,
        clock: Any | None = None,
        lower_limit: int = 100000,
        upper_limit: int = 500000,
        effects: SignInEffects | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or SignInRepository()
        self.ledger = ledger or OperationLedger()
        self.random = random_source or SystemRandom()
        self.clock = clock or SystemClock()
        self.lower_limit, self.upper_limit = validate_limits(lower_limit, upper_limit)
        self.effects = effects or NullSignInEffects()

    def _now(self) -> datetime:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if isinstance(value, datetime):
            return value
        raise TypeError("clock.now() must return a datetime")

    def lookup(self, operation_id: str) -> SignInRecord | None:
        with DatabaseUnitOfWork(self.database) as uow:
            return self.repository.operation(uow, str(operation_id).strip())

    def claim(
        self,
        *,
        user_id: str,
        operation_id: str,
        lower_limit: int | None = None,
        upper_limit: int | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            request = SignInRequest(
                str(user_id),
                str(operation_id),
                self.lower_limit if lower_limit is None else lower_limit,
                self.upper_limit if upper_limit is None else upper_limit,
            )
            try:
                request.validate()
                lower, upper = validate_limits(request.lower_limit, request.upper_limit)
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc
            payload = {"user_id": request.user_id, "lower_limit": lower, "upper_limit": upper}
            try:
                with DatabaseUnitOfWork(self.database) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            replayed = previous.replay()
                            self.effects.on_signed(
                                user_id=request.user_id,
                                operation_id=request.operation_id,
                                stone=int((replayed.data or {}).get("sign_in", {}).get("stone", 0)),
                                replayed=True,
                            )
                            return replayed
                        raise ConflictError("操作正在处理中")
                    legacy_operation = self.repository.operation(uow, request.operation_id)
                    if legacy_operation is not None:
                        snapshot = self.repository.user_snapshot(uow, request.user_id)
                        outcome = OperationOutcome.applied(
                            request.operation_id,
                            self.action,
                            data={"sign_in": legacy_operation.to_dict()},
                            before=snapshot,
                            after=snapshot,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        replayed = outcome.replay()
                        self.effects.on_signed(
                            user_id=request.user_id,
                            operation_id=request.operation_id,
                            stone=legacy_operation.stone,
                            replayed=True,
                        )
                        return replayed
                    before = self.repository.user_snapshot(uow, request.user_id)
                    states = self.repository.user_sign_states(uow, request.user_id)
                    if not states:
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "角色不存在",
                            code="user_missing",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    if all(state == 1 for state in states):
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "今日已经签到",
                            code="already_signed",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    stone = int(self.random.randint(lower, upper))
                    changed = self.repository.apply(uow, request.user_id, stone)
                    if changed < 1:
                        outcome = OperationOutcome.rejected(
                            request.operation_id,
                            self.action,
                            "今日已经签到",
                            code="already_signed",
                            before=before,
                            after=before,
                            audit_category="sign_in",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    record = SignInRecord(request.user_id, request.operation_id, stone)
                    self.repository.insert_operation(uow, record)
                    after = self.repository.user_snapshot(uow, request.user_id)
                    now = self._now().isoformat()
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data={"sign_in": record.to_dict()},
                        before=before,
                        after=after,
                        granted={"stone": stone},
                        audit_category="sign_in",
                        occurred_at=now,
                    )
                    self.ledger.finish(uow, outcome)
                    self.effects.on_signed(
                        user_id=request.user_id,
                        operation_id=request.operation_id,
                        stone=stone,
                        replayed=False,
                    )
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
        result = self.claim(**kwargs)
        if result.message:
            content = result.message
        else:
            sign_in = (result.data or {}).get("sign_in", {}) if isinstance(result.data, dict) else {}
            stone = sign_in.get("stone", result.granted.get("stone", 0))
            content = f"**修仙签到**\n---\n✅ 签到成功，获取{stone}块灵石!"
        return ReplyPlan(content, reference=True)


__all__ = ["SignInApplication"]
