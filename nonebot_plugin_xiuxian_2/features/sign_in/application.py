from __future__ import annotations

from datetime import datetime
from dataclasses import replace
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ...infrastructure.observability import trace_context
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
from .domain import SignInRecord, validate_limits
from .effects import NullSignInEffects, SignInEffects
from .repository import SignInRepository
from .schemas import SignInRequest


class SignInApplication:
    action = "sign_in.claim"
    effects_event = "sign_in.effects"

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
        outbox: OutboxStore | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or SignInRepository()
        self.ledger = ledger or OperationLedger()
        self.random = random_source or SystemRandom()
        self.clock = clock or SystemClock()
        self.lower_limit, self.upper_limit = validate_limits(lower_limit, upper_limit)
        self.effects = effects or NullSignInEffects()
        self.outbox = outbox or OutboxStore(clock=self.clock)

    def _now(self) -> datetime:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        if isinstance(value, datetime):
            return value
        raise TypeError("clock.now() must return a datetime")

    def _append_effect_event(
        self,
        uow: DatabaseUnitOfWork,
        outcome: OperationOutcome[dict[str, Any]],
        *,
        user_id: str,
        stone: int,
    ) -> None:
        # CLI and isolated application callers intentionally use no-op effects;
        # do not leave an unhandled outbox event in those contexts.
        if isinstance(self.effects, NullSignInEffects) or not outcome.ok:
            return
        self.outbox.append(
            uow,
            event_id=f"{outcome.operation_id}:{self.action}",
            aggregate_type="sign_in",
            aggregate_id=str(user_id),
            event_type=self.effects_event,
            payload={
                "operation_id": outcome.operation_id,
                "user_id": str(user_id),
                "stone": int(stone),
                "occurred_at": outcome.occurred_at or self._now().isoformat(),
            },
        )

    def reconcile_outbox_event(self, record: dict[str, Any]) -> str | None:
        payload = record.get("payload") or {}
        return self.effects.on_signed(
            user_id=str(payload["user_id"]),
            operation_id=str(payload["operation_id"]),
            stone=int(payload["stone"]),
            replayed=bool(record.get("replayed", True)),
        )

    def _apply_effects(self, outcome: OperationOutcome[dict[str, Any]], *, user_id: str, operation_id: str, stone: int, replayed: bool) -> OperationOutcome[dict[str, Any]]:
        if isinstance(self.effects, NullSignInEffects) or not outcome.ok:
            return outcome
        event_id = f"{operation_id}:{self.action}"
        try:
            with DatabaseUnitOfWork(self.database) as uow:
                row = self.outbox.get(uow, event_id)
            if row is None or (str(row["status"]) == "sent" and not replayed):
                # Applied rows created before the effects outbox was added do
                # not reveal whether their projections already ran.
                return outcome
            import json

            payload = json.loads(str(row["payload_json"]))
            message = self.reconcile_outbox_event({"payload": payload, "replayed": replayed})
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                self.outbox.mark_sent(uow, event_id)
        except Exception:
            # Asset mutation is already committed; side-effect failures must not
            # turn a successful operation into a retry that can duplicate assets.
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.outbox.mark_failed(uow, event_id)
            except Exception:
                pass
            message = "签到资产已结算，但附加奖励稍后补偿。"
        return replace(outcome, message=message or outcome.message)

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
                            uow.commit()
                            return self._apply_effects(replayed, user_id=request.user_id, operation_id=request.operation_id, stone=int((replayed.data or {}).get("sign_in", {}).get("stone", 0)), replayed=True)
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
                        self._append_effect_event(uow, outcome, user_id=request.user_id, stone=legacy_operation.stone)
                        uow.commit()
                        replayed = outcome.replay()
                        return self._apply_effects(replayed, user_id=request.user_id, operation_id=request.operation_id, stone=legacy_operation.stone, replayed=True)
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
                    self._append_effect_event(uow, outcome, user_id=request.user_id, stone=stone)
                    uow.commit()
                    return self._apply_effects(outcome, user_id=request.user_id, operation_id=request.operation_id, stone=stone, replayed=False)
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
