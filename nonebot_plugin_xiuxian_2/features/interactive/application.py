from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .repository import InteractiveRepository


class InteractiveApplication:
    """Application boundary for the four interactive daily actions."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: InteractiveRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or InteractiveRepository()
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _action(value: Any) -> str:
        action = str(value or "execute").strip().casefold()
        if action in {"exp", "exp_settle", "experience", "give_exp"}:
            return "exp_settle"
        if action in {"stone", "stone_settle", "give_stone"}:
            return "stone_settle"
        if action in {"greeting", "greeting_claim", "morning", "night"}:
            return "greeting_claim"
        if action in {"fortune", "fortune_resolve", "daily_fortune"}:
            return "fortune_resolve"
        return action

    def execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload or {})
        kind = self._action(request.pop("action", request.pop("operation", "execute")))
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        if kind not in {"exp_settle", "stone_settle", "greeting_claim", "fortune_resolve"}:
            return self._unsupported(operation_id, user_id, kind, request)
        if kind == "exp_settle":
            action = "interactive.exp_settle"
        elif kind == "stone_settle":
            action = "interactive.stone_settle"
        elif kind == "greeting_claim":
            action = "interactive.greeting_claim"
        else:
            action = "interactive.fortune_resolve"
        ledger_payload = {key: value for key, value in request.items() if key != "create_fortune"}
        ledger_payload["user_id"] = user_id
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, ledger_payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    if kind == "exp_settle":
                        result = self.repository.settle_exp(
                            uow,
                            operation_id=operation_id,
                            user_id=user_id,
                            expected_exp=int(request["expected_exp"]),
                            expected_level=str(request["expected_level"]),
                            rank_value=int(request["rank_value"]),
                            business_day=request["business_date"],
                        )
                    elif kind == "stone_settle":
                        result = self.repository.settle_stone(
                            uow,
                            operation_id=operation_id,
                            user_id=user_id,
                            expected_stone=int(request["expected_stone"]),
                            business_day=request["business_date"],
                        )
                    elif kind == "greeting_claim":
                        result = self.repository.claim_greeting(
                            uow,
                            operation_id=operation_id,
                            user_id=user_id,
                            kind=str(request["kind"]),
                            business_day=request["business_date"],
                        )
                    else:
                        result = self.repository.resolve_fortune(
                            uow,
                            operation_id=operation_id,
                            user_id=user_id,
                            business_day=request["business_date"],
                            create_fortune=request["create_fortune"],
                        )
                    data = result.to_dict()
                    if result.status in {"applied", "claimed", "generated", "existing"}:
                        outcome = OperationOutcome.applied(
                            operation_id,
                            action,
                            data=data,
                            granted={
                                key: data[key]
                                for key in ("exp_reward", "stone_reward", "position")
                                if data.get(key) not in (None, 0, False)
                            },
                            audit_category="interactive",
                        )
                    elif result.status == "duplicate":
                        outcome = OperationOutcome.applied(operation_id, action, data=data, audit_category="interactive")
                    else:
                        messages = {
                            "already_claimed": "今日已经领取过互动奖励了。",
                            "user_missing": "未找到角色信息，无法完成互动。",
                            "state_changed": "互动未结算：角色当前状态已更新。",
                            "operation_conflict": "本次互动事件与已记录结果冲突。",
                        }
                        outcome = OperationOutcome.rejected(
                            operation_id,
                            action,
                            messages.get(result.status, "互动操作未完成。"),
                            code=result.status,
                            data=data,
                            audit_category="interactive",
                        )
                    self.ledger.finish(uow, outcome)
                    return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, action, ledger_payload, str(exc))
                raise

    def _unsupported(self, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any]) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload)
        request["user_id"] = user_id
        ledger_action = "interactive.execute"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            existing = self.ledger.begin(uow, operation_id, ledger_action, request)
            if existing is not None:
                previous = existing.outcome()
                if previous is not None:
                    return previous.replay()
                raise ConflictError("操作正在处理中")
            outcome = OperationOutcome.applied(
                operation_id,
                ledger_action,
                data={"status": "unsupported", "action": action, "user_id": user_id},
                audit_category="interactive",
            )
            self.ledger.finish(uow, outcome)
            return outcome

    def cleanup_before(self, cutoff: Any) -> int:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            return self.repository.cleanup_greeting(uow, cutoff) + self.repository.cleanup_fortune(uow, cutoff)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.execute(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["InteractiveApplication"]
