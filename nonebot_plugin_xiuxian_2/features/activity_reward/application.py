from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import ActivityClaimRequest
from .repository import ActivityRewardRepository, LegacyActivityRewardRepository
from .schemas import ActivityClaimResult
from .claim_all_application import ActivityClaimAllApplication


class ActivityRewardApplication:
    action = "activity.claim_all"

    def __init__(self, database: str | Path, *, repository: ActivityRewardRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def claim_all(self, *, operation_id: str, user_id: str) -> OperationOutcome[dict[str, Any]]:
        request = ActivityClaimRequest(str(operation_id).strip(), str(user_id).strip())
        try:
            request.validate()
        except ValueError as exc:
            raise ValidationError(str(exc)) from exc
        payload = {"user_id": request.user_id}
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                if self.repository is None:
                    from ...xiuxian.xiuxian_activity.service import claim_activity_tasks, claim_activity_pass_rewards
                    from ...xiuxian.xiuxian_activity.activity_boss import claim_boss_milestone_reward, claim_boss_rank_reward
                    raw = ActivityClaimAllApplication(self.database).run(
                        request.operation_id,
                        request.user_id,
                        {
                            "tasks": lambda child_id: claim_activity_tasks(request.user_id, operation_id=child_id),
                            "pass": lambda child_id: claim_activity_pass_rewards(request.user_id, operation_id=child_id),
                            "boss_milestone": lambda child_id: claim_boss_milestone_reward(request.user_id, operation_id=child_id),
                            "boss_rank": lambda child_id: claim_boss_rank_reward(request.user_id, operation_id=child_id),
                        },
                    )
                else:
                    raw = self.repository.claim_all(request.operation_id, request.user_id)
                if isinstance(raw, tuple):
                    ok, text = bool(raw[0]), str(raw[1] or "")
                elif isinstance(raw, dict):
                    ok, text = bool(raw.get("ok")), str(raw.get("text") or "")
                else:
                    ok, text = bool(getattr(raw, "ok", False)), str(getattr(raw, "text", "") or "")
                data = ActivityClaimResult("applied" if ok else "rejected", request.operation_id, request.user_id, text).to_dict()
                outcome = (
                    OperationOutcome.applied(request.operation_id, self.action, data=data, granted={"activity_rewards": 1}, audit_category="activity_reward")
                    if ok
                    else OperationOutcome.rejected(request.operation_id, self.action, text or "当前没有可领取奖励", code="not_claimable", data=data, audit_category="activity_reward")
                )
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, request.operation_id, self.action, payload, str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.claim_all(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["ActivityRewardApplication"]
