from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import BegDailyRewardResult, NoviceGiftClaimResult
from .repository import BegRepository


class BegApplication:
    """Application boundary for the daily reward and novice gift actions."""

    def __init__(
        self,
        database: str | Path,
        *,
        repository: BegRepository | None = None,
        ledger: OperationLedger | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or BegRepository()
        self.ledger = ledger or OperationLedger()

    @staticmethod
    def _action(value: Any) -> str:
        action = str(value or "execute").strip().casefold()
        aliases = {
            "daily": "daily_settle",
            "daily_reward": "daily_settle",
            "beg": "daily_settle",
            "novice": "novice_claim",
            "gift": "novice_claim",
            "novice_gift": "novice_claim",
        }
        return aliases.get(action, action)

    @staticmethod
    def _result_outcome(operation_id: str, action: str, result: BegDailyRewardResult | NoviceGiftClaimResult) -> OperationOutcome[dict[str, Any]]:
        data = result.to_dict()
        if result.status in {"applied", "duplicate"}:
            granted = {}
            if isinstance(result, BegDailyRewardResult):
                granted = {"stone": result.stone_reward}
            elif result.stone:
                granted = {"stone": result.stone}
            return OperationOutcome.applied(
                operation_id,
                action,
                data=data,
                granted=granted,
                audit_category="beg",
            )
        messages = {
            "already_claimed": "该机缘已经领取过了。",
            "expired": "已超过新手期，无法领取。",
            "ineligible_sect": "道友已有宗门庇佑，又何必来此寻求机缘呢？",
            "ineligible_root": "道友已是轮回大能，又何必来此寻求机缘呢？",
            "ineligible_level": "当前修为已超过新手机缘范围。",
            "inventory_full": "背包空间不足，无法领取新手礼包。",
            "operation_conflict": "领取失败：请求冲突，请勿重复提交。",
            "state_changed": "领取未结算：角色当前状态已更新。",
            "user_missing": "未找到角色信息，无法领取。",
        }
        return OperationOutcome.rejected(
            operation_id,
            action,
            messages.get(result.status, "操作未完成。"),
            code=result.status,
            data=data,
            audit_category="beg",
        )

    def execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        payload: Mapping[str, Any] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload or {})
        kind = self._action(request.pop("action", request.pop("operation", request.pop("method", "execute"))))
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        action = f"beg.{kind}"
        # Expected snapshots and reward rolls are concurrency inputs, not the
        # identity of an already accepted event.  This preserves old command
        # replay semantics while the ledger still detects a different user.
        ledger_payload = {"user_id": user_id}
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, ledger_payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")

                    if kind == "daily_settle":
                        result = self.repository.settle_daily(uow, operation_id=operation_id, user_id=user_id, **request)
                    elif kind == "novice_claim":
                        result = self.repository.claim_novice(uow, operation_id=operation_id, user_id=user_id, **request)
                    else:
                        outcome = OperationOutcome.applied(
                            operation_id,
                            action,
                            data={"status": "unsupported", "action": kind, "user_id": user_id},
                            audit_category="beg",
                        )
                        self.ledger.finish(uow, outcome)
                        return outcome
                    outcome = self._result_outcome(operation_id, action, result)
                    self.ledger.finish(uow, outcome)
                    return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, action, ledger_payload, str(exc))
                raise

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.execute(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["BegApplication"]
