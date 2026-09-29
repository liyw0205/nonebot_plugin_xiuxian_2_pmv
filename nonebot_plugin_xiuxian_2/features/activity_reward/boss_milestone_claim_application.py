from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.ids import UUIDGenerator
from ...infrastructure.observability import trace_context
from .boss_milestone_claim_repository import (
    ActivityBossMilestoneClaimRepository,
    ActivityBossMilestoneClaimResult,
)


class ActivityBossMilestoneClaimApplication:
    action = "activity_reward.boss_milestone.claim"

    def __init__(
        self,
        game_database: str | Path,
        activity_database: str | Path,
        *,
        clock: Any | None = None,
        ids: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.clock = clock or SystemClock()
        self.ids = ids or UUIDGenerator()
        self.repository = ActivityBossMilestoneClaimRepository(
            game_database, activity_database, clock=self.clock
        )
        self.ledger = OperationLedger(clock=self.clock)

    @staticmethod
    def _identity(user_id: str, activity_key: str) -> dict[str, str]:
        return {"user_id": str(user_id), "activity_key": str(activity_key)}

    @staticmethod
    def _outcome(operation_id: str, result: ActivityBossMilestoneClaimResult) -> OperationOutcome[Any]:
        data = {"status": "applied" if result.succeeded else result.status, "names": list(result.names)}
        if result.succeeded:
            return OperationOutcome.applied(
                operation_id,
                ActivityBossMilestoneClaimApplication.action,
                data=data,
                granted={"activity_boss_milestone_rewards": len(result.names)},
                audit_category="activity_reward",
            )
        messages = {
            "inventory_full": "背包空间不足，奖励未领取",
            "user_missing": "角色不存在",
            "state_changed": "首领进度领奖状态已变化，请重新查询",
            "claim_in_progress": "首领进度奖励正在处理中，请稍后重试",
            "not_unlocked": "全服进度奖励尚未解锁",
            "already_claimed": "没有可领取的全服进度奖励（可能已领过）",
            "operation_conflict": "领取请求冲突，请重新发送",
        }
        return OperationOutcome.rejected(
            operation_id,
            ActivityBossMilestoneClaimApplication.action,
            messages.get(result.status, f"领取失败（{result.status}）"),
            code=result.status,
            data=data,
            audit_category="activity_reward",
        )

    @staticmethod
    def _from_outcome(outcome: OperationOutcome[Any]) -> ActivityBossMilestoneClaimResult:
        data = outcome.data if isinstance(outcome.data, Mapping) else {}
        status = str(data.get("status") or outcome.code or outcome.status)
        if status == "applied" and outcome.replayed:
            status = "duplicate"
        return ActivityBossMilestoneClaimResult(status, tuple(str(name) for name in data.get("names", ())))

    def get_result(
        self,
        operation_id: str,
        user_id: str | None = None,
    ) -> ActivityBossMilestoneClaimResult | None:
        return self.repository.get_result(operation_id, user_id)

    def resume_pending(
        self,
        operation_id: str,
        user_id: str,
    ) -> ActivityBossMilestoneClaimResult | None:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        status = self.repository.pending_status(operation_id, user_id)
        if status is None:
            return None
        if status == "operation_conflict":
            return ActivityBossMilestoneClaimResult(status)
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT request_json FROM activity_boss_milestone_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            return None
        request = json.loads(str(row["request_json"]))
        identity = self._identity(user_id, str(request["activity_key"]))
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                result = self.repository.reconcile(operation_id)
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, self._outcome(operation_id, result))
                return result
            except OperationConflictError:
                return ActivityBossMilestoneClaimResult("operation_conflict")
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, self.action, identity, str(exc))
                raise

    def claim(
        self,
        user_id: str,
        activity_key: str,
        milestones: Any,
        max_goods_num: int,
        operation_id: str | None = None,
    ) -> ActivityBossMilestoneClaimResult:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        activity_key = str(activity_key).strip()
        if operation_id is None:
            operation_id = f"activity-boss-milestone:{self.ids.new_id()}"
        self.repository.assert_schema_ready()
        operation_id, payload = self.repository.validate_claim(
            operation_id, user_id, activity_key, milestones, max_goods_num
        )
        previous = self.repository.get_result(operation_id, user_id, payload=payload)
        if previous is not None:
            return previous
        identity = self._identity(user_id, activity_key)
        with trace_context(operation_id=operation_id, user_scope=user_id):
            prepared_committed = False
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, self.action, identity)
                    if existing is not None and existing.outcome() is not None:
                        return self._from_outcome(existing.outcome())
                    prepared, newly_prepared = self.repository.prepare_operation(
                        uow, operation_id, user_id, activity_key, milestones, max_goods_num
                    )
                    if prepared is not None:
                        if prepared.status == "operation_conflict":
                            raise OperationConflictError(operation_id, self.action)
                        self.ledger.finish(uow, self._outcome(operation_id, prepared))
                        return prepared
                prepared_committed = True
                result = self.repository.claim(
                    operation_id,
                    user_id,
                    activity_key,
                    milestones,
                    max_goods_num,
                    newly_prepared=newly_prepared,
                )
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, self._outcome(operation_id, result))
                return result
            except OperationConflictError:
                return ActivityBossMilestoneClaimResult("operation_conflict")
            except Exception as exc:
                if prepared_committed:
                    self.ledger.record_failure(self.game_database, operation_id, self.action, identity, str(exc))
                raise

    def reconcile(self, record: Mapping[str, Any]) -> OperationOutcome[Any]:
        operation_id = str(record.get("operation_id", "")).strip()
        if str(record.get("action", "")) != self.action or not operation_id:
            raise ValueError("invalid activity boss milestone reconcile record")
        return self._outcome(operation_id, self.repository.reconcile(operation_id))


__all__ = ["ActivityBossMilestoneClaimApplication"]
