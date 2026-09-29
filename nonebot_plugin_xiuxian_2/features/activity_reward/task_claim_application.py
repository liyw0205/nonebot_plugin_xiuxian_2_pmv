from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .task_claim_repository import ActivityTaskClaimRepository, ActivityTaskClaimResult


class ActivityTaskClaimApplication:
    action = "activity_reward.tasks.claim"

    def __init__(
        self,
        game_database: str | Path,
        activity_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.clock = clock or SystemClock()
        self.repository = ActivityTaskClaimRepository(game_database, activity_database, clock=self.clock)
        self.ledger = OperationLedger(clock=self.clock)

    @staticmethod
    def _request(
        user_id: str,
        activity_key: str,
        tasks: Any,
        max_goods_num: int,
    ) -> dict[str, Any]:
        return {
            "user_id": str(user_id),
            "activity_key": str(activity_key),
            "tasks": [
                [str(row[0]), str(row[1]), str(row[2]), int(row[3]), str(row[4]),
                 [dict(item) for item in row[5]], str(row[6])]
                for row in tasks
            ],
            "max_goods_num": int(max_goods_num),
        }

    @staticmethod
    def _from_outcome(outcome: OperationOutcome[Any]) -> ActivityTaskClaimResult:
        data = outcome.data if isinstance(outcome.data, Mapping) else {}
        status = str(data.get("status") or outcome.code or outcome.status)
        if status == "applied" and outcome.replayed:
            status = "duplicate"
        return ActivityTaskClaimResult(status, tuple(tuple(row) for row in data.get("rewards", ())))

    @staticmethod
    def _outcome(operation_id: str, result: ActivityTaskClaimResult) -> OperationOutcome[Any]:
        data = {"status": "applied" if result.succeeded else result.status, "rewards": list(result.rewards)}
        if result.succeeded:
            return OperationOutcome.applied(
                operation_id,
                ActivityTaskClaimApplication.action,
                data=data,
                granted={"activity_task_rewards": len(result.rewards)},
                audit_category="activity_reward",
            )
        messages = {
            "inventory_full": "背包空间不足，奖励未领取",
            "user_missing": "角色不存在",
            "state_changed": "任务领取未完成：任务进度已更新，请重新查询任务",
            "claim_in_progress": "任务奖励正在处理中，请稍后重试",
            "operation_conflict": "领取请求冲突，请重新发送",
        }
        return OperationOutcome.rejected(
            operation_id,
            ActivityTaskClaimApplication.action,
            messages.get(result.status, "当前没有可领取的活动任务奖励"),
            code=result.status,
            data=data,
            audit_category="activity_reward",
        )

    def get_result(self, operation_id: str, user_id: str | None = None) -> ActivityTaskClaimResult | None:
        return self.repository.get_result(operation_id, user_id)

    def claim(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        tasks: Any,
        max_goods_num: int,
    ) -> ActivityTaskClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        self.repository.assert_schema_ready()
        previous = self.repository.get_result(operation_id, user_id)
        if previous is not None:
            return previous
        request = self._request(user_id, activity_key, tasks, max_goods_num)
        self.repository.validate_claim(
            operation_id, user_id, activity_key, request["tasks"], max_goods_num
        )
        identity = {"user_id": user_id, "activity_key": str(activity_key)}
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, self.action, identity)
                    if existing is not None and existing.outcome() is not None:
                        return self._from_outcome(existing.outcome())
                    prepared, newly_prepared = self.repository.prepare_operation(
                        uow, operation_id, user_id, activity_key, request["tasks"], max_goods_num
                    )
                    if prepared is not None:
                        self.ledger.finish(uow, self._outcome(operation_id, prepared))
                        return prepared
                result = self.repository.claim(
                    operation_id, user_id, activity_key, request["tasks"], max_goods_num,
                    newly_prepared=newly_prepared,
                )
                outcome = self._outcome(operation_id, result)
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return result
            except OperationConflictError:
                return ActivityTaskClaimResult("operation_conflict")
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, self.action, identity, str(exc))
                raise

    def reconcile(self, record: Mapping[str, Any]) -> OperationOutcome[Any]:
        operation_id = str(record.get("operation_id", "")).strip()
        if str(record.get("action", "")) != self.action or not operation_id:
            raise ValueError("invalid activity task claim reconcile record")
        result = self.repository.reconcile(operation_id)
        return self._outcome(operation_id, result)


__all__ = ["ActivityTaskClaimApplication"]
