from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import OperationConflictError
from ...core.result import OperationOutcome
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .pass_claim_repository import ActivityPassClaimRepository, ActivityPassClaimResult


class ActivityPassClaimApplication:
    action = "activity_reward.pass.claim"

    def __init__(
        self,
        game_database: str | Path,
        activity_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.clock = clock or SystemClock()
        self.repository = ActivityPassClaimRepository(game_database, activity_database, clock=self.clock)
        self.ledger = OperationLedger(clock=self.clock)

    @staticmethod
    def _identity(user_id: str, activity_key: str) -> dict[str, str]:
        return {"user_id": str(user_id), "activity_key": str(activity_key)}

    @staticmethod
    def _outcome(operation_id: str, result: ActivityPassClaimResult) -> OperationOutcome[Any]:
        data = {"status": "applied" if result.succeeded else result.status, "rewards": list(result.rewards)}
        if result.succeeded:
            return OperationOutcome.applied(
                operation_id,
                ActivityPassClaimApplication.action,
                data=data,
                granted={"activity_pass_rewards": len(result.rewards)},
                audit_category="activity_reward",
            )
        messages = {
            "inventory_full": "背包空间不足，奖励未领取",
            "user_missing": "角色不存在",
            "state_changed": "战令领奖未完成：战令进度已更新，请重新查询战令",
            "claim_in_progress": "战令奖励正在处理中，请稍后重试",
            "operation_conflict": "领取请求冲突，请重新发送",
        }
        return OperationOutcome.rejected(
            operation_id,
            ActivityPassClaimApplication.action,
            messages.get(result.status, "当前没有可领取的活动战令奖励"),
            code=result.status,
            data=data,
            audit_category="activity_reward",
        )

    @staticmethod
    def _from_outcome(outcome: OperationOutcome[Any]) -> ActivityPassClaimResult:
        data = outcome.data if isinstance(outcome.data, Mapping) else {}
        status = str(data.get("status") or outcome.code or outcome.status)
        if status == "applied" and outcome.replayed:
            status = "duplicate"
        return ActivityPassClaimResult(status, tuple(tuple(row) for row in data.get("rewards", ())))

    def get_result(self, operation_id: str, user_id: str | None = None) -> ActivityPassClaimResult | None:
        return self.repository.get_result(operation_id, user_id)

    def resume_pending(self, operation_id: str, user_id: str) -> ActivityPassClaimResult | None:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        status = self.repository.pending_status(operation_id, user_id)
        if status is None:
            return None
        if status == "operation_conflict":
            return ActivityPassClaimResult(status)
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT request_json FROM activity_pass_reward_claim_operations WHERE operation_id=?",
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
                return ActivityPassClaimResult("operation_conflict")
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, self.action, identity, str(exc))
                raise

    def claim(
        self,
        operation_id: str,
        user_id: str,
        activity_key: str,
        current_level: int,
        rewards: Any,
        max_goods_num: int,
    ) -> ActivityPassClaimResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation_id and user_id are required")
        self.repository.assert_schema_ready()
        payload = self.repository.validate_claim(
            operation_id, user_id, activity_key, current_level, rewards, max_goods_num
        )
        previous = self.repository.get_result(operation_id, user_id, payload=payload)
        if previous is not None:
            return previous
        identity = self._identity(user_id, activity_key)
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, self.action, identity)
                    if existing is not None and existing.outcome() is not None:
                        return self._from_outcome(existing.outcome())
                    prepared, newly_prepared = self.repository.prepare_operation(
                        uow, operation_id, user_id, activity_key, current_level, rewards, max_goods_num
                    )
                    if prepared is not None:
                        self.ledger.finish(uow, self._outcome(operation_id, prepared))
                        return prepared
                result = self.repository.claim(
                    operation_id, user_id, activity_key, current_level, rewards, max_goods_num,
                    newly_prepared=newly_prepared,
                )
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, self._outcome(operation_id, result))
                return result
            except OperationConflictError:
                return ActivityPassClaimResult("operation_conflict")
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, self.action, identity, str(exc))
                raise

    def reconcile(self, record: Mapping[str, Any]) -> OperationOutcome[Any]:
        operation_id = str(record.get("operation_id", "")).strip()
        if str(record.get("action", "")) != self.action or not operation_id:
            raise ValueError("invalid activity pass claim reconcile record")
        result = self.repository.reconcile(operation_id)
        return self._outcome(operation_id, result)


__all__ = ["ActivityPassClaimApplication"]
