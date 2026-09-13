from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import PackageReward, normalize_rewards
from .repository import PackageRewardRepository
from .schemas import PackageOpenRequest


class _AssetRejected(Exception):
    def __init__(self, status: str) -> None:
        self.status = status


class PackageRewardApplication:
    action = "package_reward.open"

    def __init__(self, database: str | Path, *, repository: PackageRewardRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.repository = repository or PackageRewardRepository()
        self.ledger = ledger or OperationLedger()

    def lookup(self, operation_id: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database) as uow:
            record = self.repository.operation(uow, str(operation_id).strip())
        return record

    def open_package(
        self,
        *,
        operation_id: str,
        user_id: str,
        package_id: int,
        quantity: int,
        rewards: tuple[PackageReward, ...] | list[PackageReward],
        max_goods_num: int,
    ) -> OperationOutcome[dict[str, Any]]:
        normalized_rewards = normalize_rewards(rewards)
        request = PackageOpenRequest(str(operation_id).strip(), str(user_id), int(package_id), int(quantity), normalized_rewards, int(max_goods_num))
        try:
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = {
            "user_id": request.user_id,
            "package_id": request.package_id,
            "quantity": request.quantity,
            "max_goods_num": request.max_goods_num,
            "rewards": [item.to_dict() for item in request.rewards],
        }
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    before = self.repository.snapshot(uow, request.user_id)
                    legacy = self.repository.operation(uow, request.operation_id)
                    if legacy is not None:
                        if legacy["user_id"] != request.user_id or legacy["package_id"] != request.package_id or legacy["quantity"] != request.quantity:
                            outcome = OperationOutcome.rejected(request.operation_id, self.action, "操作参数已变化", code="state_changed", before=before, after=before, audit_category="package_reward")
                        else:
                            data = {"package_reward": {**legacy, "rewards": [item.to_dict() for item in legacy["rewards"]]}}
                            outcome = OperationOutcome.applied(request.operation_id, self.action, data=data, before=before, after=before, granted={"rewards": data["package_reward"]["rewards"]}, audit_category="package_reward")
                        self.ledger.finish(uow, outcome)
                        return outcome.replay() if outcome.ok else outcome

                    try:
                        # The repository may discover capacity or balance
                        # failures after decrementing the package.  Roll all
                        # asset changes back while retaining the rejection
                        # audit in the outer transaction.
                        with uow.savepoint("package_reward_assets"):
                            status = self.repository.apply(uow, operation_id=request.operation_id, user_id=request.user_id, package_id=request.package_id, quantity=request.quantity, rewards=request.rewards, max_goods_num=request.max_goods_num)
                            if status != "applied":
                                raise _AssetRejected(status)
                    except _AssetRejected as rejected:
                        status = rejected.status
                    if status != "applied":
                        messages = {
                            "user_missing": ("未找到修仙数据，礼包未结算。", "user_missing"),
                            "item_insufficient": ("礼包数量不足。", "item_insufficient"),
                            "stone_insufficient": ("灵石不足，礼包未结算。", "stone_insufficient"),
                            "inventory_full": ("背包已满，礼包未结算。", "inventory_full"),
                            "state_changed": ("礼包状态已变化，请刷新后重试。", "state_changed"),
                        }
                        message, code = messages.get(status, ("礼包未结算。", status))
                        outcome = OperationOutcome.rejected(request.operation_id, self.action, message, code=code, before=before, after=before, audit_category="package_reward")
                        self.ledger.finish(uow, outcome)
                        return outcome
                    after = self.repository.snapshot(uow, request.user_id)
                    data = {"package_reward": {"operation_id": request.operation_id, "user_id": request.user_id, "package_id": request.package_id, "quantity": request.quantity, "rewards": [item.to_dict() for item in request.rewards]}}
                    outcome = OperationOutcome.applied(request.operation_id, self.action, data=data, before=before, after=after, consumed={"package": request.quantity}, granted={"rewards": data["package_reward"]["rewards"]}, audit_category="package_reward")
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
        result = self.open_package(**kwargs)
        return ReplyPlan(result.message or result.data, reference=True)


__all__ = ["PackageRewardApplication"]
