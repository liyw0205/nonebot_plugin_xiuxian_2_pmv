from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import WorkClaimRequest, WorkSettlementRequest
from .repository import (
    LegacyWorkClaimRepository,
    LegacyWorkSettlementRepository,
    WorkClaimRepository,
    WorkSettlementRepository,
)


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class WorkClaimApplication:
    action = "work.claim"

    def __init__(self, database: str | Path, *, repository: WorkClaimRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def claim(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_count: int,
        expected_offer: Mapping[str, Any],
        task_index: int,
        started_at: str,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = WorkClaimRequest(
                str(operation_id).strip(), str(user_id).strip(), int(expected_count),
                dict(expected_offer or {}), int(task_index), str(started_at).strip(),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.repository or LegacyWorkClaimRepository(self.database)
                raw = repository.claim(
                    request.operation_id, request.user_id, request.expected_count,
                    request.expected_offer, request.task_index, request.started_at,
                )
                data = _data(raw)
                status = str(data.get("status", "failed"))
                normalized = {
                    "status": status,
                    "operation_id": request.operation_id,
                    "task_name": str(data.get("task_name") or ""),
                    "started_at": str(data.get("started_at") or request.started_at),
                    "remaining_count": int(data.get("remaining_count", request.expected_count) or 0),
                }
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=normalized,
                        after={"work_status": 2, "task_name": normalized["task_name"]},
                        audit_category="work",
                    )
                else:
                    messages = {
                        "invalid_task": "没有这样的悬赏编号！",
                        "state_changed": "悬赏次数或列表已更新，请先发送【悬赏令】再操作。",
                        "user_missing": "未找到修仙数据。",
                        "operation_conflict": "接取请求已失效，请重新接取悬赏。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        messages.get(status, "悬赏令接取未完成。"),
                        code=status,
                        data=normalized,
                        audit_category="work",
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
        return ReplyPlan(self.claim(**kwargs).data, reference=True)

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_work: Mapping[str, Any],
        exp_gain: int,
        item: Mapping[str, Any] | None,
        max_exp: int,
        max_goods_num: int,
        success_kind: str = "",
        item_msg: str = "",
    ) -> OperationOutcome[dict[str, Any]]:
        """Expose settlement on the feature facade used by the Web adapter."""
        repository = self.repository if self.repository is not None and hasattr(self.repository, "settle") else None
        return WorkSettlementApplication(self.database, repository=repository).settle(
            operation_id=operation_id,
            user_id=user_id,
            expected_work=expected_work,
            exp_gain=exp_gain,
            item=item,
            max_exp=max_exp,
            max_goods_num=max_goods_num,
            success_kind=success_kind,
            item_msg=item_msg,
        )


class WorkSettlementApplication:
    action = "work.settle"

    def __init__(self, database: str | Path, *, repository: WorkSettlementRepository | None = None,
                 ledger: OperationLedger | None = None) -> None:
        self.database = str(database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def settle(
        self,
        *,
        operation_id: str,
        user_id: str,
        expected_work: Mapping[str, Any],
        exp_gain: int,
        item: Mapping[str, Any] | None,
        max_exp: int,
        max_goods_num: int,
        success_kind: str = "",
        item_msg: str = "",
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = WorkSettlementRequest(
                str(operation_id).strip(),
                str(user_id).strip(),
                dict(expected_work or {}),
                int(exp_gain),
                dict(item) if item is not None else None,
                int(max_exp),
                int(max_goods_num),
                str(success_kind or ""),
                str(item_msg or ""),
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        payload = request.payload()
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                repository = self.repository or LegacyWorkSettlementRepository(self.database)
                raw = repository.settle(
                    request.operation_id,
                    request.user_id,
                    request.expected_work,
                    request.exp_gain,
                    request.item,
                    request.max_exp,
                    request.max_goods_num,
                    success_kind=request.success_kind,
                    item_msg=request.item_msg,
                )
                data = _data(raw)
                status = str(data.get("status", "failed"))
                normalized = {
                    "status": status,
                    "operation_id": request.operation_id,
                    "exp": int(data.get("exp", 0) or 0),
                    "item_awarded": bool(data.get("item_awarded", False)),
                    "success_kind": str(data.get("success_kind") or request.success_kind),
                    "item_msg": str(data.get("item_msg") or request.item_msg),
                    "scheduled_time": str(data.get("scheduled_time") or request.expected_work.get("scheduled_time") or ""),
                }
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        request.operation_id,
                        self.action,
                        data=normalized,
                        granted={
                            "exp": normalized["exp"],
                            "item": normalized["item_msg"] if normalized["item_awarded"] else None,
                        },
                        audit_category="work",
                    )
                else:
                    messages = {
                        "inventory_full": "背包物品已达上限，悬赏奖励尚未结算。",
                        "user_missing": "悬赏结算失败：未找到角色数据。",
                        "state_changed": "悬赏结算未完成：悬赏进度已更新，请重新查看悬赏。",
                    }
                    outcome = OperationOutcome.rejected(
                        request.operation_id,
                        self.action,
                        messages.get(status, "悬赏结算未完成。"),
                        code=status,
                        data=normalized,
                        audit_category="work",
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
        return ReplyPlan(self.settle(**kwargs).data, reference=True)


__all__ = ["WorkClaimApplication", "WorkSettlementApplication"]
