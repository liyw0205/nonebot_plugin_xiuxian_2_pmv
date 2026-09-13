from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import IllusionChoiceResult, period_key
from .repository import IllusionRepository


class IllusionApplication:
    action = "illusion.choose"

    def __init__(
        self,
        database: str | Path,
        *,
        repository: IllusionRepository | None = None,
        ledger: OperationLedger | None = None,
        clock: Any | None = None,
    ) -> None:
        self.database = str(database)
        self.repository = repository or IllusionRepository()
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()

    def period_key(self) -> str:
        value = self.clock.now() if hasattr(self.clock, "now") else self.clock()
        return period_key(value)

    def get_choice(self, user_id: str, period: str | None = None) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.database) as uow:
            return self.repository.get_choice(uow, user_id, period or self.period_key())

    def get_result(self, operation_id: str) -> IllusionChoiceResult | None:
        with DatabaseUnitOfWork(self.database) as uow:
            return self.repository.get_result(uow, operation_id)

    def choose(
        self,
        *,
        operation_id: str,
        user_id: str,
        period: str,
        question_index: int,
        choice_index: int,
        selected_option: str,
        stone: int,
        exp: int,
        item: Mapping[str, Any] | None,
        max_goods_num: int,
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        if not operation_id or not user_id:
            raise ValidationError("operation_id and user_id are required")
        payload = {
            "user_id": user_id,
            "period": str(period),
            "question_index": int(question_index),
            "choice_index": int(choice_index),
            "selected_option": str(selected_option),
            "stone": int(stone),
            "exp": int(exp),
            "item": dict(item) if item is not None else None,
            "max_goods_num": int(max_goods_num),
        }
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, self.action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                    result = self.repository.choose(
                        uow,
                        operation_id=operation_id,
                        user_id=user_id,
                        period=payload["period"],
                        question_index=payload["question_index"],
                        choice_index=payload["choice_index"],
                        selected_option=payload["selected_option"],
                        stone=payload["stone"],
                        exp=payload["exp"],
                        item=payload["item"],
                        max_goods_num=payload["max_goods_num"],
                    )
                    data = result.to_dict()
                    if result.status == "applied":
                        outcome = OperationOutcome.applied(
                            operation_id,
                            self.action,
                            data=data,
                            granted={"stone": result.stone, "exp": result.exp, "item_id": result.item_id},
                            audit_category="illusion",
                        )
                    elif result.status == "duplicate":
                        outcome = OperationOutcome.applied(
                            operation_id,
                            self.action,
                            data=data,
                            audit_category="illusion",
                        )
                    else:
                        messages = {
                            "already_chosen": "今日已经参与过幻境寻心，请明日再来！",
                            "inventory_full": "背包物品已达上限，本次选择尚未提交。",
                            "user_missing": "幻境寻心失败：未找到角色数据。",
                            "state_changed": "幻境寻心未结算：当前状态已更新。",
                        }
                        outcome = OperationOutcome.rejected(
                            operation_id,
                            self.action,
                            messages.get(result.status, "幻境寻心操作未完成。"),
                            code=result.status,
                            data=data,
                            audit_category="illusion",
                        )
                    self.ledger.finish(uow, outcome)
                    return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, self.action, payload, str(exc))
                raise

    def execute(self, *, operation_id: str, user_id: str, payload: Mapping[str, Any] | None = None) -> OperationOutcome[dict[str, Any]]:
        request = dict(payload or {})
        action = str(request.pop("action", "choose"))
        if action != "choose":
            raise ValidationError(f"unsupported illusion action: {action}")
        return self.choose(
            operation_id=operation_id,
            user_id=user_id,
            period=str(request["period"]),
            question_index=int(request["question_index"]),
            choice_index=int(request["choice_index"]),
            selected_option=str(request["selected_option"]),
            stone=int(request.get("stone", 0)),
            exp=int(request.get("exp", 0)),
            item=request.get("item"),
            max_goods_num=int(request.get("max_goods_num", 0)),
        )

    def reply(self, **kwargs: Any) -> ReplyPlan:
        outcome = self.execute(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["IllusionApplication"]
