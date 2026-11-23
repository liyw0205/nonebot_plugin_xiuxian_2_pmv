from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.player_reasons import reason_text
from ...core.ports import Clock
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.clock import SystemClock
from ...infrastructure.observability import trace_context
from .domain import DungeonPurchaseRequest
from .explore_resolution import (
    DungeonExplorePlanApplication,
    InvalidDungeonExploreIntent,
    explore_response,
)
from .repository import DungeonRepository, DungeonSessionSqlRepository
from .reset_repository import DungeonResetSqlRepository


DUNGEON_PURCHASE_RETRYABLE_REJECTIONS = frozenset(
    {
        "dungeon_purchase_stone_write_rejected",
        "dungeon_purchase_inventory_write_rejected",
        "dungeon_purchase_receipt_write_rejected",
    }
)


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class DungeonApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: DungeonRepository | None = None, ledger: OperationLedger | None = None, clock: Clock | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.clock = clock or SystemClock()
        self.ledger = ledger or OperationLedger(clock=self.clock)

    def _repository(self) -> DungeonRepository:
        return self.repository or DungeonSessionSqlRepository(self.game_database, self.player_database)

    def _reset_repository(self) -> DungeonResetSqlRepository:
        return DungeonResetSqlRepository(self.player_database, clock=self.clock)

    def _execute_purchase(self, request: DungeonPurchaseRequest) -> OperationOutcome[dict[str, Any]]:
        action = "dungeon.purchase"
        with trace_context(operation_id=request.operation_id, user_scope=request.user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, request.operation_id, action, request.payload())
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = _data(self._repository().purchase(request.operation_id, request.user_id, request.item_id, request.item_name, request.item_type, request.quantity, request.unit_cost, request.expected_stone, request.max_goods, request.bind_flag))
                status = str(raw.get("status", "failed"))
                data = {"status": status, **raw}
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(request.operation_id, action, data=data, consumed={"stone": int(raw.get("cost", request.quantity * request.unit_cost) or 0)}, granted={"items": request.quantity}, audit_category="dungeon")
                else:
                    outcome = OperationOutcome.rejected(request.operation_id, action, str(raw.get("response", "副本兑换未完成。")), code=status, data=data, audit_category="dungeon")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError as exc:
                if exc.code in DUNGEON_PURCHASE_RETRYABLE_REJECTIONS:
                    details = exc.details if isinstance(exc.details, Mapping) else {}
                    reason = str(details.get("reason") or exc.code)
                    subject = str(details.get("subject") or "actor")
                    message = reason_text(reason) or "副本兑换写入未完成，可在故障解除后重试。"
                    message += f"（原因：{reason}；主体：{subject}）"
                    outcome = OperationOutcome.failed(
                        request.operation_id,
                        action,
                        message,
                        code=reason,
                        data={"status": "state_changed", "reason": reason, "subject": subject},
                        audit_category="dungeon",
                    )
                    with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                        self.ledger.finish(uow, outcome)
                    return outcome
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, request.operation_id, action, request.payload(), str(exc))
                raise

    def purchase(self, *, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, expected_stone: int, max_goods: int, bind_flag: int = 1) -> OperationOutcome[dict[str, Any]]:
        try:
            request = DungeonPurchaseRequest(str(operation_id).strip(), str(user_id).strip(), int(item_id), str(item_name), str(item_type), int(quantity), int(unit_cost), int(expected_stone), int(max_goods), int(bind_flag))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute_purchase(request)

    def operation_result(self, *, operation_id: str, user_id: str, item_id: int, quantity: int, bind_flag: int = 1) -> Any:
        return self._repository().operation_result(operation_id, user_id, item_id, quantity, bind_flag)

    def session_operation(self, *, operation_id: str, user_id: str, action: str) -> Any:
        return self._repository().operation_session_result(operation_id, user_id, action)

    def session_transition(self, *, operation_id: str, user_id: str, expected: Mapping[str, Any], dungeon: Mapping[str, Any], action: str) -> Any:
        return self._repository().session_transition(operation_id, user_id, dict(expected), dict(dungeon), action)

    def reset(self, operation_id: str, business_date: Any, source: str, dungeon_factory: Any) -> dict[str, Any]:
        return self._reset_repository().reset(operation_id, business_date, source, dungeon_factory)

    def reset_operation_result(self, operation_id: str) -> dict[str, Any] | None:
        return self._reset_repository().operation_result(operation_id)

    def global_state(self) -> dict[str, Any] | None:
        return self._reset_repository().global_state()

    def ensure_player_status(self, user_id: str, fallback_snapshot: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._reset_repository().ensure_player_status(user_id, fallback_snapshot)

    def replay(self, *, operation_id: str, user_id: str) -> Any:
        return self._repository().replay(operation_id, user_id)

    def prepare(self, *, operation_id: str, user_id: str, plan: Mapping[str, Any]) -> Any:
        if not str(operation_id).strip() or not str(user_id).strip() or not isinstance(plan, Mapping):
            raise ValidationError("operation_id, user_id and plan are required")
        return self._repository().prepare(operation_id, user_id, dict(plan))

    def prepare_intent(self, *, operation_id: str, user_id: str, intent: Mapping[str, Any]) -> Any:
        if not str(operation_id).strip() or not str(user_id).strip() or not isinstance(intent, Mapping):
            raise ValidationError("operation_id, user_id and intent are required")
        return self._repository().prepare_intent(operation_id, user_id, dict(intent))

    def prepare_resolution(self, *, operation_id: str, user_id: str, plan: Mapping[str, Any]) -> Any:
        if not str(operation_id).strip() or not str(user_id).strip() or not isinstance(plan, Mapping):
            raise ValidationError("operation_id, user_id and plan are required")
        return self._repository().prepare_resolution(operation_id, user_id, dict(plan))

    def settle(self, *, operation_id: str, user_id: str, max_goods_num: int) -> Any:
        if not str(operation_id).strip() or not str(user_id).strip() or int(max_goods_num) < 0:
            raise ValidationError("operation_id, user_id and max_goods_num are required")
        return self._repository().settle(operation_id, user_id, int(max_goods_num))

    def resolve_rejection(self, *, operation_id: str, user_id: str, result_status: str, response: Mapping[str, Any], max_goods_num: int, current_layer: int = 0, dungeon_status: str = "") -> Any:
        if not str(operation_id).strip() or not str(user_id).strip():
            raise ValidationError("operation_id and user_id are required")
        return self._repository().resolve_rejection(operation_id, user_id, result_status, dict(response), int(max_goods_num), current_layer=int(current_layer), dungeon_status=str(dungeon_status))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "purchase"))
        result = getattr(self, action)(**kwargs)
        return ReplyPlan(result.data if isinstance(result, OperationOutcome) else _data(result), reference=True)


class DungeonExploreExecutionApplication:
    """Resolve a frozen exploration intent and hand its plan to durable settlement."""

    def __init__(self, storage: DungeonApplication, planner: DungeonExplorePlanApplication) -> None:
        self.storage = storage
        self.planner = planner

    def resume_prepared(
        self, *, operation_id: str, user_id: str, max_goods_num: int
    ) -> dict[str, Any]:
        return self.storage.settle(
            operation_id=operation_id,
            user_id=user_id,
            max_goods_num=max_goods_num,
        )

    async def resolve(
        self,
        *,
        operation_id: str,
        user_id: str,
        intent: Mapping[str, Any],
        max_goods_num: int,
    ) -> dict[str, Any]:
        try:
            plan = await self.planner.build(operation_id, user_id, intent)
        except InvalidDungeonExploreIntent:
            expected = intent.get("expected_status") if isinstance(intent, Mapping) else {}
            expected = expected if isinstance(expected, Mapping) else {}
            response = explore_response(
                "副本操作未结算：冻结的战斗输入已损坏或发生变化，请重新发起。"
            )
            response.update(reason="dungeon_explore_invalid_intent", subject=str(user_id))
            return self.storage.resolve_rejection(
                operation_id=operation_id,
                user_id=user_id,
                result_status="state_changed",
                response=response,
                max_goods_num=max_goods_num,
                current_layer=int(expected.get("current_layer", 0) or 0),
                dungeon_status=str(expected.get("dungeon_status", "")),
            )

        prepared = self.storage.prepare_resolution(
            operation_id=operation_id, user_id=user_id, plan=plan
        )
        if prepared.get("status") in {"operation_conflict", "schema_missing", "unsupported"}:
            return prepared
        if prepared.get("phase") == "completed":
            return prepared

        settled = self.storage.settle(
            operation_id=operation_id,
            user_id=user_id,
            max_goods_num=max_goods_num,
        )
        return settled if settled.get("phase") == "completed" else {
            **settled,
            "status": "processing_failure",
        }

__all__ = ["DungeonApplication", "DungeonExploreExecutionApplication"]
