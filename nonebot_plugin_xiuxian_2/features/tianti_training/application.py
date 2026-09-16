from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.clock import SystemClock
from ...infrastructure.observability import trace_context
from .domain import (
    BreakthroughRequest,
    MedicineBathRequest,
    QiaoxueRequest,
    StoneTrainingRequest,
    normalize_plan,
)
from .repository import (
    StoneTrainingSqlRepository,
    TiantiBreakthroughSqlRepository,
    TiantiMedicineBathSqlRepository,
    TiantiQiaoxueSqlRepository,
    TiantiTrainingRepository,
)


def _result_data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class TiantiTrainingApplication:
    """Coordinates Tianti asset writes without embedding the legacy rules."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: TiantiTrainingRepository | None = None,
        ledger: OperationLedger | None = None,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()

    def _execute(
        self,
        *,
        operation_id: str,
        user_id: str,
        action: str,
        payload: Mapping[str, Any],
        ledger_database: str,
        call: Callable[[], Any],
        success_statuses: set[str],
        messages: Mapping[str, str],
        before: Mapping[str, Any] | None = None,
        after: Mapping[str, Any] | None = None,
        consumed: Mapping[str, Any] | None = None,
        granted: Mapping[str, Any] | None = None,
        result_summary: Callable[[Mapping[str, Any]], Mapping[str, Mapping[str, Any]]] | None = None,
    ) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(ledger_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = call()
                data = _result_data(raw)
                status = str(data.get("status", "failed"))
                summary = dict(result_summary(data)) if result_summary is not None else {}
                if status in success_statuses:
                    outcome = OperationOutcome.applied(
                        operation_id,
                        action,
                        data=data,
                        before=dict(summary.get("before", before or {})),
                        after=dict(summary.get("after", after or {})),
                        consumed=dict(summary.get("consumed", consumed or {})),
                        granted=dict(summary.get("granted", granted or {})),
                        audit_category="tianti_training",
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        operation_id,
                        action,
                        messages.get(status, "炼体操作未完成。"),
                        code=status,
                        data=data,
                        audit_category="tianti_training",
                    )
                with DatabaseUnitOfWork(ledger_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(ledger_database, operation_id, action, payload, str(exc))
                raise

    def _stone_repository(self) -> StoneTrainingSqlRepository | TiantiTrainingRepository:
        return self.repository or StoneTrainingSqlRepository(self.game_database, self.player_database)

    def _breakthrough_repository(self) -> TiantiBreakthroughSqlRepository | TiantiTrainingRepository:
        return self.repository or TiantiBreakthroughSqlRepository(self.player_database)

    def _qiaoxue_repository(self) -> TiantiQiaoxueSqlRepository | TiantiTrainingRepository:
        return self.repository or TiantiQiaoxueSqlRepository(self.player_database)

    def _bath_repository(self) -> TiantiMedicineBathSqlRepository | TiantiTrainingRepository:
        return self.repository or TiantiMedicineBathSqlRepository(self.game_database, self.player_database)

    def _item_reward_repository(self) -> Any:
        if self.repository is not None and hasattr(self.repository, "apply_item_reward"):
            return self.repository
        from .repository import TiantiItemRewardSqlRepository
        return TiantiItemRewardSqlRepository(self.game_database, self.player_database)

    def grant_item_tianti(
        self,
        *,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        minutes: int,
        settled_at: datetime | None = None,
        sect_fairyland_level: int = 0,
    ) -> OperationOutcome[dict[str, Any]]:
        settled_at = settled_at or self.clock.now()
        if not isinstance(settled_at, datetime):
            raise ValidationError("settled_at must be a datetime")
        if not operation_id or not user_id or int(item_id) <= 0 or int(quantity) <= 0 or int(minutes) <= 0:
            raise ValidationError("operation_id, user_id, item_id, quantity and minutes are required")
        payload = {
            "user_id": str(user_id), "item_id": int(item_id),
            "quantity": int(quantity), "minutes": int(minutes),
            "settled_at": settled_at.isoformat(),
            "sect_fairyland_level": int(sect_fairyland_level),
        }
        action = "tianti.item_reward"
        with trace_context(operation_id=str(operation_id), user_scope=str(user_id)):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, str(operation_id), action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = self._item_reward_repository().apply(
                    str(operation_id), str(user_id), int(item_id), int(quantity), int(minutes),
                    settled_at=settled_at, sect_fairyland_level=int(sect_fairyland_level),
                )
                data = _result_data(raw)
                status = str(data.get("status", "failed"))
                if status in {"applied", "duplicate"}:
                    outcome = OperationOutcome.applied(
                        str(operation_id), action, data=data,
                        consumed={"item": {"item_id": int(item_id), "quantity": int(quantity)}},
                        granted={"tianti_hp": int(data.get("detail", {}).get("real_gain", 0) or 0)},
                        after={"tianti_hp": int(data.get("detail", {}).get("new_hp", 0) or 0)},
                        audit_category="tianti_training",
                    )
                else:
                    outcome = OperationOutcome.rejected(
                        str(operation_id), action,
                        {"item_insufficient": "物品数量不足，炼体时间奖励未结算。", "user_missing": "未找到修仙数据。"}.get(status, "炼体时间奖励未完成。"),
                        code=status, data=data, audit_category="tianti_training",
                    )
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, str(operation_id), action, payload, str(exc))
                raise

    def train(self, *, operation_id: str, user_id: str, requested_stone: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = StoneTrainingRequest(str(operation_id).strip(), str(user_id).strip(), int(requested_stone))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id,
            user_id=request.user_id,
            action="tianti.train",
            payload=request.payload(),
            ledger_database=self.game_database,
            call=lambda: self._stone_repository().train(request.operation_id, request.user_id, request.requested_stone),
            success_statuses={"trained", "duplicate"},
            messages={
                "at_cap": "已达当前炼体境界上限，无法继续灵石炼体。",
                "stone_insufficient": "灵石不足，请重新查看后再炼体。",
                "stone_changed": "灵石余额已更新，请重新查看后再炼体。",
                "user_missing": "未找到修仙数据。",
            },
            result_summary=lambda data: {
                "after": {"tianti_hp": int(data.get("new_hp", 0) or 0)},
                "consumed": {"stone": int(data.get("stone_cost", 0) or 0)},
                "granted": {"tianti_hp": int(data.get("hp_gain", 0) or 0)},
            },
        )

    def apply_bath(
        self,
        *,
        operation_id: str,
        user_id: str,
        consume_plan: Sequence[Mapping[str, Any]],
        effect: float,
        slot_name: str,
        started_at: datetime,
        duration_minutes: int,
        sect_fairyland_level: int = 0,
    ) -> OperationOutcome[dict[str, Any]]:
        if not isinstance(started_at, datetime):
            raise ValidationError("started_at must be a datetime")
        try:
            request = MedicineBathRequest(
                str(operation_id).strip(),
                str(user_id).strip(),
                normalize_plan(consume_plan),
                float(effect),
                str(slot_name),
                started_at,
                int(duration_minutes),
                int(sect_fairyland_level),
            )
            request.validate()
        except (TypeError, ValueError, KeyError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id,
            user_id=request.user_id,
            action="tianti.bath",
            payload=request.payload(),
            ledger_database=self.game_database,
            call=lambda: self._bath_repository().apply_bath(
                request.operation_id,
                request.user_id,
                request.consume_plan,
                request.effect,
                request.slot_name,
                request.started_at,
                request.duration_minutes,
                sect_fairyland_level=request.sect_fairyland_level,
            ),
            success_statuses={"applied", "duplicate"},
            messages={
                "bath_active": "当前药浴仍在生效，药浴结束后再使用新的药材。",
                "item_insufficient": "药材不足，请重新查看后再炼体。",
                "item_changed": "药材库存已更新，请重新查看后再炼体。",
                "user_missing": "未找到修仙数据。",
            },
        )

    def breakthrough(
        self,
        *,
        operation_id: str,
        user_id: str,
        cultivation_rank: int,
        roll_success: bool,
    ) -> OperationOutcome[dict[str, Any]]:
        try:
            request = BreakthroughRequest(
                str(operation_id).strip(), str(user_id).strip(), int(cultivation_rank), bool(roll_success)
            )
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id,
            user_id=request.user_id,
            action="tianti.breakthrough",
            payload=request.payload(),
            ledger_database=self.player_database,
            call=lambda: self._breakthrough_repository().breakthrough(
                request.operation_id,
                request.user_id,
                cultivation_rank=request.cultivation_rank,
                roll_success=request.roll_success,
            ),
            success_statuses={"completed", "duplicate"},
            messages={
                "max_level": "你的炼体已达最高境界。",
                "cultivation_insufficient": "修仙境界不足。",
                "hp_insufficient": "炼体气血不足。",
                "state_changed": "炼体状态已变化，请重新执行。",
            },
        )

    def open_qiaoxue(self, *, operation_id: str, user_id: str, roll: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = QiaoxueRequest(str(operation_id).strip(), str(user_id).strip(), int(roll))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id,
            user_id=request.user_id,
            action="tianti.qiaoxue",
            payload=request.payload(),
            ledger_database=self.player_database,
            call=lambda: self._qiaoxue_repository().open_qiaoxue(request.operation_id, request.user_id, request.roll),
            success_statuses={"opened", "duplicate"},
            messages={
                "limit_reached": "当前炼体境界无法继续开窍。",
                "hp_insufficient": "炼体气血不足，无法冲窍。",
                "state_changed": "炼体状态已变化，请重新执行。",
            },
        )

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "train"))
        outcome = getattr(self, action)(**kwargs)
        return ReplyPlan(outcome.message or outcome.data, reference=True)


__all__ = ["TiantiTrainingApplication"]
