from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import PetFeedRequest, PetTravelClaimRequest
from .repository import LegacyPetRepository, PetRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class PetApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: PetRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()

    def _repository(self) -> PetRepository:
        return self.repository or LegacyPetRepository(self.game_database, self.player_database)

    def _execute(self, *, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any], call) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = _data(call())
                status = str(raw.get("status", "failed"))
                data = {"status": status, **raw}
                if status in {"applied", "duplicate"}:
                    granted = {key: raw[key] for key in ("stone", "exp", "items", "pets") if key in raw}
                    outcome = OperationOutcome.applied(operation_id, action, data=data, granted=granted, audit_category="pet")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, "宠物操作未完成。", code=status, data=data, audit_category="pet")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def claim_travel(self, *, operation_id: str, user_id: str, expected_travel: Mapping[str, Any], stone: int, exp: int, items: Sequence[Mapping[str, Any]], max_goods_num: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = PetTravelClaimRequest(str(operation_id).strip(), str(user_id).strip(), dict(expected_travel or {}), int(stone), int(exp), tuple(dict(item) for item in (items or ())), int(max_goods_num))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="pet.travel_claim", payload=request.payload(), call=lambda: self._repository().travel_claim(request.operation_id, request.user_id, request.expected_travel, request.stone, request.exp, request.items, request.max_goods_num))

    def start_travel(self, *, operation_id: str, user_id: str, pet_uid: str, expected_travel: Mapping[str, Any] | None, travel: Mapping[str, Any]) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip() or not str(pet_uid).strip() or not isinstance(travel, Mapping):
            raise ValidationError("operation_id, user_id, pet_uid and travel are required")
        payload = {"user_id": str(user_id), "pet_uid": str(pet_uid), "expected_travel": expected_travel, "travel": dict(travel)}
        return self._execute(operation_id=str(operation_id).strip(), user_id=str(user_id), action="pet.travel_start", payload=payload, call=lambda: self._repository().travel_start(operation_id, user_id, expected_travel, travel))

    def feed(self, *, operation_id: str, user_id: str, uid: str, item_id: int, count: int, expected: Sequence[int], updated: Sequence[int]) -> OperationOutcome[dict[str, Any]]:
        try:
            request = PetFeedRequest(str(operation_id).strip(), str(user_id).strip(), str(uid), int(item_id), int(count), tuple(int(value) for value in expected), tuple(int(value) for value in updated))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="pet.feed", payload=request.payload(), call=lambda: self._repository().feed(request.operation_id, request.user_id, request.uid, request.item_id, request.count, request.expected, request.updated))

    def hatch(self, *, operation_id: str, user_id: str, expected_stone: int, cost: int, expected_meta: Sequence[Any], pets: Sequence[Any], updated_meta: Sequence[Any], bag_limit: int) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip() or min(int(expected_stone), int(cost), int(bag_limit)) < 0:
            raise ValidationError("hatch operation and values are invalid")
        payload = {"user_id": str(user_id), "cost": int(cost), "count": len(pets)}
        return self._execute(operation_id=str(operation_id).strip(), user_id=str(user_id), action="pet.hatch", payload=payload, call=lambda: self._repository().hatch(operation_id, user_id, expected_stone, cost, expected_meta, pets, updated_meta, bag_limit))

    def hatch_result(self, *, operation_id: str) -> Any:
        return self._repository().hatch_result(operation_id)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "claim_travel"))
        result = getattr(self, action)(**kwargs)
        return ReplyPlan(result.data if isinstance(result, OperationOutcome) else _data(result), reference=True)


__all__ = ["PetApplication"]
