from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import PetFeedRequest, PetTravelClaimRequest
from .repository import PetActiveSwitchSqlRepository, PetFeedSqlRepository, PetFusionBreakthroughSqlRepository, PetHatchSqlRepository, PetReleaseSqlRepository, PetRepository, PetSkillRerollSqlRepository, PetTravelClaimSqlRepository, PetTravelStartSqlRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class PetApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: PetRepository | None = None, ledger: OperationLedger | None = None, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.clock = clock


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
        if self.repository is None:
            repository = PetTravelClaimSqlRepository(self.game_database, self.player_database)
            call = lambda: repository.claim(request.operation_id, request.user_id, request.expected_travel, request.stone, request.exp, request.items, request.max_goods_num)
        else:
            call = lambda: self.repository.travel_claim(request.operation_id, request.user_id, request.expected_travel, request.stone, request.exp, request.items, request.max_goods_num)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="pet.travel_claim", payload=request.payload(), call=call)

    def start_travel(self, *, operation_id: str, user_id: str, pet_uid: str, expected_travel: Mapping[str, Any] | None, travel: Mapping[str, Any]) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip() or not str(pet_uid).strip() or not isinstance(travel, Mapping):
            raise ValidationError("operation_id, user_id, pet_uid and travel are required")
        payload = {"user_id": str(user_id), "pet_uid": str(pet_uid), "expected_travel": expected_travel, "travel": dict(travel)}
        if self.repository is None:
            repository = PetTravelStartSqlRepository(self.player_database)
            call = lambda: repository.start(operation_id, user_id, pet_uid, expected_travel, travel)
        else:
            call = lambda: self.repository.travel_start(operation_id, user_id, expected_travel, travel)
        return self._execute(operation_id=str(operation_id).strip(), user_id=str(user_id), action="pet.travel_start", payload=payload, call=call)

    def feed(self, *, operation_id: str, user_id: str, uid: str, item_id: int, count: int, expected: Sequence[int], updated: Sequence[int]) -> OperationOutcome[dict[str, Any]]:
        try:
            request = PetFeedRequest(str(operation_id).strip(), str(user_id).strip(), str(uid), int(item_id), int(count), tuple(int(value) for value in expected), tuple(int(value) for value in updated))
            request.validate()
        except (TypeError, ValueError) as exc:
            raise ValidationError(str(exc)) from exc
        repository = self.repository or PetFeedSqlRepository(self.game_database, self.player_database)
        return self._execute(operation_id=request.operation_id, user_id=request.user_id, action="pet.feed", payload=request.payload(), call=lambda: repository.feed(request.operation_id, request.user_id, request.uid, request.item_id, request.count, request.expected, request.updated))

    def hatch(self, *, operation_id: str, user_id: str, expected_stone: int, cost: int, expected_meta: Sequence[Any], pets: Sequence[Any], updated_meta: Sequence[Any], bag_limit: int) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip() or min(int(expected_stone), int(cost), int(bag_limit)) < 0:
            raise ValidationError("hatch operation and values are invalid")
        payload = {"user_id": str(user_id), "cost": int(cost), "count": len(pets)}
        if self.repository is None:
            repository = PetHatchSqlRepository(self.game_database, self.player_database, clock=self.clock)
            call = lambda: repository.hatch(operation_id, user_id, expected_stone, cost, expected_meta, pets, updated_meta, bag_limit)
        else:
            call = lambda: self.repository.hatch(operation_id, user_id, expected_stone, cost, expected_meta, pets, updated_meta, bag_limit)
        return self._execute(operation_id=str(operation_id).strip(), user_id=str(user_id), action="pet.hatch", payload=payload, call=call)

    def hatch_result(self, *, operation_id: str) -> Any:
        if self.repository is None:
            return PetHatchSqlRepository(self.game_database, self.player_database, clock=self.clock).get_result(operation_id)
        return self.repository.hatch_result(operation_id)

    def release(self, *, operation_id: str, user_id: str, uid: str, expected_exp: int, refund_item: int, refund_name: str, refund_type: str, refund: int, max_goods: int, expected_is_active: bool = True) -> Any:
        return PetReleaseSqlRepository(self.game_database, self.player_database).release(
            operation_id, user_id, uid, expected_exp, refund_item, refund_name, refund_type, refund, max_goods, expected_is_active
        )

    def release_batch(self, *, operation_id: str, user_id: str, expected_pets: Sequence[Mapping[str, Any]], refund_item: int, refund_name: str, refund_type: str, refund: int, max_goods: int) -> Any:
        return PetReleaseSqlRepository(self.game_database, self.player_database).release_batch(
            operation_id, user_id, expected_pets, refund_item, refund_name, refund_type, refund, max_goods
        )

    def fusion_breakthrough(self, *, operation_id: str, user_id: str, expected_main: Sequence[Any], expected_materials: Sequence[Sequence[Any]], updated_stars: int, updated_exp: int, skill_offer: Any = None) -> Any:
        return PetFusionBreakthroughSqlRepository(self.player_database).breakthrough(
            operation_id, user_id, expected_main, expected_materials, updated_stars, updated_exp, skill_offer
        )

    def skill_reroll(self, *, operation_id: str, user_id: str, expected_pet: Sequence[Any], new_skill_id: str, item_id: int) -> Any:
        return PetSkillRerollSqlRepository(self.game_database, self.player_database).reroll(
            operation_id, user_id, expected_pet, new_skill_id, item_id
        )

    def switch(self, *, operation_id: str, user_id: str, expected_active_uid: str, target_uid: str, travel_pet_uid: str = "") -> Any:
        repository = self.repository or PetActiveSwitchSqlRepository(self.player_database)
        return repository.switch(operation_id, user_id, expected_active_uid, target_uid, travel_pet_uid)

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "claim_travel"))
        result = getattr(self, action)(**kwargs)
        return ReplyPlan(result.data if isinstance(result, OperationOutcome) else _data(result), reference=True)


__all__ = ["PetApplication"]
