from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.clock import SystemClock
from ...infrastructure.observability import trace_context
from .repository import SectRenameSqlRepository, SectRepository
from .daily_maintenance_repository import SectDailyMaintenanceSqlRepository
from .close_mountain_repository import SectCloseMountainSqlRepository
from .owner_inherit_repository import SectOwnerInheritSqlRepository
from .join_state_repository import SectJoinStateSqlRepository
from .disband_repository import SectDisbandSqlRepository
from .owner_transfer_repository import SectOwnerTransferSqlRepository
from .scheduled_material_repository import SectScheduledMaterialSqlRepository
from .fairyland_repository import SectFairylandSqlRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class SectMutationResult(dict):
    @property
    def status(self) -> str:
        return str(self.get("status", ""))

    @property
    def applied(self) -> bool:
        return self.status in {"closed", "inherited", "duplicate"}

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class SectApplication:
    def __init__(self, database: str | Path, *, repository: SectRepository | None = None, ledger: OperationLedger | None = None, clock=None) -> None:
        self.database = str(database)
        self.repository = repository
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()

    def _repository(self) -> SectRepository:
        return self.repository or SectRenameSqlRepository(self.database, clock=self.clock)

    def rename(self, *, operation_id: str, user_id: str, sect_id: int, new_name: str, cost: int, card_id: int) -> OperationOutcome[dict[str, Any]]:
        payload = {"user_id": str(user_id), "sect_id": int(sect_id), "new_name": str(new_name), "cost": int(cost), "card_id": int(card_id)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.rename", payload=payload, call=lambda: self._repository().rename(operation_id, user_id, sect_id, new_name, cost, card_id))

    def leave(self, *, operation_id: str, user_id: str, owner_position: int = 0) -> OperationOutcome[dict[str, Any]]:
        payload = {"user_id": str(user_id), "owner_position": int(owner_position)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.leave", payload=payload, call=lambda: self._repository().leave(operation_id, user_id, owner_position=owner_position))

    def kick(self, *, operation_id: str, user_id: str, target_id: str, manager_max_position: int) -> OperationOutcome[dict[str, Any]]:
        payload = {"user_id": str(user_id), "target_id": str(target_id), "manager_max_position": int(manager_max_position)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.kick", payload=payload, call=lambda: self._repository().kick(operation_id, user_id, target_id, manager_max_position=manager_max_position))

    def change_position(self, *, operation_id: str, user_id: str, target_id: str, requested_position: int, position_limits: Mapping[str, Any], manager_max_position: int) -> OperationOutcome[dict[str, Any]]:
        payload = {"user_id": str(user_id), "target_id": str(target_id), "requested_position": int(requested_position), "position_limits": dict(position_limits), "manager_max_position": int(manager_max_position)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.position_change", payload=payload, call=lambda: self._repository().change_position(operation_id, user_id, target_id, requested_position, position_limits, manager_max_position=manager_max_position))

    def donate(self, *, operation_id: str, user_id: str, sect_id: int, stone: int, materials: int) -> OperationOutcome[dict[str, Any]]:
        payload = {"user_id": str(user_id), "sect_id": int(sect_id), "stone": int(stone), "materials": int(materials)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.donate", payload=payload, call=lambda: self._repository().donate(operation_id, user_id, sect_id, stone, materials))

    def reset_daily_maintenance(self, business_date: str, maintenance_costs: Mapping[int, int]):
        return SectDailyMaintenanceSqlRepository(self.database).settle(business_date, dict(maintenance_costs))

    def close_mountain(self, operation_id: str, actor_id: str, *, owner_position: int = 0, former_owner_position: int = 2, expected_sect_id: int | None = None):
        return SectMutationResult(SectCloseMountainSqlRepository(self.database).close(operation_id, actor_id, owner_position=owner_position, former_owner_position=former_owner_position, expected_sect_id=expected_sect_id))

    def inherit_owner(self, operation_id: str, actor_id: str, *, expected_sect_id: int | None = None, eligible_positions=(1, 2, 6, 7), eligible_user_ids=None, owner_position: int = 0):
        return SectMutationResult(SectOwnerInheritSqlRepository(self.database).inherit(operation_id, actor_id, expected_sect_id=expected_sect_id, eligible_positions=eligible_positions, eligible_user_ids=eligible_user_ids, owner_position=owner_position))

    def open_join(self, operation_id: str, actor_id: str, *, owner_position: int = 0, expected_sect_id: int | None = None):
        return SectMutationResult(SectJoinStateSqlRepository(self.database).open(operation_id, actor_id, owner_position=owner_position, expected_sect_id=expected_sect_id))

    def close_join(self, operation_id: str, actor_id: str, *, owner_position: int = 0, expected_sect_id: int | None = None):
        return SectMutationResult(SectJoinStateSqlRepository(self.database).close(operation_id, actor_id, owner_position=owner_position, expected_sect_id=expected_sect_id))

    def disband_inactive(self, operation_id: str, sect_id: int, reason: str, *, expected_sect_name: str, expected_owner_id: str | None, expected_closed: bool, expected_member_ids, expected_active_candidate_ids, checked_at, inactivity_days: int):
        return SectMutationResult(SectDisbandSqlRepository(self.database).disband_inactive(operation_id, sect_id, reason, expected_sect_name=expected_sect_name, expected_owner_id=expected_owner_id, expected_closed=expected_closed, expected_member_ids=expected_member_ids, expected_active_candidate_ids=expected_active_candidate_ids, checked_at=checked_at, inactivity_days=inactivity_days))

    def transfer_owner(self, operation_id: str, actor_id: str, target_id: str, *, owner_position: int = 0, former_owner_position: int | None = None):
        return SectMutationResult(SectOwnerTransferSqlRepository(self.database).transfer(operation_id, actor_id, target_id, owner_position=owner_position, former_owner_position=former_owner_position))

    def grant_scheduled_materials(self, operation_id: str, sect_id: int, multiplier: int):
        return SectMutationResult(SectScheduledMaterialSqlRepository(self.database).grant(operation_id, sect_id, multiplier))

    def upgrade_fairyland(self, operation_id: str, actor_id: str, sect_id: int, expected_level: int, next_level: int, stone_cost: int, materials_cost: int, *, owner_position: int = 0):
        return SectMutationResult(SectFairylandSqlRepository(self.database).upgrade(operation_id, actor_id, sect_id, expected_level, next_level, stone_cost, materials_cost, owner_position=owner_position))

    def _execute(self, *, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any], call) -> OperationOutcome[dict[str, Any]]:
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = _data(call())
                status = str(raw.get("status", "failed"))
                data = {"status": status, **raw}
                if status in {"applied", "joined", "learned", "duplicate"}:
                    outcome = OperationOutcome.applied(operation_id, action, data=data, audit_category="sect")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, "宗门操作未完成。", code=status, data=data, audit_category="sect")
                with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.database, operation_id, action, payload, str(exc))
                raise

    def join(self, *, operation_id: str, user_id: str, sect_id: int, member_position: int = 12) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip() or int(sect_id) <= 0:
            raise ValidationError("operation_id, user_id and sect_id are required")
        payload = {"user_id": str(user_id), "sect_id": int(sect_id), "member_position": int(member_position)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.join", payload=payload, call=lambda: self._repository().join(operation_id, user_id, sect_id, member_position=member_position))

    def purchase(self, *, operation_id: str, user_id: str, **kwargs: Any) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip():
            raise ValidationError("operation_id and user_id are required")
        payload = {"user_id": str(user_id), **kwargs}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.purchase", payload=payload, call=lambda: self._repository().purchase(operation_id, user_id, **kwargs))

    def learn_main(self, *, operation_id: str, user_id: str, **kwargs: Any) -> OperationOutcome[dict[str, Any]]:
        return self._learn(operation_id=operation_id, user_id=user_id, action="sect.learn_main", method="learn_main", kwargs=kwargs)

    def learn_secondary(self, *, operation_id: str, user_id: str, **kwargs: Any) -> OperationOutcome[dict[str, Any]]:
        return self._learn(operation_id=operation_id, user_id=user_id, action="sect.learn_secondary", method="learn_secondary", kwargs=kwargs)

    def _learn(self, *, operation_id: str, user_id: str, action: str, method: str, kwargs: Mapping[str, Any]) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip():
            raise ValidationError("operation_id and user_id are required")
        payload = {"user_id": str(user_id), **dict(kwargs)}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action=action, payload=payload, call=lambda: getattr(self._repository(), method)(operation_id, user_id, **dict(kwargs)))

    def claim_elixir(self, *, operation_id: str, user_id: str, **kwargs: Any) -> OperationOutcome[dict[str, Any]]:
        if not str(operation_id).strip() or not str(user_id).strip():
            raise ValidationError("operation_id and user_id are required")
        payload = {"user_id": str(user_id), **kwargs}
        return self._execute(operation_id=str(operation_id), user_id=str(user_id), action="sect.claim_elixir", payload=payload, call=lambda: self._repository().claim_elixir(operation_id, user_id, **kwargs))

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "join"))
        result = getattr(self, action)(**kwargs)
        return ReplyPlan(result.data, reference=True)


__all__ = ["SectApplication"]
