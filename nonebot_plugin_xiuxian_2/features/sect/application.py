from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.clock import SystemClock
from ...infrastructure.random_source import SystemRandom
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
from .elixir_room_repository import SectElixirRoomSqlRepository
from .buff_search_repository import SectBuffSearchSqlRepository
from .practice_repository import SectPracticeSqlRepository
from .task_settlement_repository import SectTaskSettlementSqlRepository
from .creation_repository import SectCreationSqlRepository
from .name_refresh_repository import SectNameRefreshSqlRepository
from .weekly_reward_repository import SectWeeklyRewardRepository, SectWeeklyRewardSqlRepository
from .weekly_progress_repository import SectWeeklyProgressSqlRepository
from .manual_disband_repository import SectManualDisbandSqlRepository
from .activity_repository import SectActivitySqlRepository
from .directory_repository import SectDirectorySqlRepository
from .inactive_owner_repository import SectInactiveOwnerSqlRepository
from .sect_info_repository import SectInfoSqlRepository
from .member_repository import SectMemberSqlRepository
from .task_state_repository import SectTaskStateSqlRepository


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
        return self.status in {"closed", "inherited", "disbanded", "duplicate", "upgraded", "granted"}

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class SectApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: SectRepository | None = None,
        weekly_repository: SectWeeklyRewardRepository | None = None,
        weekly_progress_repository: SectWeeklyProgressSqlRepository | None = None,
        task_state_repository: SectTaskStateSqlRepository | None = None,
        player_database: str | Path | None = None,
        ledger: OperationLedger | None = None,
        clock=None,
        random_source=None,
    ) -> None:
        self.database = str(database)
        self.player_database = str(player_database) if player_database is not None else None
        self.repository = repository
        self.weekly_repository = weekly_repository
        self.weekly_progress_repository = weekly_progress_repository
        self.ledger = ledger or OperationLedger()
        self.clock = clock or SystemClock()
        self.random = random_source or SystemRandom()
        self.activity_repository = SectActivitySqlRepository(self.database)
        self.directory_repository = SectDirectorySqlRepository(self.database)
        self.inactive_owner_repository = SectInactiveOwnerSqlRepository(self.database)
        self.sect_info_repository = SectInfoSqlRepository(self.database)
        self.member_repository = SectMemberSqlRepository(self.database)
        self.scheduled_material_repository = SectScheduledMaterialSqlRepository(self.database)
        self.task_state_repository = task_state_repository or SectTaskStateSqlRepository(
            self.database
        )

    def list_sects_with_member_count(self) -> list[tuple[Any, ...]]:
        return self.directory_repository.list_with_member_count()

    def list_active_sect_names(self) -> list[str | None]:
        return self.directory_repository.list_active_sect_names()

    def list_sect_scale_rank(self) -> list[tuple[Any, ...]]:
        return self.directory_repository.list_scale_rank()

    def list_sect_combat_power_rank(self) -> list[tuple[Any, ...]]:
        return self.directory_repository.list_combat_power_rank()

    def get_sect_info(self, sect_id: int | str) -> dict[str, Any] | None:
        return self.sect_info_repository.get_by_id(sect_id)

    def get_sect_id_by_name(self, sect_name: str) -> int | str | None:
        return self.sect_info_repository.get_id_by_name(sect_name)

    def get_inactive_owner_sect_state(self, sect_id: int) -> dict[str, Any] | None:
        return self.inactive_owner_repository.get_sect_state(sect_id)

    def list_sect_members(self, sect_id: int | str) -> list[dict[str, Any]]:
        return self.member_repository.list_by_sect_id(sect_id)

    def get_user_profile(self, user_id: int | str) -> dict[str, Any] | None:
        return self.member_repository.get_user_profile(user_id)

    def get_user_profile_by_name(self, user_name: str) -> dict[str, Any] | None:
        return self.member_repository.get_user_profile_by_name(user_name)

    def _task_now(self):
        value = self.clock.now()
        return value.astimezone() if value.tzinfo is not None else value

    def current_task_period(self) -> str:
        return self._task_now().strftime("%Y-%m-%d")

    def get_active_task(self, user_id: str | int) -> dict[str, Any] | None:
        return self.task_state_repository.get_active_task(
            user_id, self.current_task_period()
        )

    def accept_task(
        self,
        user_id: str | int,
        sect_id: str | int,
        task_config: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        task_key = self.random.choice(list(task_config))
        task_data = dict(task_config[task_key])
        now = self._task_now()
        return self.task_state_repository.accept_task(
            user_id,
            sect_id,
            task_key,
            task_data,
            now.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d %H:%M:%S"),
        )

    def complete_task(self, user_id: str | int) -> None:
        now = self._task_now()
        self.task_state_repository.complete_task(
            user_id,
            now.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d %H:%M:%S"),
        )

    def clear_task(self, user_id: str | int) -> None:
        self.task_state_repository.clear_task(user_id, self.current_task_period())

    def _weekly_progress_repository(self) -> SectWeeklyProgressSqlRepository:
        if self.weekly_progress_repository is None:
            self.weekly_progress_repository = SectWeeklyProgressSqlRepository(self.database)
        return self.weekly_progress_repository

    def assert_weekly_progress_schema(self) -> None:
        self._weekly_progress_repository().assert_schema_ready()

    def ensure_weekly_goals(
        self,
        sect_id: int | str,
        week_key: str,
        goals: Iterable[Mapping[str, Any]],
        updated_at: str,
    ) -> None:
        self._weekly_progress_repository().ensure_goals(sect_id, week_key, goals, updated_at)

    def list_weekly_goal_rows(
        self,
        sect_id: int | str,
        week_key: str,
        goals: Iterable[Mapping[str, Any]],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        return self._weekly_progress_repository().list_goals(sect_id, week_key, goals, updated_at)

    def record_weekly_progress(
        self,
        sect_id: int | str,
        week_key: str,
        user_id: int | str,
        amount: int,
        goals: Iterable[Mapping[str, Any]],
        updated_at: str,
    ) -> list[dict[str, Any]]:
        return self._weekly_progress_repository().record_progress(
            sect_id, week_key, user_id, amount, goals, updated_at
        )

    def weekly_rank(self, limit: int, week_key: str) -> list[dict[str, Any]]:
        return self._weekly_progress_repository().weekly_rank(limit, week_key)

    def get_inactive_owner_user_profile(self, owner_id: str) -> dict[str, Any] | None:
        return self.inactive_owner_repository.get_owner_profile(owner_id)

    def update_last_check_info_time(self, user_id: str) -> int:
        occurred_at = self.clock.now()
        if occurred_at.tzinfo is not None:
            # Legacy activity readers only accept local, timezone-naive timestamps.
            occurred_at = occurred_at.astimezone().replace(tzinfo=None)
        return self.activity_repository.update_last_check_info_time(
            str(user_id), occurred_at.isoformat(sep=" ")
        )

    def get_last_check_info_time(self, user_id: str):
        return self.activity_repository.get_last_check_info_time(str(user_id))

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

    def disband(self, operation_id: str, actor_id: str, *, expected_sect_id: int | None = None, owner_position: int = 0):
        return SectMutationResult(SectManualDisbandSqlRepository(self.database).disband(operation_id, actor_id, expected_sect_id=expected_sect_id, owner_position=owner_position))

    def transfer_owner(self, operation_id: str, actor_id: str, target_id: str, *, owner_position: int = 0, former_owner_position: int | None = None):
        return SectMutationResult(SectOwnerTransferSqlRepository(self.database).transfer(operation_id, actor_id, target_id, owner_position=owner_position, former_owner_position=former_owner_position))

    def grant_scheduled_materials(self, operation_id: str, sect_id: int, multiplier: int):
        return SectMutationResult(self.scheduled_material_repository.grant(operation_id, sect_id, multiplier))

    def list_scheduled_material_targets(self) -> list[tuple[Any, ...]]:
        return self.scheduled_material_repository.list_targets()

    def upgrade_fairyland(self, operation_id: str, actor_id: str, sect_id: int, expected_level: int, next_level: int, stone_cost: int, materials_cost: int, *, owner_position: int = 0):
        return SectMutationResult(SectFairylandSqlRepository(self.database).upgrade(operation_id, actor_id, sect_id, expected_level, next_level, stone_cost, materials_cost, owner_position=owner_position))

    def upgrade_elixir_room(self, operation_id: str, actor_id: str, sect_id: int, expected_level: int, next_level: int, stone_cost: int, scale_cost: int, *, owner_position: int = 0):
        return SectMutationResult(SectElixirRoomSqlRepository(self.database).upgrade(operation_id, actor_id, sect_id, expected_level, next_level, stone_cost, scale_cost, owner_position=owner_position))

    def apply_buff_search(self, operation_id: str, actor_id: str, sect_id: int, buff_type: str, previous_value: str, new_value: str, stone_cost: int, materials_cost: int):
        return SectMutationResult(SectBuffSearchSqlRepository(self.database).apply(operation_id, actor_id, sect_id, buff_type, previous_value, new_value, stone_cost, materials_cost))

    def upgrade_practice(self, operation_id: str, user_id: str, sect_id: int, practice_type: str, expected_level: int, next_level: int, stone_cost: int, materials_cost: int):
        return SectMutationResult(SectPracticeSqlRepository(self.database).upgrade(operation_id, user_id, sect_id, practice_type, expected_level, next_level, stone_cost, materials_cost))

    def settle_task(self, operation_id: str, user_id: str, sect_id: int, period: str, cost_type: str, cost: int, exp_reward: int, sect_reward: int, expected_task_key=None, expected_task_data=None):
        return SectMutationResult(SectTaskSettlementSqlRepository(self.database).settle(operation_id, user_id, sect_id, period, cost_type, cost, exp_reward, sect_reward, expected_task_key, expected_task_data))

    def create_sect(self, operation_id: str, user_id: str, sect_name: str, stone_cost: int, owner_position: int):
        return SectMutationResult(SectCreationSqlRepository(self.database).create(operation_id, user_id, sect_name, stone_cost, owner_position))

    def charge_name_refresh(self, operation_id: str, user_id: str, stone_cost: int):
        return SectMutationResult(SectNameRefreshSqlRepository(self.database).charge(operation_id, user_id, stone_cost))

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

    def claim_weekly(
        self,
        *,
        operation_id: str,
        user_id: str,
        sect_id: int,
        week_key: str,
        goals: Any,
        max_goods_num: int,
    ) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        try:
            sect_id = int(sect_id)
            week_key = str(week_key).strip()
            max_goods_num = int(max_goods_num)
            goals = [dict(goal) for goal in (goals or ())]
        except (TypeError, ValueError) as exc:
            raise ValidationError("weekly reward claim values are invalid") from exc
        if not operation_id or not user_id or sect_id <= 0 or not week_key or not goals or max_goods_num < 0:
            raise ValidationError("operation_id, user_id, sect_id and week_key are required")
        payload = {
            "user_id": user_id,
            "sect_id": sect_id,
            "week_key": week_key,
            "goals": goals,
            "max_goods_num": max_goods_num,
        }
        repository = self.weekly_repository
        if repository is None:
            if self.player_database is None:
                raise ValidationError("player_database is required for weekly reward claims")
            repository = SectWeeklyRewardSqlRepository(self.database, self.player_database, clock=self.clock)
            self.weekly_repository = repository
        # The repository receipt commits with both databases; an outer ledger could strand "started" after a crash.
        action = "sect.weekly_reward_claim"
        with trace_context(operation_id=operation_id, user_scope=user_id):
            raw = _data(
                repository.claim(
                    operation_id,
                    user_id,
                    sect_id,
                    week_key,
                    payload["goals"],
                    max_goods_num,
                )
            )
            status = str(raw.get("status", "failed"))
            data = {"status": status, **raw}
            if status in {"applied", "duplicate"}:
                return OperationOutcome.applied(
                    operation_id,
                    action,
                    data=data,
                    granted={"rewards": data.get("rewards", ())},
                    audit_category="sect",
                    replayed=status == "duplicate",
                    clock=self.clock,
                )
            return OperationOutcome.rejected(
                operation_id,
                action,
                "宗门周常奖励未完成。",
                code=status,
                data=data,
                audit_category="sect",
                clock=self.clock,
            )

    def reply(self, **kwargs: Any) -> ReplyPlan:
        action = str(kwargs.pop("action", "join"))
        result = getattr(self, action)(**kwargs)
        return ReplyPlan(result.data, reference=True)


__all__ = ["SectApplication"]
