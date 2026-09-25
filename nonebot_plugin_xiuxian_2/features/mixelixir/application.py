from __future__ import annotations

from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from ...core.errors import ConflictError, DomainError, ValidationError
from ...core.result import OperationOutcome, ReplyPlan
from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...infrastructure.observability import trace_context
from .domain import HarvestRequest, SettlementRequest, normalize_rewards
from .repository import MixelixirRepository
from .harvest_repository import MixelixirHarvestSqlRepository
from .settlement_repository import MixelixirSettlementSqlRepository
from .harvest_level_upgrade_repository import MixelixirHarvestLevelUpgradeSqlRepository
from .fire_control_upgrade_repository import MixelixirFireControlUpgradeSqlRepository
from .refine_cost_repository import MixelixirRefineCostSqlRepository
from .refine_reward_repository import MixelixirRefineRewardSqlRepository


def _data(raw: Any) -> dict[str, Any]:
    if is_dataclass(raw):
        return dict(asdict(raw))
    if isinstance(raw, Mapping):
        return dict(raw)
    return dict(vars(raw))


class MixelixirApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: MixelixirRepository | None = None, ledger: OperationLedger | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository
        self._explicit_repository = repository
        self.ledger = ledger or OperationLedger()

    def _execute(self, *, operation_id: str, user_id: str, action: str, payload: Mapping[str, Any], call, success_statuses: set[str], messages: Mapping[str, str], normalize):
        with trace_context(operation_id=operation_id, user_scope=user_id):
            try:
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    existing = self.ledger.begin(uow, operation_id, action, payload)
                    if existing is not None:
                        previous = existing.outcome()
                        if previous is not None:
                            return previous.replay()
                        raise ConflictError("操作正在处理中")
                raw = call()
                data = normalize(_data(raw))
                status = str(data.get("status", "failed"))
                if status in success_statuses:
                    outcome = OperationOutcome.applied(operation_id, action, data=data, granted=data.get("granted", {}), audit_category="mixelixir")
                else:
                    outcome = OperationOutcome.rejected(operation_id, action, messages.get(status, "炼丹操作未完成。"), code=status, data=data, audit_category="mixelixir")
                with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                    self.ledger.finish(uow, outcome)
                return outcome
            except DomainError:
                raise
            except Exception as exc:
                self.ledger.record_failure(self.game_database, operation_id, action, payload, str(exc))
                raise

    def _repository(self) -> MixelixirRepository:
        if self.repository is None:
            raise RuntimeError("legacy mixelixir repository requires explicit injection")
        return self.repository

    def harvest(self, *, operation_id: str, user_id: str, expected_last_time: str, harvested_at: str, rewards: Any, max_goods_num: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = HarvestRequest(str(operation_id).strip(), str(user_id).strip(), str(expected_last_time), str(harvested_at), normalize_rewards(rewards or ()), int(max_goods_num))
            request.validate()
        except (TypeError, ValueError, KeyError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id, user_id=request.user_id, action="mixelixir.harvest", payload=request.payload(),
            call=lambda: (MixelixirHarvestSqlRepository(self.game_database, self.player_database) if self._explicit_repository is None else self._repository()).harvest(request.operation_id, request.user_id, request.expected_last_time, request.harvested_at, request.rewards, max_goods_num=request.max_goods_num),
            success_statuses={"applied", "duplicate"},
            messages={"state_changed": "药材未发放：灵田状态已更新，请重新收取。", "user_missing": "未找到修仙数据。"},
            normalize=lambda data: {"status": data.get("status", "failed"), "harvested_at": str(data.get("harvested_at", request.harvested_at)), "rewards": data.get("rewards", []), "granted": {"herbs": data.get("rewards", [])}},
        )

    def settle(self, *, operation_id: str, user_id: str, materials: Mapping[int, int], reward_id: int, reward_name: str, reward_quantity: int, max_goods_num: int) -> OperationOutcome[dict[str, Any]]:
        try:
            request = SettlementRequest(str(operation_id).strip(), str(user_id).strip(), {int(k): int(v) for k, v in dict(materials or {}).items()}, int(reward_id), str(reward_name), int(reward_quantity), int(max_goods_num))
            request.validate()
        except (TypeError, ValueError, KeyError) as exc:
            raise ValidationError(str(exc)) from exc
        return self._execute(
            operation_id=request.operation_id, user_id=request.user_id, action="mixelixir.settle", payload=request.payload(),
            call=lambda: (MixelixirSettlementSqlRepository(self.game_database) if self._explicit_repository is None else self._repository()).settle(request.operation_id, request.user_id, request.materials, request.reward_id, request.reward_name, request.reward_quantity, max_goods_num=request.max_goods_num),
            success_statuses={"applied", "duplicate"},
            messages={"item_insufficient": "药材数量不足，本次未消耗药材。", "state_changed": "炼丹数据已更新，请重新提交。", "user_missing": "未找到修仙数据。"},
            normalize=lambda data: {"status": data.get("status", "failed"), "reward_quantity": int(data.get("reward_quantity", 0) or 0), "granted": {"elixir": int(data.get("reward_quantity", 0) or 0)}},
        )

    def harvest_level_upgrade(self, *, operation_id: str, user_id: str, current_level: int, experience: int, next_level: int, cost: int):
        from ...xiuxian.xiuxian_mixelixir.transaction_service import MixelixirHarvestLevelUpgradeService
        return self._execute(
            operation_id=operation_id,
            user_id=user_id,
            action="mixelixir.harvest_level_upgrade",
            payload={"current_level": current_level, "experience": experience, "next_level": next_level, "cost": cost},
            call=lambda: (MixelixirHarvestLevelUpgradeSqlRepository(self.game_database, self.player_database) if self._explicit_repository is None else MixelixirHarvestLevelUpgradeService(self.game_database, self.player_database)).upgrade(operation_id, user_id, current_level, experience, expected_stone=0, next_level=next_level, cost=cost) if self._explicit_repository is None else MixelixirHarvestLevelUpgradeService(self.game_database, self.player_database).upgrade(operation_id, user_id, current_level, experience, 0, next_level, cost),
            success_statuses={"applied", "duplicate"},
            messages={"experience_insufficient": "炼丹经验不足。", "state_changed": "炼丹数据已更新，请重新查看。"},
            normalize=lambda data: data,
        )

    def fire_control_upgrade(self, *, operation_id: str, user_id: str, current_level: int, experience: int, expected_stone: int, next_level: int, cost: int):
        return self._execute(
            operation_id=operation_id, user_id=user_id, action="mixelixir.fire_control_upgrade",
            payload={"current_level": current_level, "experience": experience, "next_level": next_level, "cost": cost},
            call=lambda: MixelixirFireControlUpgradeSqlRepository(self.game_database, self.player_database).upgrade(operation_id, user_id, current_level, experience, expected_stone, next_level, cost),
            success_statuses={"applied", "duplicate"},
            messages={"experience_insufficient": "炼丹经验不足。", "state_changed": "炼丹数据已更新，请重新查看。"},
            normalize=lambda data: data,
        )

    def refine_cost(
        self,
        *,
        operation_id: str,
        user_id: str,
        recipe_set_id: str,
        daily_count: int,
        expected_snapshot: dict,
        updated_mix_state: dict,
        max_goods_num: int,
        recipe_key: str,
        materials: Mapping[int, int],
        furnace_id: int,
        reward_id: int,
        reward_name: str,
        reward_quantity: int,
    ):
        return self._execute(
            operation_id=operation_id, user_id=user_id, action="mixelixir.refine_cost",
            payload={"recipe_set_id": recipe_set_id, "recipe_key": recipe_key},
            call=lambda: MixelixirRefineCostSqlRepository(self.game_database).start(
                operation_id, user_id, recipe_set_id, daily_count, expected_snapshot,
                updated_mix_state, max_goods_num, recipe_key=recipe_key, materials=materials,
                furnace_id=furnace_id, reward_id=reward_id, reward_name=reward_name,
                reward_quantity=reward_quantity,
            ),
            success_statuses={"applied", "duplicate"},
            messages={
                "item_insufficient": "炼丹材料不足。",
                "limit_reached": "道友今日炼丹已达上限，请明日再来！",
                "operation_conflict": "炼丹请求冲突，请重新提交。",
                "schema_missing": "炼丹数据尚未就绪，请稍后重试。",
                "state_changed": "炼丹配方状态已更新，请重新提交。",
                "user_missing": "未找到修仙数据。",
            },
            normalize=lambda data: data,
        )

    def refine_reward(self, *, operation_id: str, user_id: str, task_id: str, max_goods_num: int):
        return self._execute(
            operation_id=operation_id, user_id=user_id, action="mixelixir.refine_reward",
            payload={"task_id": task_id},
            call=lambda: MixelixirRefineRewardSqlRepository(self.game_database, self.player_database).claim(operation_id, user_id, task_id, max_goods_num),
            success_statuses={"applied", "duplicate"},
            messages={
                "inventory_full": "丹药背包已满。",
                "operation_conflict": "炼丹领取请求冲突。",
                "schema_missing": "炼丹数据尚未就绪，请稍后重试。",
                "task_missing": "炼丹任务不存在。",
                "state_changed": "炼丹任务状态已更新。",
                "user_missing": "未找到修仙数据。",
            },
            normalize=lambda data: {
                **data,
                "granted": {"elixir": int(data.get("reward_quantity", 0) or 0)}
                if data.get("status") in {"applied", "duplicate"}
                else {},
            },
        )

    def latest_ready_refine_task(self, *, user_id: str, recipe_key: str | None = None) -> str | None:
        return MixelixirRefineRewardSqlRepository(
            self.game_database, self.player_database
        ).latest_ready_task(user_id, recipe_key)

    def reply(self, **kwargs: Any):
        action = str(kwargs.pop("action", "harvest"))
        return ReplyPlan(getattr(self, action)(**kwargs).data, reference=True)


__all__ = ["MixelixirApplication"]
