from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import replace
import json
from pathlib import Path
from typing import Any, Protocol

from .._legacy_application import LegacyApplication
from ..combat_settlement.application import CombatSettlementApplication
from ...core.result import OperationOutcome
from ...infrastructure.database import DatabaseUnitOfWork
from .schemas import MapNearbyTargetResult
from .nearby_display_query import select_nearby_display
from .nearby_display_repository import MapNearbyDisplaySqlQueryRepository
from .random_target_query import select_random_nearby_target
from .random_target_repository import MapRandomTargetSqlQueryRepository
from .repository import LegacyMapRepository, MapCombatLifecyclePlanSqlRepository, MapCombatLifecycleQueryRepository, MapCombatLifecycleStartSqlRepository, MapDongfuBuildSqlRepository, MapDongfuSqlQueryRepository, MapExploreSettlementSqlRepository, MapExploreStartSqlRepository, MapExploreStatusSqlQueryRepository, MapExploreStatusSqlWriteRepository, MapMissionClaimSqlRepository, MapMissionSqlQueryRepository, MapMissionSqlWriteRepository, MapNearbyPlayersSqlQueryRepository, MapProjectionSqlRepository, MapProjectionSqlWriteRepository, MapSeedPurchaseSqlRepository, MapHomeReturnSqlRepository, MapInteractiveFailureSqlRepository, MapInteractiveSettlementSqlRepository, MapInteractiveSqlQueryRepository, MapInteractiveStartSqlRepository, MapMovementSqlRepository, MapResourceRewardSqlRepository, MapStatusSqlQueryRepository, MapStatusSqlWriteRepository, MapRepository

class MapCombatRunner(Protocol):
    def __call__(
        self,
        user_id: str,
        enemy: dict[str, Any],
        *,
        bot_id: Any,
    ) -> Awaitable[tuple[Any, str, dict[str, Any]]]: ...


class MapApplication(LegacyApplication):
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: MapRepository | None = None,
        combat_runner: MapCombatRunner | None = None,
        game_event_effects: Any | None = None,
    ) -> None:
        super().__init__(game_database, repository=repository, feature="map")
        self._explicit_repository = repository
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self._combat_runner = combat_runner
        self.game_event_effects = game_event_effects
        self._combat_settlement_application = CombatSettlementApplication(
            self.game_database,
            self.player_database,
        )

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"map.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def move(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.move",
                payload={"user_id": user_id, **kwargs},
                call=lambda: MapMovementSqlRepository(self.game_database, self.player_database).move(operation_id, user_id, **kwargs),
            )
        return self._action("move", operation_id=operation_id, user_id=user_id, **kwargs)
    def return_home(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.return_home",
                payload={"user_id": user_id, **kwargs},
                call=lambda: MapHomeReturnSqlRepository(self.player_database).return_home(operation_id, user_id),
            )
        return self._action("return_home", operation_id=operation_id, user_id=user_id, **kwargs)

    def get_active(self, user_id: str) -> dict[str, Any] | None:
        if self._explicit_repository is None:
            return MapInteractiveSqlQueryRepository(self.player_database, self.game_database).get_active(user_id)
        return self.repository.get_active(user_id)

    def interactive_replay(self, operation_id: str, user_id: str, action_type: str):
        return MapInteractiveSqlQueryRepository(self.player_database, self.game_database).replay_start(operation_id, user_id, action_type)

    def daily_limit(self, user_id: str, today: str) -> dict[str, int | str]:
        return MapProjectionSqlRepository(self.player_database).daily_limit(user_id, today)

    def save_daily_limit(self, user_id: str, state: dict[str, Any]) -> dict[str, int | str]:
        return MapProjectionSqlWriteRepository(self.player_database).save_daily_limit(user_id, state)

    def cooldown_until(self, user_id: str, field: str) -> str | None:
        return MapProjectionSqlRepository(self.player_database).cooldown_until(user_id, field)

    def set_cooldown(self, user_id: str, field: str, value: str | None) -> str | None:
        return MapProjectionSqlWriteRepository(self.player_database).set_cooldown(user_id, field, value)

    def map_status(self, user_id: str) -> dict[str, Any] | None:
        return MapStatusSqlQueryRepository(self.player_database).get(user_id)

    def save_status(self, user_id: str, realm: str, heaven: str, node_id: str, visited_nodes: list[str]) -> dict[str, Any]:
        return MapStatusSqlWriteRepository(self.player_database).upsert(user_id, realm, heaven, node_id, visited_nodes)

    def explore_status(self, user_id: str) -> dict[str, Any] | None:
        return MapExploreStatusSqlQueryRepository(self.player_database).get(user_id)

    def save_explore_status(self, user_id: str, state: dict[str, Any]) -> dict[str, Any]:
        return MapExploreStatusSqlWriteRepository(self.player_database).save(user_id, state)

    def mission(self, user_id: str) -> dict[str, Any] | None:
        return MapMissionSqlQueryRepository(self.player_database).get(user_id)

    def save_mission(self, user_id: str, state: dict[str, Any]) -> dict[str, Any]:
        return MapMissionSqlWriteRepository(self.player_database).save(user_id, state)

    def dongfu(self, user_id: str) -> dict[str, Any] | None:
        return MapDongfuSqlQueryRepository(self.player_database).get(user_id)

    def nearby_players(self, realm: str, heaven: str, node_id: str) -> list[dict[str, Any]]:
        return MapNearbyPlayersSqlQueryRepository(self.player_database, self.game_database).list(realm, heaven, node_id)

    def nearby_target(
        self,
        realm: str,
        heaven: str,
        node_id: str,
        user_name: str,
        *,
        exclude_user_id: str | None = None,
    ) -> MapNearbyTargetResult:
        return MapNearbyPlayersSqlQueryRepository(self.player_database, self.game_database).find(
            realm=realm, heaven=heaven, node_id=node_id,
            user_name=user_name, exclude_user_id=exclude_user_id,
        )

    async def random_nearby_target(
        self,
        realm: str,
        heaven: str,
        node_id: str,
        *,
        exclude_user_id: str,
        random_source,
    ) -> dict[str, Any] | None:
        return await select_random_nearby_target(
            MapRandomTargetSqlQueryRepository(self.player_database, self.game_database),
            realm=realm, heaven=heaven, node_id=node_id,
            exclude_user_id=exclude_user_id, random_source=random_source,
        )

    async def nearby_display(
        self,
        realm: str,
        heaven: str,
        node_id: str,
        *,
        exclude_user_id: str,
        random_source,
    ) -> list[dict[str, Any]]:
        return await select_nearby_display(
            MapNearbyDisplaySqlQueryRepository(self.player_database, self.game_database),
            realm=realm, heaven=heaven, node_id=node_id,
            exclude_user_id=exclude_user_id, random_source=random_source,
        )

    def interactive_start(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.interactive_start",
                payload={"user_id": user_id, **kwargs},
                call=lambda: MapInteractiveStartSqlRepository(self.game_database, self.player_database).start(operation_id, user_id, **kwargs),
            )
        return self._action("interactive_start", operation_id=operation_id, user_id=user_id, **kwargs)

    def interactive_failure(self, *, operation_id: str, user_id: str, action_id: str, outcome: str, cooldown_until: str):
        if self._explicit_repository is None:
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.interactive_failure",
                payload={"user_id": user_id, "action_id": action_id, "outcome": outcome, "cooldown_until": cooldown_until},
                call=lambda: MapInteractiveFailureSqlRepository(self.player_database).finish_failure(operation_id, user_id, action_id, outcome, cooldown_until),
            )
        return self._action("interactive_failure", operation_id=operation_id, user_id=user_id, action_id=action_id, outcome=outcome, cooldown_until=cooldown_until)

    def interactive_settlement(self, *, operation_id: str, user_id: str, action_id: str, settlement: dict[str, Any]):
        if self._explicit_repository is None:
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.interactive_settlement",
                payload={"user_id": user_id, "action_id": action_id, "settlement": settlement},
                call=lambda: MapInteractiveSettlementSqlRepository(self.player_database).save_settlement(user_id, action_id, settlement),
            )
        return self._action("interactive_settlement", operation_id=operation_id, user_id=user_id, action_id=action_id, settlement=settlement)


    def interactive_finish(self, *, operation_id: str, user_id: str, action_id: str, settlement: dict[str, Any]):
        return self.interactive_settlement(
            operation_id=operation_id,
            user_id=user_id,
            action_id=action_id,
            settlement=settlement,
        )
    def combat_pending(self, user_id: str): return MapCombatLifecycleQueryRepository(self.game_database,self.player_database).get_pending(user_id)
    def combat_replay(self, operation_id: str, user_id: str): return MapCombatLifecycleQueryRepository(self.game_database,self.player_database).replay_start(operation_id,user_id)
    async def combat_battle(
        self,
        *,
        user_id: str,
        enemy: dict[str, Any],
        bot_id: Any,
    ) -> tuple[Any, str, dict[str, Any]]:
        if self._combat_runner is None:
            raise RuntimeError("MapApplication requires a combat runner to execute battles")
        return await self._combat_runner(user_id, enemy, bot_id=bot_id)
    def combat_save_plan(self, *, operation_id: str, user_id: str, task_id: str, plan: dict[str, Any]):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.combat_save_plan",payload={"user_id":user_id,"task_id":task_id,"plan":plan},call=lambda:MapCombatLifecyclePlanSqlRepository(self.player_database).save_plan(operation_id,user_id,task_id,plan))
        return self._action("combat_save_plan",operation_id=operation_id,user_id=user_id,task_id=task_id,plan=plan)
    def combat_start(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.combat_start",payload={"user_id":user_id,**kwargs},call=lambda:MapCombatLifecycleStartSqlRepository(self.game_database,self.player_database).start(operation_id,user_id,**kwargs))
        return self._action("combat_start",operation_id=operation_id,user_id=user_id,**kwargs)
    def combat_settle(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._combat_settlement_application.settle(
                operation_id=operation_id,
                user_id=user_id,
                **kwargs,
            )
        return self._action("combat_settle", operation_id=operation_id, user_id=user_id, **kwargs)
    def explore_start(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="map.explore_start", payload={"user_id": user_id, **kwargs}, call=lambda: MapExploreStartSqlRepository(self.game_database, self.player_database).start(operation_id, user_id, **kwargs))
        return self._action("explore_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def explore_settle(self, *, operation_id: str, user_id: str, clock: Any, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="map.explore_settle", payload={"user_id": user_id, **kwargs}, call=lambda: MapExploreSettlementSqlRepository(self.game_database, self.player_database, clock=clock).settle(operation_id, user_id, **kwargs))
        return self._action("explore_settle", operation_id=operation_id, user_id=user_id, **kwargs)
    def resource_reward(self, *, operation_id: str, user_id: str, clock: Any = None, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.resource_reward",
                payload={"user_id": user_id, **kwargs},
                call=lambda: MapResourceRewardSqlRepository(self.game_database, self.player_database, clock=clock).settle(operation_id, user_id, **kwargs),
            )
        return self._action("resource_reward", operation_id=operation_id, user_id=user_id, **kwargs)
    def mission_claim(self, *, operation_id: str, user_id: str, clock: Any, **kwargs: Any):
        if self._explicit_repository is None:
            request_payload = {
                "user_id": user_id,
                **{
                    key: value
                    for key, value in kwargs.items()
                    if key not in {"clock", "event_meta"}
                },
            }
            outcome = self._execute(
                operation_id=operation_id,
                user_id=user_id,
                action="map.mission_claim",
                payload=request_payload,
                call=lambda: MapMissionClaimSqlRepository(
                    self.game_database, self.player_database, clock=clock
                ).claim(operation_id, user_id, **kwargs),
            )
            if outcome.ok and isinstance(outcome.data, dict) and outcome.data.get("effects_event_id") and self.game_event_effects is not None:
                if not self.game_event_effects.dispatch(str(outcome.data["effects_event_id"])):
                    return replace(outcome, message="委托奖励已发放，统计和进度正在补偿。")
            return outcome
        return self._action("mission_claim",operation_id=operation_id,user_id=user_id,**kwargs)

    def reconcile_mission_claim_operation(self, record: dict[str, Any]) -> OperationOutcome[dict[str, Any]]:
        operation_id = str(record["operation_id"])
        action = str(record.get("action") or "map.mission_claim")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            receipt = uow.query_one(
                "SELECT stone,rewards FROM map_mission_claim_operations WHERE operation_id=?",
                (operation_id,),
            )
            event = uow.query_one(
                "SELECT payload_json FROM domain_outbox WHERE event_id=?",
                (f"map.mission.effects:{operation_id}",),
            )
        if receipt is None:
            return OperationOutcome.failed(
                operation_id,
                action,
                "map mission claim receipt is missing; request may be retried",
                code="reconcile_receipt_missing",
                audit_category="map",
            )
        data = {
            "status": "applied",
            "stone": int(receipt["stone"]),
            "rewards": tuple(
                tuple(int(value) for value in row)
                for row in json.loads(str(receipt["rewards"]))
            ),
        }
        if event is not None:
            payload = json.loads(str(event["payload_json"]))
            data["effects_event_id"] = f"map.mission.effects:{operation_id}"
            data["occurred_at"] = str(payload["occurred_at"])
        return OperationOutcome.applied(
            operation_id,
            action,
            data=data,
            audit_category="map",
        )

    def purchase_seed(self, *, operation_id: str, user_id: str, clock: Any, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.purchase_seed",payload={"user_id":user_id,**kwargs},call=lambda:MapSeedPurchaseSqlRepository(self.game_database,clock=clock).purchase(operation_id,user_id,**kwargs))
        return self._action("purchase_seed",operation_id=operation_id,user_id=user_id,**kwargs)
    def build_dongfu(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.build_dongfu",payload={"user_id":user_id,**kwargs},call=lambda:MapDongfuBuildSqlRepository(self.game_database,self.player_database).build(operation_id,user_id,**kwargs))
        return self._action("build_dongfu",operation_id=operation_id,user_id=user_id,**kwargs)


__all__ = ["MapApplication"]
