from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import LegacyMapRepository, MapCombatLifecyclePlanSqlRepository, MapCombatLifecycleQueryRepository, MapCombatLifecycleStartSqlRepository, MapDongfuBuildSqlRepository, MapExploreSettlementSqlRepository, MapExploreStartSqlRepository, MapMissionClaimSqlRepository, MapSeedPurchaseSqlRepository, MapHomeReturnSqlRepository, MapInteractiveFailureSqlRepository, MapInteractiveSettlementSqlRepository, MapInteractiveSqlQueryRepository, MapInteractiveStartSqlRepository, MapMovementSqlRepository, MapResourceRewardSqlRepository, MapRepository


class MapApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: MapRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyMapRepository(game_database, player_database), feature="map")
        self._explicit_repository = repository
        self.game_database = str(game_database)
        self.player_database = str(player_database)

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
    def combat_save_plan(self, *, operation_id: str, user_id: str, task_id: str, plan: dict[str, Any]):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.combat_save_plan",payload={"user_id":user_id,"task_id":task_id,"plan":plan},call=lambda:MapCombatLifecyclePlanSqlRepository(self.player_database).save_plan(operation_id,user_id,task_id,plan))
        return self._action("combat_save_plan",operation_id=operation_id,user_id=user_id,task_id=task_id,plan=plan)
    def combat_start(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.combat_start",payload={"user_id":user_id,**kwargs},call=lambda:MapCombatLifecycleStartSqlRepository(self.game_database,self.player_database).start(operation_id,user_id,**kwargs))
        return self._action("combat_start",operation_id=operation_id,user_id=user_id,**kwargs)
    def combat_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("combat_settle", operation_id=operation_id, user_id=user_id, **kwargs)
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
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.mission_claim",payload={"user_id":user_id,**kwargs},call=lambda:MapMissionClaimSqlRepository(self.game_database,self.player_database,clock=clock).claim(operation_id,user_id,**kwargs))
        return self._action("mission_claim",operation_id=operation_id,user_id=user_id,**kwargs)
    def purchase_seed(self, *, operation_id: str, user_id: str, clock: Any, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.purchase_seed",payload={"user_id":user_id,**kwargs},call=lambda:MapSeedPurchaseSqlRepository(self.game_database,clock=clock).purchase(operation_id,user_id,**kwargs))
        return self._action("purchase_seed",operation_id=operation_id,user_id=user_id,**kwargs)
    def build_dongfu(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id,user_id=user_id,action="map.build_dongfu",payload={"user_id":user_id,**kwargs},call=lambda:MapDongfuBuildSqlRepository(self.game_database,self.player_database).build(operation_id,user_id,**kwargs))
        return self._action("build_dongfu",operation_id=operation_id,user_id=user_id,**kwargs)


__all__ = ["MapApplication"]
