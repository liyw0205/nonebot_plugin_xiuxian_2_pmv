from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import LegacyMapRepository, MapHomeReturnSqlRepository, MapInteractiveSqlQueryRepository, MapInteractiveStartSqlRepository, MapMovementSqlRepository, MapRepository


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
            return MapInteractiveSqlQueryRepository(self.player_database).get_active(user_id)
        return self.repository.get_active(user_id)

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


    def interactive_finish(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("interactive_finish", operation_id=operation_id, user_id=user_id, **kwargs)
    def combat_start(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("combat_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def combat_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("combat_settle", operation_id=operation_id, user_id=user_id, **kwargs)
    def explore_start(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("explore_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def explore_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("explore_settle", operation_id=operation_id, user_id=user_id, **kwargs)
    def resource_reward(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("resource_reward", operation_id=operation_id, user_id=user_id, **kwargs)
    def mission_claim(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("mission_claim", operation_id=operation_id, user_id=user_id, **kwargs)
    def purchase_seed(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("purchase_seed", operation_id=operation_id, user_id=user_id, **kwargs)
    def build_dongfu(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("build_dongfu", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["MapApplication"]
