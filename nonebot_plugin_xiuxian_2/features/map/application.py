from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import LegacyMapRepository, MapRepository


class MapApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: MapRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyMapRepository(game_database, player_database), feature="map")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"map.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def move(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("move", operation_id=operation_id, user_id=user_id, **kwargs)
    def return_home(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("return_home", operation_id=operation_id, user_id=user_id, **kwargs)
    def interactive_start(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("interactive_start", operation_id=operation_id, user_id=user_id, **kwargs)
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
