from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import BaseRepository, LegacyBaseRepository
from .rename_repository import BaseRenameSqlRepository


class BaseApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: BaseRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyBaseRepository(game_database, player_database), feature="base")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"base.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def breakthrough(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("breakthrough", operation_id=operation_id, user_id=user_id, **kwargs)
    def tribulation(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("tribulation", operation_id=operation_id, user_id=user_id, **kwargs)
    def rename(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="base.rename", payload={"user_id": user_id, **kwargs}, call=lambda: BaseRenameSqlRepository(self.database).rename(operation_id, user_id, kwargs.get("rename_kind", "user"), kwargs.get("new_name", ""), item_id=kwargs.get("item_id"), stone_cost=int(kwargs.get("stone_cost", 0) or 0)))
        return self._action("rename", operation_id=operation_id, user_id=user_id, **kwargs)
    def stone_contest(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("stone_contest", operation_id=operation_id, user_id=user_id, **kwargs)
    def stone_robbery(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("stone_robbery", operation_id=operation_id, user_id=user_id, **kwargs)
    def sign(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("sign", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["BaseApplication"]
