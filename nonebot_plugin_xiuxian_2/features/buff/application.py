from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import BuffRepository, LegacyBuffRepository


class BuffApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: BuffRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyBuffRepository(game_database, player_database), feature="buff")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"buff.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def open(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("open", operation_id=operation_id, user_id=user_id, **kwargs)
    def upgrade_field(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("upgrade_field", operation_id=operation_id, user_id=user_id, **kwargs)
    def rename(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("rename", operation_id=operation_id, user_id=user_id, **kwargs)
    def training_start(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("training_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def training_complete(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("training_complete", operation_id=operation_id, user_id=user_id, **kwargs)
    def stone_training(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("stone_training", operation_id=operation_id, user_id=user_id, **kwargs)
    def pvp_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("pvp_settle", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["BuffApplication"]
