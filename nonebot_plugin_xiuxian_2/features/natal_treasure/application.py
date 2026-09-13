from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import LegacyNatalTreasureRepository, NatalTreasureRepository


class NatalTreasureApplication(LegacyApplication):
    def __init__(self, player_database: str | Path, game_database: str | Path, *, repository: NatalTreasureRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyNatalTreasureRepository(player_database, game_database), feature="natal_treasure")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"natal_treasure.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: getattr(self.repository, action)(operation_id, user_id, **kwargs))

    def awaken(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("awaken", operation_id=operation_id, user_id=user_id, **kwargs)
    def reawaken(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("reawaken", operation_id=operation_id, user_id=user_id, **kwargs)
    def train(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("train", operation_id=operation_id, user_id=user_id, **kwargs)
    def upgrade(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("upgrade", operation_id=operation_id, user_id=user_id, **kwargs)
    def engrave(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("engrave", operation_id=operation_id, user_id=user_id, **kwargs)
    def forget(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("forget", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["NatalTreasureApplication"]
