from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import LegacyRiftRepository, RiftRepository
from .speedup_repository import RiftSpeedupSqlRepository


class RiftApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: RiftRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyRiftRepository(game_database, player_database), feature="rift")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"rift.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def generate(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("generate", operation_id=operation_id, user_id=user_id, **kwargs)
    def enter(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("enter", operation_id=operation_id, user_id=user_id, **kwargs)
    def terminate(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("terminate", operation_id=operation_id, user_id=user_id, **kwargs)
    def event_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("event_settle", operation_id=operation_id, user_id=user_id, **kwargs)
    def speedup(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self.repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="rift.speedup", payload={"user_id": user_id, **kwargs}, call=lambda: RiftSpeedupSqlRepository(self.database).apply(operation_id, user_id, kwargs["item_id"], kwargs.get("expected_rift"), kwargs.get("expected_cd"), kwargs["remaining_ratio"]))
        return self._action("speedup", operation_id=operation_id, user_id=user_id, **kwargs)
    def settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("settle", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["RiftApplication"]
