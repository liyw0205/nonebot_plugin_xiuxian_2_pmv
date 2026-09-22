from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import ValidationError
from .._legacy_application import LegacyApplication
from .repository import BuffRepository, LegacyBuffRepository
from .rename_repository import BlessedSpotRenameSqlRepository
from .upgrade_repository import BlessedSpotUpgradeSqlRepository
from .training_start_repository import NormalTrainingStartSqlRepository
from .training_complete_repository import NormalTrainingCompleteSqlRepository


class BuffApplication(LegacyApplication):
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: BuffRepository | None = None) -> None:
        self._explicit_repository = repository
        super().__init__(game_database, repository=repository or LegacyBuffRepository(game_database, player_database), feature="buff")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"buff.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def open(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("open", operation_id=operation_id, user_id=user_id, **kwargs)
    def upgrade_field(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            if "expected_level" not in kwargs or "stone_cost" not in kwargs:
                raise ValidationError("expected_level and stone_cost are required")
            repository = BlessedSpotUpgradeSqlRepository(self.game_database, self.player_database)
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.upgrade_field", payload={"user_id": user_id, **kwargs}, call=lambda: repository.upgrade(operation_id, user_id, kwargs["expected_level"], kwargs["stone_cost"], kwargs.get("max_level", 10)))
        return self._action("upgrade_field", operation_id=operation_id, user_id=user_id, **kwargs)
    def rename(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            if "expected_name" not in kwargs or "new_name" not in kwargs:
                raise ValidationError("expected_name and new_name are required")
            repository = BlessedSpotRenameSqlRepository(self.game_database)
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.rename", payload={"user_id": user_id, **kwargs}, call=lambda: repository.rename(operation_id, user_id, kwargs["expected_name"], kwargs["new_name"]))
        return self._action("rename", operation_id=operation_id, user_id=user_id, **kwargs)
    def training_start(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.training_start", payload={"user_id": user_id, **kwargs}, call=lambda: NormalTrainingStartSqlRepository(self.game_database).start(operation_id, user_id, kwargs["kind"], kwargs["expected_exp"], kwargs["expected_stone"], kwargs["reward"], kwargs["exp_cap"], kwargs["power_multiplier"], kwargs.get("duration_seconds", 60)))
        return self._action("training_start", operation_id=operation_id, user_id=user_id, **kwargs)
    def training_complete(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self._execute(operation_id=operation_id, user_id=user_id, action="buff.training_complete", payload={"user_id": user_id, **kwargs}, call=lambda: NormalTrainingCompleteSqlRepository(self.game_database, self.player_database).complete(operation_id, kwargs["task_period"]))
        return self._action("training_complete", operation_id=operation_id, user_id=user_id, **kwargs)
    def stone_training(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("stone_training", operation_id=operation_id, user_id=user_id, **kwargs)
    def closing_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("closing_settle", operation_id=operation_id, user_id=user_id, **kwargs)
    def pvp_settle(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("pvp_settle", operation_id=operation_id, user_id=user_id, **kwargs)


__all__ = ["BuffApplication"]
