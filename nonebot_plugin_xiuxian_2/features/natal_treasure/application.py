from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .awaken_repository import NatalAwakenSqlRepository
from .reawaken_repository import NatalReawakenSqlRepository
from .training_repository import NatalTrainingSqlRepository
from .effect_upgrade_repository import NatalEffectUpgradeSqlRepository
from .engraving_repository import NatalEngravingSqlRepository
from .forget_repository import NatalForgetSqlRepository
from .repository import LegacyNatalTreasureRepository, NatalTreasureRepository


class NatalTreasureApplication(LegacyApplication):
    def __init__(self, player_database: str | Path, game_database: str | Path, *, repository: NatalTreasureRepository | None = None) -> None:
        super().__init__(game_database, repository=repository or LegacyNatalTreasureRepository(player_database, game_database), feature="natal_treasure")
        self.player_database = str(player_database)
        self._explicit_awaken_repository = repository

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"natal_treasure.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: getattr(self.repository, action)(operation_id, user_id, **kwargs))

    def awaken(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_awaken_repository is not None:
            return self._action("awaken", operation_id=operation_id, user_id=user_id, **kwargs)
        return self._execute(operation_id=operation_id, user_id=user_id, action="natal_treasure.awaken", payload={"user_id": user_id, **kwargs}, call=lambda: NatalAwakenSqlRepository(self.player_database).awaken(operation_id, user_id, **kwargs))
    def reawaken(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_awaken_repository is not None:
            return self._action("reawaken", operation_id=operation_id, user_id=user_id, **kwargs)
        return self._execute(operation_id=operation_id, user_id=user_id, action="natal_treasure.reawaken", payload={"user_id": user_id, **kwargs}, call=lambda: NatalReawakenSqlRepository(self.game_database, self.player_database).reawaken(operation_id, user_id, **kwargs))
    def train(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action="natal_treasure.train", payload={"user_id": user_id, **kwargs}, call=lambda: NatalTrainingSqlRepository(self.game_database, self.player_database).train(operation_id, user_id, **kwargs))
    def upgrade(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action="natal_treasure.upgrade", payload={"user_id": user_id, **kwargs}, call=lambda: NatalEffectUpgradeSqlRepository(self.game_database, self.player_database).upgrade(operation_id, user_id, **kwargs))
    def engrave(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action="natal_treasure.engrave", payload={"user_id": user_id, **kwargs}, call=lambda: NatalEngravingSqlRepository(self.game_database, self.player_database).engrave(operation_id, user_id, **kwargs))
    def forget(self, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action="natal_treasure.forget", payload={"user_id": user_id, **kwargs}, call=lambda: NatalForgetSqlRepository(self.game_database, self.player_database).forget(operation_id, user_id, **kwargs))


__all__ = ["NatalTreasureApplication"]
