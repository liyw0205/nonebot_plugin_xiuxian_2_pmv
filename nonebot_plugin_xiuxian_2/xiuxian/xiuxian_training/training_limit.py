from __future__ import annotations

from ...paths import get_paths
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from .transaction_service import TrainingStateService


player_data_manager = PlayerDataManager()


class TrainingLimit:
    """Compatibility facade for transactional training-state reads."""

    def __init__(self, state_service: TrainingStateService | None = None) -> None:
        self._state_service = state_service or TrainingStateService(
            get_paths().player_db,
            player_data_manager.lock,
        )

    def get_user_training_info(self, user_id):
        return self._state_service.get(user_id)

    def get_weekly_purchases(self, user_id, item_id):
        weekly = self.get_user_training_info(user_id)["weekly_purchases"]
        return int(weekly.get(str(item_id), 0))


training_limit = TrainingLimit()


__all__ = ["TrainingLimit", "training_limit"]
