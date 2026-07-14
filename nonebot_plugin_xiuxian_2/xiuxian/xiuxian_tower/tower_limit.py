from __future__ import annotations

from ...paths import get_paths
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from .state_service import TowerStateService


player_data_manager = PlayerDataManager()


class TowerLimit:
    """Compatibility facade for transactional tower-state reads."""

    def __init__(self, state_service: TowerStateService | None = None) -> None:
        self._state_service = state_service or TowerStateService(
            get_paths().player_db,
            player_data_manager.lock,
        )

    def get_user_tower_info(self, user_id):
        return self._state_service.get(user_id)

    def get_weekly_purchases(self, user_id, item_id):
        weekly = self.get_user_tower_info(user_id)["weekly_purchases"]
        return int(weekly.get(str(item_id), 0))

    def reset_all_floors(self):
        player_data_manager.update_all_records("tower", "current_floor", 0)


tower_limit = TowerLimit()


__all__ = ["TowerLimit", "tower_limit"]
