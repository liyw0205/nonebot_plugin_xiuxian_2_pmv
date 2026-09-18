from __future__ import annotations

from ...paths import get_paths
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from .transaction_service import TowerStateService


_player_data_manager_instance = None
_state_service_instance = None


def _player_data_manager():
    global _player_data_manager_instance
    if _player_data_manager_instance is None:
        _player_data_manager_instance = PlayerDataManager()
    return _player_data_manager_instance


def _state_service():
    global _state_service_instance
    if _state_service_instance is None:
        _state_service_instance = TowerStateService(
            get_paths().player_db,
            _player_data_manager().lock,
        )
    return _state_service_instance


class TowerLimit:
    """Compatibility facade for transactional tower-state reads."""

    def __init__(self, state_service: TowerStateService | None = None) -> None:
        self._state_service_override = state_service

    def get_user_tower_info(self, user_id):
        service = self._state_service_override or _state_service()
        return service.get(user_id)

    def get_weekly_purchases(self, user_id, item_id):
        weekly = self.get_user_tower_info(user_id)["weekly_purchases"]
        return int(weekly.get(str(item_id), 0))

    def reset_all_floors(self):
        _player_data_manager().update_all_records("tower", "current_floor", 0)


tower_limit = TowerLimit()


__all__ = ["TowerLimit", "tower_limit"]
