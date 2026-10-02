from __future__ import annotations

from threading import RLock

from ...paths import get_paths
from ...features.tower.state_application import TowerStateApplication


_state_application_instance = None
_state_lock = RLock()


def _state_application():
    global _state_application_instance
    if _state_application_instance is None:
        _state_application_instance = TowerStateApplication(
            get_paths().player_db,
            lock=_state_lock,
        )
    return _state_application_instance


class TowerLimit:
    """Compatibility facade for transactional tower-state reads."""

    def __init__(self, state_application: TowerStateApplication | None = None) -> None:
        self._state_application_override = state_application

    def get_user_tower_info(self, user_id):
        state_application = self._state_application_override or _state_application()
        return state_application.get(user_id)

    def get_weekly_purchases(self, user_id, item_id):
        weekly = self.get_user_tower_info(user_id)["weekly_purchases"]
        return int(weekly.get(str(item_id), 0))

    def reset_all_floors(self, *, operation_id: str, source: str, period_key: str):
        state_application = self._state_application_override or _state_application()
        return state_application.reset_all_floors(
            operation_id=operation_id, source=source, period_key=period_key
        )

    def ranking(self, field: str, limit: int = 50):
        state_application = self._state_application_override or _state_application()
        return state_application.ranking(field, limit)


tower_limit = TowerLimit()


__all__ = ["TowerLimit", "tower_limit"]
