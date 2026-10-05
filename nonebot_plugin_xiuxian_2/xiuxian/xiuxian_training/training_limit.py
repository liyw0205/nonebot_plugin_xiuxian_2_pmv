from __future__ import annotations

from ...paths import get_paths
from ...features.training.state_application import TrainingStateApplication


_state_application_instance = None


def _state_application():
    global _state_application_instance
    if _state_application_instance is None:
        _state_application_instance = TrainingStateApplication(
            get_paths().player_db,
        )
    return _state_application_instance


class TrainingLimit:
    """Compatibility facade for transactional training-state reads."""

    def __init__(self, state_application: TrainingStateApplication | None = None) -> None:
        self._state_application_override = state_application

    def configure(self, state_application: TrainingStateApplication) -> None:
        self._state_application_override = state_application

    def get_user_training_info(self, user_id):
        state_application = self._state_application_override or _state_application()
        return state_application.get(user_id)

    def get_weekly_purchases(self, user_id, item_id):
        weekly = self.get_user_training_info(user_id)["weekly_purchases"]
        return int(weekly.get(str(item_id), 0))


training_limit = TrainingLimit()


__all__ = ["TrainingLimit", "training_limit"]
