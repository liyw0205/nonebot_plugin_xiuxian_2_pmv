from __future__ import annotations

from typing import Any

from ..xiuxian_utils.json_store import safe_json_loads
from ..xiuxian_utils.periods import get_daily_key


class SectTaskStateManager:
    """Compatibility façade for callers not yet using SectApplication."""

    table_name = "sect_task_state"

    def __init__(self, application=None) -> None:
        self._application_instance = application

    def bind_application(self, application) -> None:
        self._application_instance = application

    def _application(self):
        if self._application_instance is None:
            raise RuntimeError("SectTaskStateManager requires a bound SectApplication")
        return self._application_instance

    @staticmethod
    def _period() -> str:
        return get_daily_key()

    @staticmethod
    def _row_to_task(row) -> dict[str, Any] | None:
        if not row:
            return None
        return {
            "任务名称": row["task_key"],
            "任务内容": safe_json_loads(row["task_data"], {}, dict),
            "sect_id": row["sect_id"],
            "period": row["period"],
            "status": row["status"],
            "progress": row["progress"],
            "target": row["target"],
            "accepted_at": row["accepted_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }

    def get_active_task(self, user_id: str | int) -> dict[str, Any] | None:
        return self._application().get_active_task(user_id)

    def accept_task(
        self,
        user_id: str | int,
        sect_id: str | int,
        task_config: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        return self._application().accept_task(user_id, sect_id, task_config)

    def complete_task(self, user_id: str | int) -> None:
        self._application().complete_task(user_id)

    def clear_task(self, user_id: str | int) -> None:
        self._application().clear_task(user_id)


sect_task_state_manager = SectTaskStateManager()
