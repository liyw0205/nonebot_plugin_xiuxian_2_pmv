from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort


class TasksRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("tasks", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks", handlers={
            "claim_rewards": self._claim_rewards,
        })
        self.database = str(database)

    @staticmethod
    def _claim_rewards(**kwargs: Any):
        from ...xiuxian.xiuxian_tasks.task_data import task_manager
        return task_manager.claim_rewards(
            str(kwargs.get("operation_id", "")),
            str(kwargs.get("user_id", "")),
            kwargs.get("cycle"),
        )


__all__ = ["TasksRepository"]
