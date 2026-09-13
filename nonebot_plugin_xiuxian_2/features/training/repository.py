from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort


class TrainingRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("training", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training", handlers={
            "event_apply": self._event_apply,
            "purchase": self._purchase,
            "reset": self._reset,
        })
        self.database = str(database)

    @staticmethod
    def _services():
        from ...xiuxian.xiuxian_training import (
            training_event_service,
            training_purchase_service,
            training_reset_service,
        )
        return training_event_service, training_purchase_service, training_reset_service

    @classmethod
    def _event_apply(cls, **kwargs: Any):
        return cls._services()[0].apply(**kwargs)

    @classmethod
    def _purchase(cls, **kwargs: Any):
        return cls._services()[1].purchase(**kwargs)

    @classmethod
    def _reset(cls, **kwargs: Any):
        return cls._services()[2].reset(**kwargs)


__all__ = ["TrainingRepository"]
