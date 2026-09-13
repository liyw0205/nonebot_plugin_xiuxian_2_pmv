from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort


class LunhuiRepository(ServicePort):
    def __init__(self, database: str | Path, *databases: str | Path) -> None:
        self.database = str(database)
        self.databases = tuple(str(item) for item in databases)
        super().__init__("lunhui", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui", handlers={
            "reset": self._reset,
            "recall": self._recall,
            "settle": self._settle,
        })

    @staticmethod
    def _services():
        from ...xiuxian.xiuxian_lunhui import (
            cultivation_reset_service,
            lunhui_recall_service,
            lunhui_settlement_service,
        )
        return cultivation_reset_service, lunhui_recall_service, lunhui_settlement_service

    @classmethod
    def _reset(cls, **kwargs: Any):
        return cls._services()[0].reset(**kwargs)

    @classmethod
    def _recall(cls, **kwargs: Any):
        return cls._services()[1].recall(**kwargs)

    @classmethod
    def _settle(cls, **kwargs: Any):
        return cls._services()[2].settle(**kwargs)


__all__ = ["LunhuiRepository"]
