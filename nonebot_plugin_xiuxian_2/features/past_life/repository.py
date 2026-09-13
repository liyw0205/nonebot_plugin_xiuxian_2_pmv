from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort


class PastLifeRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("past_life", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_past_life")
        self.database = str(database)


__all__ = ["PastLifeRepository"]
