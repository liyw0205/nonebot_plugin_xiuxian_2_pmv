from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort


class DufangRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("dufang", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang")
        self.database = str(database)


__all__ = ["DufangRepository"]
