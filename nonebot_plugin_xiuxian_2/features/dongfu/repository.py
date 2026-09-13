from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort


class DongfuRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("dongfu", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu")
        self.database = str(database)


__all__ = ["DongfuRepository"]
