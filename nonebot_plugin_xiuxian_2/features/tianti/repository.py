from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort


class TiantiRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("tianti", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti")
        self.database = str(database)


__all__ = ["TiantiRepository"]
