from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort


class ImpartRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("impart", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart")
        self.database = str(database)


__all__ = ["ImpartRepository"]
