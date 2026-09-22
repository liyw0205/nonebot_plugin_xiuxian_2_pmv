from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort
from .love_sand_repository import LoveSandSqlRepository


class ImpartRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("impart", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart")
        self.database = str(database)

    def love_sand(self, game_database, player_database, operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num):
        return LoveSandSqlRepository(game_database, self.database, player_database).apply(operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num)


__all__ = ["ImpartRepository"]
