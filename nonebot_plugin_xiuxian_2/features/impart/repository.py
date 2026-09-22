from __future__ import annotations

from pathlib import Path

from .._service_port import ServicePort
from .love_sand_repository import LoveSandSqlRepository
from .prayer_repository import ImpartPrayerSqlRepository
from .compose_repository import ImpartCardComposeSqlRepository
from .disassemble_repository import ImpartCardDisassembleSqlRepository


class ImpartRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("impart", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart")
        self.database = str(database)

    def love_sand(self, game_database, player_database, operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num):
        return LoveSandSqlRepository(game_database, self.database, player_database).apply(operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num)

    def prayer(self, game_database, operation_id, user_id, item_id, quantity, cards, card_definitions):
        return ImpartPrayerSqlRepository(game_database, self.database).settle(operation_id, user_id, item_id, quantity, cards, card_definitions)

    def compose(self, operation_id, user_id, source_card, target_card, expected_source_quantity, expected_target_quantity, cost, card_definitions):
        return ImpartCardComposeSqlRepository(self.database).compose(operation_id, user_id, source_card, target_card, expected_source_quantity, expected_target_quantity, cost, card_definitions)

    def disassemble(self, operation_id, user_id, card_name, quantity, expected_card_quantity, expected_stone_quantity, reward_per_card, card_definitions):
        return ImpartCardDisassembleSqlRepository(self.database).disassemble(operation_id, user_id, card_name, quantity, expected_card_quantity, expected_stone_quantity, reward_per_card, card_definitions)


__all__ = ["ImpartRepository"]
