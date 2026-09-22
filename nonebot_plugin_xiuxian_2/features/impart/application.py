from __future__ import annotations

from pathlib import Path

from .._migrated_application import MigratedFeatureApplication
from .repository import ImpartRepository


class ImpartApplication(MigratedFeatureApplication):
    def __init__(self, database: str | Path, *, repository: ImpartRepository | None = None) -> None:
        super().__init__(database, feature="impart", repository=repository or ImpartRepository(database))

    def love_sand(self, *, operation_id: str, user_id: str, game_database: str, player_database: str, item_id: int, quantity: int, gained: int, expected_item_count: int, expected_stone_num: int):
        return self.repository.love_sand(game_database, player_database, operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num)

    def prayer_settle(self, *, operation_id: str, user_id: str, game_database: str, item_id: int, quantity: int, cards, card_definitions):
        return self.repository.prayer(game_database, operation_id, user_id, item_id, quantity, cards, card_definitions)

    def compose(self, *, operation_id: str, user_id: str, source_card: str, target_card: str, expected_source_quantity: int, expected_target_quantity: int, cost: int, card_definitions):
        return self.repository.compose(operation_id, user_id, source_card, target_card, expected_source_quantity, expected_target_quantity, cost, card_definitions)

    def disassemble(self, *, operation_id: str, user_id: str, card_name: str, quantity: int, expected_card_quantity: int, expected_stone_quantity: int, reward_per_card: int, card_definitions):
        return self.repository.disassemble(operation_id, user_id, card_name, quantity, expected_card_quantity, expected_stone_quantity, reward_per_card, card_definitions)

__all__ = ["ImpartApplication"]
