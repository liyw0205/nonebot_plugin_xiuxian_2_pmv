from __future__ import annotations

from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .._service_port import ServicePort
from .catalog import card_definitions
from .love_sand_repository import LoveSandSqlRepository
from .prayer_repository import ImpartPrayerSqlRepository
from .compose_repository import ImpartCardComposeSqlRepository
from .disassemble_repository import ImpartCardDisassembleSqlRepository
from .draw_repository import ImpartCrystalDrawSqlRepository, ImpartDrawSqlRepository


class ImpartRepository(ServicePort):
    def __init__(
        self,
        database: str | Path,
        *,
        game_database: str | Path | None = None,
        player_database: str | Path | None = None,
    ) -> None:
        super().__init__("impart", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart")
        self.database = str(database)
        self.game_database = str(game_database or database)
        self.player_database = str(player_database or database)

    def state(self, user_id: str, *, ensure: bool = False) -> dict | None:
        user_id = str(user_id)
        with DatabaseUnitOfWork(self.database, immediate=ensure, read_only=not ensure) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='xiuxian_impart'"
            ) is None:
                return None
            if ensure:
                uow.execute(
                    "INSERT OR IGNORE INTO xiuxian_impart("
                    "user_id,impart_hp_per,impart_atk_per,impart_mp_per,impart_exp_up,boss_atk,"
                    "impart_know_per,impart_burst_per,impart_mix_per,impart_reap_per,impart_two_exp,"
                    "stone_num,impart_lv,impart_num,exp_day,wish) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (user_id, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                )
            return uow.query_one("SELECT * FROM xiuxian_impart WHERE user_id=?", (user_id,))

    def cards(self, user_id: str) -> dict[str, int]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='impart_cards'"
            ) is None:
                return {}
            return {
                str(row["card_name"]): int(row["quantity"])
                for row in uow.query_all(
                    "SELECT card_name,quantity FROM impart_cards WHERE user_id=?", (str(user_id),)
                )
            }

    @staticmethod
    def definitions() -> dict[str, dict]:
        return card_definitions()

    def item_count(self, user_id: str, item_id: int) -> int:
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='back'"
            ) is None:
                return 0
            row = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                (str(user_id), int(item_id)),
            )
            return int(row["goods_num"]) if row is not None else 0

    def refresh(self, user_id: str, definitions: dict | None = None) -> dict:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            return self._refresh_in_transaction(uow, user_id, definitions)

    @staticmethod
    def _refresh_in_transaction(uow, user_id: str, definitions: dict | None = None) -> dict:
        from .card_bonus import refresh_card_bonuses

        return refresh_card_bonuses(
            uow.connection, str(user_id), definitions or card_definitions(), schema="main"
        )

    def draw(self, *args, **kwargs):
        return ImpartDrawSqlRepository(
            self.game_database, self.database, self.player_database
        ).draw(*args, **kwargs)

    def crystal_draw(self, *args, **kwargs):
        return ImpartCrystalDrawSqlRepository(
            self.database, self.player_database
        ).draw(*args, **kwargs)

    def draw_result(self, operation_id, user_id, requested_pulls):
        return ImpartDrawSqlRepository(
            self.game_database, self.database, self.player_database
        ).get_result(operation_id, user_id, requested_pulls)

    def crystal_draw_result(self, operation_id, user_id, cost, pulls):
        return ImpartCrystalDrawSqlRepository(
            self.database, self.player_database
        ).get_result(operation_id, user_id, cost, pulls)

    def love_sand(self, game_database, player_database, operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num):
        return LoveSandSqlRepository(game_database, self.database, player_database).apply(operation_id, user_id, item_id, quantity, gained, expected_item_count, expected_stone_num)

    def prayer(self, game_database, player_database, operation_id, user_id, item_id, quantity, cards, card_definitions):
        return ImpartPrayerSqlRepository(game_database, self.database, player_database).settle(operation_id, user_id, item_id, quantity, cards, card_definitions)

    def compose(self, operation_id, user_id, source_card, target_card, expected_source_quantity, expected_target_quantity, cost, card_definitions):
        return ImpartCardComposeSqlRepository(self.database).compose(operation_id, user_id, source_card, target_card, expected_source_quantity, expected_target_quantity, cost, card_definitions)

    def disassemble(self, operation_id, user_id, card_name, quantity, expected_card_quantity, expected_stone_quantity, reward_per_card, card_definitions):
        return ImpartCardDisassembleSqlRepository(self.database).disassemble(operation_id, user_id, card_name, quantity, expected_card_quantity, expected_stone_quantity, reward_per_card, card_definitions)


__all__ = ["ImpartRepository"]
