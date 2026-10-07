from __future__ import annotations

import tempfile
import unittest

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..application import ImpartApplication
from ..catalog import card_definitions, card_names
from ..migrations import (
    apply_impart_card_operations,
    apply_impart_crystal_draw_operations,
    apply_impart_draw_operations,
    apply_impart_draw_player_statistics,
)


class ImpartApplicationTest(unittest.TestCase):
    def test_catalog_reads_literal_data_and_returns_detached_definitions(self) -> None:
        names = card_names()
        definitions = card_definitions()
        self.assertEqual(len(names), 106)
        self.assertEqual(set(names), set(definitions))
        definitions[names[0]]["id"] = -1
        self.assertNotEqual(card_definitions()[names[0]]["id"], -1)

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                OperationLedger().ensure_schema(uow)
            app = ImpartApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)

    def test_mutation_facade_forwards_to_the_declared_repository_methods(self) -> None:
        class RecordingRepository:
            def __init__(self) -> None:
                self.calls = []

            def prayer(self, *args):
                self.calls.append(("prayer", args))
                return "prayer-result"

            def love_sand(self, *args):
                self.calls.append(("love_sand", args))
                return "sand-result"

            def compose(self, *args):
                self.calls.append(("compose", args))
                return "compose-result"

            def disassemble(self, *args):
                self.calls.append(("disassemble", args))
                return "disassemble-result"

        repository = RecordingRepository()
        app = ImpartApplication("unused.db", repository=repository)

        self.assertEqual(
            app.prayer_settle(
                operation_id="p",
                user_id="u",
                game_database="g.db",
                player_database="p.db",
                item_id=1,
                quantity=1,
                cards=("A",),
                card_definitions={"A": {}},
            ),
            "prayer-result",
        )
        self.assertEqual(
            app.love_sand(
                operation_id="s",
                user_id="u",
                game_database="g.db",
                player_database="p.db",
                item_id=2,
                quantity=1,
                gained=10,
                expected_item_count=2,
                expected_stone_num=3,
            ),
            "sand-result",
        )
        self.assertEqual(
            app.compose(
                operation_id="c",
                user_id="u",
                source_card="A",
                target_card="B",
                expected_source_quantity=5,
                expected_target_quantity=0,
                cost=5,
                card_definitions={},
            ),
            "compose-result",
        )
        self.assertEqual(
            app.disassemble(
                operation_id="d",
                user_id="u",
                card_name="A",
                quantity=1,
                expected_card_quantity=2,
                expected_stone_quantity=0,
                reward_per_card=2,
                card_definitions={},
            ),
            "disassemble-result",
        )
        self.assertEqual([name for name, _ in repository.calls], ["prayer", "love_sand", "compose", "disassemble"])

    def test_compose_application_reaches_repository_and_replays_frozen_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/impart.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE impart_cards("
                    "user_id TEXT,card_name TEXT,quantity INTEGER,"
                    "PRIMARY KEY(user_id,card_name))"
                )
                uow.execute("INSERT INTO impart_cards VALUES(?,?,?)", ("u", "源卡", 6))
                uow.execute("INSERT INTO impart_cards VALUES(?,?,?)", ("u", "目标卡", 1))
                uow.execute(
                    "CREATE TABLE impart_card_compose_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)"
                )

            app = ImpartApplication(database)
            first = app.compose(
                operation_id="compose-1",
                user_id="u",
                source_card="源卡",
                target_card="目标卡",
                expected_source_quantity=6,
                expected_target_quantity=1,
                cost=5,
                card_definitions=None,
            )
            duplicate = app.compose(
                operation_id="compose-1",
                user_id="u",
                source_card="源卡",
                target_card="目标卡",
                expected_source_quantity=0,
                expected_target_quantity=99,
                cost=5,
                card_definitions=None,
            )
            stale = app.compose(
                operation_id="compose-2",
                user_id="u",
                source_card="源卡",
                target_card="目标卡",
                expected_source_quantity=6,
                expected_target_quantity=1,
                cost=5,
                card_definitions=None,
            )

            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
            with DatabaseUnitOfWork(database, immediate=False) as uow:
                self.assertEqual(
                    dict(uow.execute("SELECT card_name,quantity FROM impart_cards").fetchall()),
                    {"源卡": 1, "目标卡": 2},
                )

    def test_disassemble_application_reaches_repository_and_rejects_stale_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/impart.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE impart_cards("
                    "user_id TEXT,card_name TEXT,quantity INTEGER,"
                    "PRIMARY KEY(user_id,card_name))"
                )
                uow.execute("INSERT INTO impart_cards VALUES(?,?,?)", ("u", "卡", 3))
                uow.execute(
                    "CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)"
                )
                uow.execute("INSERT INTO xiuxian_impart VALUES(?,?)", ("u", 10))
                uow.execute(
                    "CREATE TABLE impart_card_disassemble_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "card_quantity INTEGER NOT NULL,stone_quantity INTEGER NOT NULL)"
                )

            app = ImpartApplication(database)
            first = app.disassemble(
                operation_id="disassemble-1",
                user_id="u",
                card_name="卡",
                quantity=2,
                expected_card_quantity=3,
                expected_stone_quantity=10,
                reward_per_card=2,
                card_definitions=None,
            )
            duplicate = app.disassemble(
                operation_id="disassemble-1",
                user_id="u",
                card_name="卡",
                quantity=2,
                expected_card_quantity=1,
                expected_stone_quantity=14,
                reward_per_card=2,
                card_definitions=None,
            )
            stale = app.disassemble(
                operation_id="disassemble-2",
                user_id="u",
                card_name="卡",
                quantity=2,
                expected_card_quantity=3,
                expected_stone_quantity=10,
                reward_per_card=2,
                card_definitions=None,
            )

            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
            with DatabaseUnitOfWork(database, immediate=False) as uow:
                self.assertEqual(
                    tuple(uow.execute("SELECT quantity FROM impart_cards WHERE user_id=?", ("u",)).fetchone()),
                    (1,),
                )
                self.assertEqual(
                    tuple(uow.execute("SELECT stone_num FROM xiuxian_impart WHERE user_id=?", ("u",)).fetchone()),
                    (14,),
                )

    def test_read_facade_is_read_only_for_missing_and_existing_users(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/impart.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)"
                )
                uow.execute("INSERT INTO xiuxian_impart VALUES(?,?)", ("u", 7))
                uow.execute(
                    "CREATE TABLE impart_cards("
                    "user_id TEXT,card_name TEXT,quantity INTEGER,"
                    "PRIMARY KEY(user_id,card_name))"
                )
                uow.execute("INSERT INTO impart_cards VALUES(?,?,?)", ("u", "卡", 2))
                uow.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "PRIMARY KEY(user_id,goods_id))"
                )
                uow.execute("INSERT INTO back VALUES(?,?,?)", ("u", 20005, 4))

            app = ImpartApplication(database)
            self.assertEqual(app.state("u")["stone_num"], 7)
            self.assertIsNone(app.state("missing"))
            self.assertEqual(app.cards("u"), {"卡": 2})
            self.assertEqual(app.cards("missing"), {})
            self.assertEqual(app.item_count("u", 20005), 4)
            self.assertEqual(app.item_count("missing", 20005), 0)

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.execute("SELECT COUNT(*) FROM xiuxian_impart").fetchone()[0],
                    1,
                )
                self.assertEqual(
                    uow.execute("SELECT COUNT(*) FROM impart_cards").fetchone()[0],
                    1,
                )

    def test_draws_settle_receipts_cards_and_statistics_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game, impart, player = (
                f"{directory}/{name}" for name in ("game.db", "impart.db", "player.db")
            )
            with DatabaseUnitOfWork(game) as uow:
                OperationLedger().ensure_schema(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES(?,?)", ("u", 100_000_000))
                apply_impart_draw_operations(uow)
            with DatabaseUnitOfWork(impart) as uow:
                uow.execute(
                    "CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER,wish INTEGER,"
                    "impart_num INTEGER,exp_day INTEGER,impart_two_exp REAL,impart_exp_up REAL,"
                    "impart_atk_per REAL,impart_hp_per REAL,impart_mp_per REAL,boss_atk REAL,"
                    "impart_know_per REAL,impart_burst_per REAL,impart_mix_per REAL,impart_reap_per REAL)"
                )
                uow.execute(
                    "INSERT INTO xiuxian_impart VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("u", 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                )
                uow.execute(
                    "CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,"
                    "PRIMARY KEY(user_id,card_name))"
                )
                apply_impart_crystal_draw_operations(uow)
                apply_impart_card_operations(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_impart_draw_player_statistics(uow)

            app = ImpartApplication(
                game, impart_database=impart, player_database=player
            )
            definitions = {"A": {"type": "impart_atk_per", "vale": 0.1}}
            paid = app.draw(
                operation_id="paid-1",
                user_id="u",
                expected_stone=100_000_000,
                expected_wish=0,
                expected_count=0,
                cost=10_000_000,
                new_wish=10,
                pulls=1,
                cards=("A",),
                card_definitions=definitions,
                statistics_user_id="u",
                statistics={"传承抽卡": 10, "传承抽卡次数": 1, "传承抽卡灵石消耗": 10_000_000},
            )
            self.assertEqual(paid.status, "applied")
            self.assertEqual(paid.new_cards, ("A",))
            self.assertEqual(app.draw_result("paid-1", "u", 1).status, "duplicate")
            self.assertEqual(app.draw_result("paid-1", "u", 2).status, "operation_conflict")

            crystal = app.crystal_draw(
                operation_id="crystal-1",
                user_id="u",
                expected_stone=50,
                expected_wish=10,
                cost=10,
                new_wish=20,
                exp_minutes=66,
                cards=("A",),
                card_definitions=definitions,
                statistics_user_id="u",
                statistics={"传承祈愿": 10, "传承祈愿次数": 1, "虚神界时间获取": 66},
            )
            self.assertEqual(crystal.status, "applied")
            self.assertEqual(crystal.new_cards, ())
            self.assertEqual(crystal.existing_cards, ("A",))
            self.assertEqual(app.crystal_draw_result("crystal-1", "u", 10, 1).status, "duplicate")

            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertEqual(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"], 90_000_000)
            with DatabaseUnitOfWork(impart, read_only=True) as uow:
                state = uow.query_one(
                    "SELECT stone_num,wish,impart_num,exp_day,impart_atk_per FROM xiuxian_impart WHERE user_id='u'"
                )
                self.assertEqual(tuple(state.values()), (40, 20, 1, 66, 0.1))
                self.assertEqual(uow.query_one("SELECT quantity FROM impart_cards WHERE user_id='u' AND card_name='A'")["quantity"], 2)
            with DatabaseUnitOfWork(player, read_only=True) as uow:
                stats = uow.query_one(
                    'SELECT "传承抽卡","传承抽卡次数","传承抽卡灵石消耗","传承祈愿","传承祈愿次数","虚神界时间获取","传承新卡","传承重复卡" FROM statistics WHERE user_id=?',
                    ("u",),
                )
                self.assertEqual(tuple(stats.values()), (10, 1, 10_000_000, 10, 1, 66, 1, 1))


if __name__ == "__main__":
    unittest.main()
