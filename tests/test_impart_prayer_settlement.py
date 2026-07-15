from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import (
    ImpartPrayerSettlementService,
)
from tests.test_db_backend import db_backend


BONUS_COLUMNS = (
    "impart_two_exp REAL,impart_exp_up REAL,impart_atk_per REAL,"
    "impart_hp_per REAL,impart_mp_per REAL,boss_atk REAL,impart_know_per REAL,"
    "impart_burst_per REAL,impart_mix_per REAL,impart_reap_per REAL"
)


class ImpartPrayerSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.sqlite3"
        self.impart = root / "impart.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s)", ("u", 20005, 5, 4))
        with db_backend.transaction(self.impart) as conn:
            conn.execute(
                "CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,"
                "PRIMARY KEY(user_id,card_name))"
            )
            conn.execute(f"CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,{BONUS_COLUMNS})")
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "A", 1))
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,0,0,0,0,0,0,0,0,0,0)", ("u",))
        self.service = ImpartPrayerSettlementService(self.game, self.impart)
        self.definitions = {
            "A": {"type": "impart_atk_per", "vale": 0.1},
            "B": {"type": "impart_hp_per", "vale": 0.2},
        }

    def tearDown(self):
        self.temp.cleanup()

    def settle(self, operation="prayer", quantity=3, cards=("A", "A", "B")):
        return self.service.settle(
            operation, "u", 20005, quantity, cards, self.definitions
        )

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone())
        with db_backend.connection(self.impart) as conn:
            cards = dict(conn.execute("SELECT card_name,quantity FROM impart_cards").fetchall())
            bonuses = tuple(
                map(
                    float,
                    conn.execute(
                        "SELECT impart_atk_per,impart_hp_per FROM xiuxian_impart WHERE user_id=%s",
                        ("u",),
                    ).fetchone(),
                )
            )
        return item, cards, bonuses

    def test_fixed_batch_consumes_item_updates_cards_and_bonuses(self):
        result = self.settle()
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.cards, ("A", "A", "B"))
        self.assertEqual(result.new_cards, ("B",))
        self.assertEqual(dict(result.card_counts), {"A": 3, "B": 1})
        self.assertEqual(result.item_remaining, 2)
        self.assertEqual(self.state(), ((2, 1), {"A": 3, "B": 1}, (0.1, 0.2)))

    def test_replay_returns_first_batch_and_rejects_identity_conflict(self):
        first = self.settle("same")
        duplicate = self.service.settle(
            "same", "u", 20005, 3, ("B", "B", "B"), self.definitions
        )
        replay = self.service.replay("same", "u", 20005, 3)
        conflict = self.service.replay("same", "u", 20005, 2)
        self.assertEqual(
            (first.status, duplicate.status, replay.status, conflict.status),
            ("applied", "duplicate", "duplicate", "operation_conflict"),
        )
        self.assertEqual(duplicate.cards, first.cards)
        self.assertEqual(replay.card_counts, first.card_counts)
        self.assertEqual(self.state(), ((2, 1), {"A": 3, "B": 1}, (0.1, 0.2)))

    def test_missing_item_or_user_changes_nothing(self):
        missing_item = self.service.settle(
            "missing-item", "u", 20005, 6, ("A",) * 6, self.definitions
        )
        with db_backend.transaction(self.impart) as conn:
            conn.execute("DELETE FROM xiuxian_impart WHERE user_id=%s", ("u",))
        missing_user = self.settle("missing-user")
        self.assertEqual((missing_item.status, missing_user.status), ("item_missing", "user_missing"))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back").fetchone()), (5, 4))
        with db_backend.connection(self.impart) as conn:
            self.assertEqual(dict(conn.execute("SELECT card_name,quantity FROM impart_cards")), {"A": 1})

    def test_unbound_items_are_preserved_when_bound_count_is_too_low(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET bind_num=1")
        self.settle("mixed-bind")
        self.assertEqual(self.state()[0], (2, 1))

    def test_operation_failure_rolls_back_item_cards_and_bonuses(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE impart_prayer_operations("
                "operation_id TEXT PRIMARY KEY,identity_json TEXT,cards_json TEXT,"
                "new_cards_json TEXT,card_counts_json TEXT,item_remaining INTEGER,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_prayer BEFORE INSERT ON impart_prayer_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.settle("rollback")
        self.assertEqual(self.state(), ((5, 4), {"A": 1}, (0.0, 0.0)))

    def test_production_handler_has_no_split_write_bypass(self):
        impart_source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart/__init__.py"
        ).read_text(encoding="utf-8")
        handler = impart_source.split("async def use_wishing_stone", 1)[1].split(
            "async def use_love_sand", 1
        )[0]
        self.assertIn("impart_prayer_service.settle(", handler)
        self.assertNotIn("data_person_add_batch(", handler)
        self.assertNotIn("update_back_j(", handler)
        self.assertNotIn("re_impart_data(", handler)

        back_source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/__init__.py"
        ).read_text(encoding="utf-8")
        use_item_handler = back_source.split("async def use_item_", 1)[1].split(
            "async def use_pet_egg_item", 1
        )[0]
        self.assertIn("if goods_id != 20005:", use_item_handler)


if __name__ == "__main__":
    unittest.main()
