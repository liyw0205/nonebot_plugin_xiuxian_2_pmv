from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.transaction_service import EngravingService
from tests.test_db_backend import db_backend


class EngravingServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES (%s, %s, %s)", ("user", 20009, 2))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, effect1_type INTEGER, effect1_base_value REAL, effect1_level INTEGER, effect2_type INTEGER, effect2_base_value REAL, effect2_level INTEGER, effect3_type INTEGER, effect3_base_value REAL, effect3_level INTEGER)")
            conn.execute("INSERT INTO natal_treasure VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, 1, 0.1, 1, 0, 0, 0, 0, 0, 0))
        self.service = EngravingService(self.game, self.player)
        self.configs = {1: (0.1, 0.2), 2: (0.3, 0.4), 3: (0.5, 0.6)}

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def engrave(self, operation_id, seed=5):
        return self.service.engrave(operation_id, "user", 20009, 1, 3,
                                    self.configs, {2}, seed)

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = int(conn.execute("SELECT goods_num FROM back WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT effect2_type, effect2_base_value, effect2_level FROM natal_treasure WHERE user_id=%s", ("user",)).fetchone()
        return item, (int(row[0]), float(row[1]), int(row[2]))

    def test_engraves_distinct_effect_and_consumes_scripture(self) -> None:
        result = self.engrave("engrave")
        self.assertEqual((result.status, result.slot), ("engraved", 2))
        self.assertIn(result.effect_type, (2, 3))
        self.assertEqual(self.state()[0], 1)
        self.assertEqual(self.state()[1][2], 1)

    def test_duplicate_reuses_random_effect_without_second_cost(self) -> None:
        first = self.engrave("repeat", 5)
        duplicate = self.engrave("repeat", 5)
        conflict = self.engrave("repeat", 6)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("engraved", "duplicate", "duplicate"))
        self.assertEqual((first.effect_type, first.base_value), (duplicate.effect_type, duplicate.base_value))
        self.assertEqual(self.state()[0], 1)

    def test_insufficient_item_changes_no_slot(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        result = self.engrave("short")
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (0, (0, 0.0, 0)))

    def test_full_slots_do_not_consume_item(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET effect2_type=2, effect3_type=3")
        result = self.engrave("full")
        self.assertEqual(result.status, "slots_full")
        self.assertEqual(self.state()[0], 2)

    def test_missing_treasure_does_not_consume_item(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("DELETE FROM natal_treasure WHERE user_id=%s", ("user",))
        result = self.engrave("missing")
        self.assertEqual(result.status, "treasure_missing")
        with db_backend.connection(self.game) as conn:
            item = conn.execute(
                "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20009),
            ).fetchone()
        self.assertEqual(int(item[0]), 2)

    def test_exhausted_effect_pool_does_not_consume_item(self) -> None:
        result = self.service.engrave(
            "exhausted", "user", 20009, 1, 3, {1: (0.1, 0.2)}, set(), 5,
        )
        self.assertEqual(result.status, "effect_exhausted")
        self.assertEqual(self.state(), (2, (0, 0.0, 0)))

    def test_player_write_failure_rolls_back_item(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_engrave BEFORE UPDATE ON natal_treasure BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.engrave("rollback")
        self.assertEqual(self.state(), (2, (0, 0.0, 0)))


if __name__ == "__main__":
    unittest.main()
