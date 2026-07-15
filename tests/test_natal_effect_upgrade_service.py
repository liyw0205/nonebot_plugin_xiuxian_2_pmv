from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.effect_upgrade_service import EffectUpgradeService
from tests.test_db_backend import db_backend


class EffectUpgradeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES (%s, %s, %s)", ("user", 20009, 2))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, effect1_type INTEGER, effect1_level INTEGER, effect2_type INTEGER, effect2_level INTEGER, effect3_type INTEGER, effect3_level INTEGER)")
            conn.execute("INSERT INTO natal_treasure VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, 11, 1, 12, 1, 13, 3))
        self.service = EffectUpgradeService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def upgrade(self, operation_id, seed=7):
        return self.service.upgrade(operation_id, "user", 20009, 1, 3, 10, seed)

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = int(conn.execute("SELECT goods_num FROM back WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            levels = tuple(int(value) for value in conn.execute("SELECT effect1_level, effect2_level, effect3_level FROM natal_treasure WHERE user_id=%s", ("user",)).fetchone())
        return item, levels

    def test_upgrades_one_lowest_effect_and_consumes_scripture(self) -> None:
        result = self.upgrade("upgrade")
        self.assertEqual((result.status, result.level), ("upgraded", 2))
        self.assertIn(result.slot, (1, 2))
        self.assertEqual(sum(self.state()[1]), 6)
        self.assertEqual(self.state()[0], 1)

    def test_duplicate_reuses_selected_slot_without_second_cost(self) -> None:
        first = self.upgrade("repeat", 7)
        duplicate = self.upgrade("repeat", 7)
        conflict = self.upgrade("repeat", 8)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("upgraded", "duplicate", "duplicate"))
        self.assertEqual((first.slot, duplicate.slot), (duplicate.slot, duplicate.slot))
        self.assertEqual(self.state()[0], 1)

    def test_insufficient_item_changes_no_effect(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        result = self.upgrade("short")
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (0, (1, 1, 3)))

    def test_all_maxed_does_not_consume_item(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET effect1_level=10, effect2_level=10, effect3_level=10")
        result = self.upgrade("maxed")
        self.assertEqual(result.status, "all_maxed")
        self.assertEqual(self.state(), (2, (10, 10, 10)))

    def test_player_write_failure_rolls_back_item(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_effect BEFORE UPDATE ON natal_treasure BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.upgrade("rollback")
        self.assertEqual(self.state(), (2, (1, 1, 3)))


if __name__ == "__main__":
    unittest.main()
