from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.reawaken_service import ReawakenService
from tests.test_db_backend import db_backend


class ReawakenServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, bind_num INTEGER DEFAULT 0, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)", ("user", 20009, "神秘经书", "神物", 2, 0))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, name TEXT, level INTEGER, exp INTEGER, max_exp INTEGER, effect1_type INTEGER, effect1_base_value REAL, effect1_level INTEGER, effect2_type INTEGER, effect2_base_value REAL, effect2_level INTEGER, effect3_type INTEGER, effect3_base_value REAL, effect3_level INTEGER, fate_revive_count INTEGER, immortal_revive_count INTEGER, invincible_gain_count INTEGER, nirvana_revive_count INTEGER, soul_return_revive_count INTEGER, charge_status INTEGER, soul_summon_count TEXT, enlightenment_count TEXT)")
            conn.execute("INSERT INTO natal_treasure VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", ("user", 1, "旧法宝", 5, 50, 600, 1, 0.1, 1, 2, 0.2, 3, 3, 0.3, 2, 1, 2, 3, 4, 5, 1, '{"a": 1}', '{"b": 1}'))
        self.service = ReawakenService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def reawaken(self, operation_id, seed=7, cost=1, max_goods_num=1000):
        return self.service.reawaken(operation_id, "user", 20009, "神秘经书", "神物", cost, 3, max_goods_num, {1: (0.1, 0.2), 2: (0.3, 0.4)}, {1: ("一号",), 2: ("二号",)}, {2}, seed)

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = int(conn.execute("SELECT goods_num FROM back WHERE user_id=%s", ("user",)).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT level, exp, max_exp, effect1_type, effect1_level, effect2_type, effect3_type, fate_revive_count, charge_status, soul_summon_count FROM natal_treasure WHERE user_id=%s", ("user",)).fetchone()
        return item, tuple(row)

    def test_reawaken_refunds_upgrades_and_resets_all_state(self) -> None:
        result = self.reawaken("reset")
        self.assertEqual((result.status, result.scripture_change), ("reawakened", 2))
        item, state = self.state()
        self.assertEqual(item, 4)
        self.assertEqual(state[:3], (0, 0, 100))
        self.assertIn(state[3], (1, 2))
        self.assertEqual(state[4:], (1, 0, 0, 0, 0, "{}"))

    def test_reawaken_can_consume_scripture(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET effect2_level=1, effect3_level=1")
        result = self.reawaken("consume", cost=1)
        self.assertEqual((result.status, result.scripture_change), ("reawakened", -1))
        self.assertEqual(self.state()[0], 1)

    def test_duplicate_reuses_random_result_without_second_refund(self) -> None:
        first = self.reawaken("repeat", 7)
        duplicate = self.reawaken("repeat", 7)
        conflict = self.reawaken("repeat", 8)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("reawakened", "duplicate", "duplicate"))
        self.assertEqual((first.form, first.name, first.effect_type, first.base_value), (duplicate.form, duplicate.name, duplicate.effect_type, duplicate.base_value))
        self.assertEqual(self.state()[0], 4)

    def test_inventory_and_item_rejections_change_nothing(self) -> None:
        self.assertEqual(self.reawaken("full", max_goods_num=3).status, "inventory_full")
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET effect2_level=1, effect3_level=1")
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        self.assertEqual(self.reawaken("short", cost=1).status, "item_insufficient")
        self.assertEqual(self.state()[0], 0)
        self.assertEqual(self.state()[1][0], 5)

    def test_player_write_failure_rolls_back_refund(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_reawaken BEFORE UPDATE ON natal_treasure BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.reawaken("rollback")
        self.assertEqual(self.state()[0], 2)
        self.assertEqual(self.state()[1][0], 5)


if __name__ == "__main__":
    unittest.main()
