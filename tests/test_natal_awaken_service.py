from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.awaken_service import AwakenService
from tests.test_db_backend import db_backend


class AwakenServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.player = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, name TEXT, level INTEGER, exp INTEGER, max_exp INTEGER, effect1_type INTEGER, effect1_base_value REAL, effect1_level INTEGER, effect2_type INTEGER, effect2_base_value REAL, effect2_level INTEGER, effect3_type INTEGER, effect3_base_value REAL, effect3_level INTEGER, fate_revive_count INTEGER, immortal_revive_count INTEGER, invincible_gain_count INTEGER, nirvana_revive_count INTEGER, soul_return_revive_count INTEGER, charge_status INTEGER, soul_summon_count TEXT, enlightenment_count TEXT)")
            conn.execute("INSERT INTO natal_treasure (user_id, form) VALUES (%s, %s)", ("user", 0))
        self.service = AwakenService(self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def awaken(self, operation_id, seed=7):
        return self.service.awaken(operation_id, "user", 3, {1: (0.1, 0.2), 2: (0.3, 0.4)}, {1: ("一号",), 2: ("二号",)}, {2}, seed)

    def state(self):
        with db_backend.connection(self.player) as conn:
            return tuple(conn.execute("SELECT form, name, level, exp, max_exp, effect1_type, effect1_base_value, effect1_level, effect2_type, effect3_type, fate_revive_count, charge_status, soul_summon_count FROM natal_treasure WHERE user_id=%s", ("user",)).fetchone())

    def test_awaken_writes_complete_initial_state_atomically(self) -> None:
        result = self.awaken("awaken")
        self.assertEqual(result.status, "awakened")
        state = self.state()
        self.assertIn(state[0], (1, 2, 3, 4))
        self.assertIn(state[1], ("一号", "二号"))
        self.assertEqual(state[2:5], (0, 0, 100))
        self.assertIn(state[5], (1, 2))
        self.assertEqual(state[7:], (1, 0, 0, 0, 0, "{}"))

    def test_duplicate_reuses_random_result_and_conflict_is_rejected(self) -> None:
        first = self.awaken("repeat", 7)
        duplicate = self.awaken("repeat", 7)
        conflict = self.awaken("repeat", 8)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("awakened", "duplicate", "state_changed"))
        self.assertEqual((first.form, first.name, first.effect_type, first.base_value), (duplicate.form, duplicate.name, duplicate.effect_type, duplicate.base_value))

    def test_already_awakened_is_not_overwritten(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET form=3, name=%s WHERE user_id=%s", ("已有法宝", "user"))
        self.assertEqual(self.awaken("existing").status, "already_awakened")
        self.assertEqual(self.state()[:2], (3, "已有法宝"))

    def test_missing_schema_returns_explicit_failure(self) -> None:
        service = AwakenService(Path(self.temp_dir.name) / "missing.sqlite3")
        result = service.awaken("missing", "user", 3, {1: (0.1, 0.2)}, {1: ("一号",)}, set(), 7)
        self.assertEqual(result.status, "treasure_missing")

    def test_write_failure_leaves_unawakened_state(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_awaken BEFORE UPDATE ON natal_treasure BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.awaken("rollback")
        self.assertEqual(self.state()[0], 0)


if __name__ == "__main__":
    unittest.main()
