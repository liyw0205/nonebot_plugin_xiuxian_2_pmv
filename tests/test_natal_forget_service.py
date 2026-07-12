from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure.forget_service import ForgetEffectService
from tests.test_db_backend import db_backend


class ForgetEffectServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.player = root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, "
                "goods_type TEXT, goods_num INTEGER, bind_num INTEGER DEFAULT 0, "
                "UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 20009, "神秘经书", "神物", 2, 0),
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE natal_treasure (user_id TEXT PRIMARY KEY, form INTEGER, "
                "effect1_type INTEGER, effect1_base_value REAL, effect1_level INTEGER, "
                "effect2_type INTEGER, effect2_base_value REAL, effect2_level INTEGER, "
                "effect3_type INTEGER, effect3_base_value REAL, effect3_level INTEGER)"
            )
            conn.execute(
                "INSERT INTO natal_treasure VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ("user", 1, 1, 0.1, 1, 2, 0.2, 3, 0, 0.0, 0),
            )
        self.service = ForgetEffectService(self.game, self.player)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def forget(self, operation_id, effect_type=2, cost=1, max_goods_num=1000):
        return self.service.forget(
            operation_id, "user", effect_type, 20009, "神秘经书", "神物",
            cost, 3, max_goods_num,
        )

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = conn.execute(
                "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20009),
            ).fetchone()
        with db_backend.connection(self.player) as conn:
            effect = conn.execute(
                "SELECT effect2_type, effect2_base_value, effect2_level "
                "FROM natal_treasure WHERE user_id=%s", ("user",),
            ).fetchone()
        return int(item[0]), (int(effect[0]), float(effect[1]), int(effect[2]))

    def test_forget_refunds_scripture_and_clears_effect_atomically(self) -> None:
        result = self.forget("refund")
        self.assertEqual(
            (result.status, result.slot, result.effect_level, result.scripture_change),
            ("forgotten", 2, 3, 1),
        )
        self.assertEqual(self.state(), (3, (0, 0.0, 0)))

    def test_forget_can_consume_scripture(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET effect2_level=1")
        result = self.forget("consume", cost=2)
        self.assertEqual((result.status, result.scripture_change), ("forgotten", -2))
        self.assertEqual(self.state(), (0, (0, 0.0, 0)))

    def test_duplicate_does_not_refund_twice_and_conflict_is_rejected(self) -> None:
        first = self.forget("repeat")
        duplicate = self.forget("repeat")
        conflict = self.forget("repeat", effect_type=1)
        self.assertEqual(
            (first.status, duplicate.status, conflict.status),
            ("forgotten", "duplicate", "state_changed"),
        )
        self.assertEqual(self.state(), (3, (0, 0.0, 0)))

    def test_missing_effect_and_last_effect_change_nothing(self) -> None:
        missing = self.forget("missing", effect_type=3)
        self.assertEqual(missing.status, "effect_missing")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE natal_treasure SET effect2_type=0, effect2_base_value=0, effect2_level=0"
            )
        last = self.forget("last", effect_type=1)
        self.assertEqual(last.status, "last_effect")
        self.assertEqual(self.state()[0], 2)

    def test_insufficient_item_does_not_clear_effect(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute("UPDATE natal_treasure SET effect2_level=1")
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE back SET goods_num=0")
        result = self.forget("short", cost=1)
        self.assertEqual(result.status, "item_insufficient")
        self.assertEqual(self.state(), (0, (2, 0.2, 1)))

    def test_inventory_full_does_not_clear_effect(self) -> None:
        result = self.forget("full", max_goods_num=2)
        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.state(), (2, (2, 0.2, 3)))

    def test_player_write_failure_rolls_back_scripture(self) -> None:
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER fail_forget BEFORE UPDATE ON natal_treasure "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.forget("rollback")
        self.assertEqual(self.state(), (2, (2, 0.2, 3)))


if __name__ == "__main__":
    unittest.main()
