from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.transaction_service import DongfuAccelerateService
from tests.test_db_backend import db_backend


class DongfuAccelerateServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        self.slots = [{"slot": 1, "seed_id": 21001, "seed_name": "青灵草种", "plant_start": "2026-07-13 10:00:00", "plant_finish": "2026-07-13 12:00:00", "fertilizer": 0}]
        self.expected_slots = json.dumps(self.slots, ensure_ascii=False)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
            conn.execute("INSERT INTO back VALUES (%s,%s,%s)", ("u", 21005, 2))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT)")
            conn.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s)", ("u", 1, self.expected_slots, 1, 21001, "2026-07-13 10:00:00", "2026-07-13 12:00:00"))
        self.service = DongfuAccelerateService(self.game, self.player)

    def tearDown(self):
        self.temp_dir.cleanup()

    def accelerate(self, operation_id="op", **overrides):
        values = dict(slots=self.expected_slots, slot_no=1, item_id=21005, now="2026-07-13 11:00:00", finish="2026-07-13 11:00:00")
        values.update(overrides)
        return self.service.accelerate(operation_id, "u", values["slots"], values["slot_no"], values["item_id"], values["now"], values["finish"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 21005)).fetchone()
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT plant_slots,plant_finish FROM dongfu_status WHERE user_id=%s", ("u",)).fetchone()
        return int(item[0]), json.loads(row[0])[0]["plant_finish"], str(row[1])

    def test_success_consumes_item_and_updates_finish_together(self):
        self.assertEqual(self.accelerate().status, "accelerated")
        self.assertEqual(self.state(), (1, "2026-07-13 11:00:00", "2026-07-13 11:00:00"))

    def test_duplicate_and_rejections_do_not_consume_more_items(self):
        self.assertEqual(self.accelerate("repeat").status, "accelerated")
        self.assertEqual(self.accelerate("repeat", now="later", new_finish="later2", slots="[]").status, "duplicate")
        self.assertIsNotNone(self.service.get_result("repeat"))
        self.assertEqual(self.state()[0], 1)
        self.setUp()
        self.assertEqual(self.accelerate("stale", slots="[]").status, "state_changed")
        self.assertEqual(self.accelerate("mature", now="2026-07-13 12:00:00").status, "already_mature")
        self.assertEqual(self.accelerate("none", item_id=21006).status, "item_insufficient")
        self.assertEqual(self.state()[0], 2)

    def test_operation_failure_rolls_back_item_and_finish_time(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dongfu_accelerate_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_accelerate BEFORE INSERT ON dongfu_accelerate_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.accelerate("rollback")
        self.assertEqual(self.state(), (2, "2026-07-13 12:00:00", "2026-07-13 12:00:00"))


if __name__ == "__main__":
    unittest.main()
