from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.plant_service import DongfuPlantService
from tests.test_db_backend import db_backend


class DongfuPlantServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        self.slots = [{"slot": 1, "seed_id": 0, "seed_name": "", "plant_start": "", "plant_finish": "", "fertilizer": 0}]
        self.expected_slots = json.dumps(self.slots, ensure_ascii=False)
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
            conn.execute("INSERT INTO back VALUES (%s,%s,%s)", ("u", 21001, 2))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT)")
            conn.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s)", ("u", 1, self.expected_slots, 0, 0, "", ""))
        self.service = DongfuPlantService(self.game, self.player)

    def tearDown(self):
        self.temp_dir.cleanup()

    def plant(self, operation_id="op", **overrides):
        values = dict(slots=self.expected_slots, slot_no=1, seed_id=21001, seed_name="青灵草种", start="2026-07-13 10:00:00", finish="2026-07-13 11:00:00")
        values.update(overrides)
        return self.service.plant(operation_id, "u", values["slots"], values["slot_no"], values["seed_id"], values["seed_name"], values["start"], values["finish"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            seed = conn.execute("SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 21001)).fetchone()
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT plant_slots,planting,plant_seed_id FROM dongfu_status WHERE user_id=%s", ("u",)).fetchone()
        return int(seed[0]), json.loads(row[0]), int(row[1]), int(row[2])

    def test_success_consumes_seed_and_occupies_plot_together(self):
        self.assertEqual(self.plant().status, "planted")
        seed, slots, planting, seed_id = self.state()
        self.assertEqual((seed, slots[0]["seed_id"], planting, seed_id), (1, 21001, 1, 21001))

    def test_duplicate_and_state_rejections_do_not_consume_more_seeds(self):
        first = self.plant("repeat")
        # mutable expected_slots/times must not break same-op replay
        duplicate = self.plant("repeat", plant_start="later", plant_finish="later2", slots="[]")
        self.assertEqual((first.status, duplicate.status), ("planted", "duplicate"))
        self.assertEqual(self.state()[0], 1)
        self.assertIsNotNone(self.service.get_result("repeat"))
        self.setUp()
        self.assertEqual(self.plant("stale", slots="[]").status, "state_changed")
        self.assertEqual(self.plant("bad", seed_id=21002).status, "seed_insufficient")
        self.assertEqual(self.state()[0], 2)

    def test_operation_failure_rolls_back_seed_and_plot(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dongfu_plant_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_plant BEFORE INSERT ON dongfu_plant_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.plant("rollback")
        seed, slots, planting, seed_id = self.state()
        self.assertEqual((seed, slots[0]["seed_id"], planting, seed_id), (2, 0, 0, 0))


if __name__ == "__main__":
    unittest.main()
