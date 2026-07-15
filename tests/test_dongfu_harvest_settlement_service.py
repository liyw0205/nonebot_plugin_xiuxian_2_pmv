from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dongfu.harvest_settlement_service import DongfuHarvestSettlementService
from tests.test_db_backend import db_backend


class DongfuHarvestSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        self.slots = [
            {"slot": 1, "seed_id": 21001, "seed_name": "种子", "plant_start": "2026-07-13 09:00:00", "plant_finish": "2026-07-13 10:00:00", "fertilizer": 0},
            {"slot": 2, "seed_id": 0, "seed_name": "", "plant_start": "", "plant_finish": "", "fertilizer": 0},
        ]
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s)", ("u", 10))
            conn.execute("CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE dongfu_status (user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT,harvest_settlement TEXT)")
            conn.execute("INSERT INTO dongfu_status VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, json.dumps(self.slots), 1, 21001, "2026-07-13 09:00:00", "2026-07-13 10:00:00", "snapshot"))
        self.service = DongfuHarvestSettlementService(self.game, self.player)
        self.items = [{"id": 1, "name": "材料", "type": "材料", "amount": 2}]

    def tearDown(self):
        self.temp_dir.cleanup()

    def harvest(self, operation_id="op", **overrides):
        values = dict(slots=self.slots, slot_numbers=[1], items=self.items, max_goods=99, settled_at="2026-07-13 10:01:00")
        values.update(overrides)
        return self.service.harvest(operation_id, "u", values["slots"], values["slot_numbers"], values["items"], values["max_goods"], values["settled_at"])

    def state(self):
        with db_backend.connection(self.game) as conn:
            item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id=%s AND goods_id=%s", ("u", 1)).fetchone()
        with db_backend.connection(self.player) as conn:
            row = conn.execute("SELECT plant_slots,planting,harvest_settlement FROM dongfu_status WHERE user_id=%s", ("u",)).fetchone()
        return json.loads(row[0]), int(row[1]), str(row[2]), tuple(map(int, item)) if item else None

    def test_success_clears_plot_and_adds_items_together(self):
        result = self.harvest()
        self.assertEqual((result.status, result.rewards), ("harvested", ((1, 2),)))
        slots, planting, snapshot, item = self.state()
        self.assertEqual((slots[0]["seed_id"], planting, snapshot, item), (0, 0, "", (2, 2)))

    def test_duplicate_and_rejections_preserve_plot(self):
        first = self.harvest("repeat")
        # mutable reward amounts must not break same-op replay
        duplicate = self.harvest("repeat", items=[{"id": 1, "name": "材料", "type": "材料", "amount": 9}], settled_at="2099-01-01 00:00:00")
        self.assertEqual((first.status, duplicate.status, first.rewards), ("harvested", "duplicate", ((1, 2),)))
        self.assertEqual(duplicate.rewards, first.rewards)
        self.assertIsNotNone(self.service.get_result("repeat"))
        self.setUp()
        self.assertEqual(self.harvest("early", settled_at="2026-07-13 09:59:00").status, "not_mature")
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u", 1, "材料", "材料", 99, "", "", 99))
        self.assertEqual(self.harvest("full").status, "inventory_full")
        slots, planting, snapshot, item = self.state()
        self.assertEqual((slots[0]["seed_id"], planting, snapshot, item), (21001, 1, "snapshot", (99, 99)))

    def test_operation_failure_rolls_back_everything(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE dongfu_harvest_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,rewards TEXT NOT NULL,created_at TIMESTAMP)")
            conn.execute("CREATE TRIGGER fail_harvest BEFORE INSERT ON dongfu_harvest_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.harvest("rollback")
        slots, planting, snapshot, item = self.state()
        self.assertEqual((slots[0]["seed_id"], planting, snapshot, item), (21001, 1, "snapshot", None))


if __name__ == "__main__":
    unittest.main()
