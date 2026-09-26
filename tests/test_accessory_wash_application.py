from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.accessory_wash_application import AccessoryWashApplication
from nonebot_plugin_xiuxian_2.features.back.accessory_wash_domain import plan_wash
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_accessory_affix_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class FixedRandom:
    def sample(self, values, count):
        return list(values)[:count]

    def uniform(self, low, high):
        return low


class AccessoryWashApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        self.accessory = {
            "uid": "acc-1",
            "name": "测试戒指",
            "quality": 4,
            "affixes": [{"type": "攻击", "value": 0.1}, {"type": "速度", "value": 20}, {"type": "气血", "value": 0.08}],
            "locked_affixes": [0],
            "wash_count": 149,
        }
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',20023,'洗练石','特殊道具',20,20)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE player_accessory(user_id TEXT PRIMARY KEY,equipped TEXT,bag TEXT)")
            conn.execute("INSERT INTO player_accessory VALUES(?,?,?)", ("u", "{}", json.dumps([self.accessory], ensure_ascii=False)))
        with DatabaseUnitOfWork(self.game) as uow:
            apply_accessory_affix_operations(uow)
        self.application = AccessoryWashApplication(self.game, self.player, random_source=FixedRandom())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def state(self):
        with sqlite3.connect(self.game) as conn:
            stone = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=20023").fetchone()
        with sqlite3.connect(self.player) as conn:
            bag = json.loads(conn.execute("SELECT bag FROM player_accessory WHERE user_id='u'").fetchone()[0])
        return tuple(stone), bag

    def test_wash_replay_pity_and_locked_affix(self) -> None:
        first = self.application.wash("wash-1", "u", "acc-1", self.accessory, 20, 20023, 16)
        duplicate = self.application.wash("wash-1", "u", "acc-1", self.accessory, 20, 20023, 16)
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(first.accessory["wash_count"], 150)
        self.assertEqual(first.accessory["affixes"][0], self.accessory["affixes"][0])
        self.assertEqual(first.accessory["affixes"][1]["value"], 0.16)
        self.assertEqual(self.state()[0], (4, 4))

    def test_stale_snapshot_and_trigger_failure_do_not_mutate(self) -> None:
        stale = dict(self.accessory, wash_count=1)
        self.assertEqual(self.application.wash("stale", "u", "acc-1", stale, 20, 20023, 16).status, "state_changed")
        before = self.state()
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_wash BEFORE INSERT ON accessory_transaction_operations BEGIN SELECT RAISE(ABORT,'reject'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.application.wash("rollback", "u", "acc-1", self.accessory, 20, 20023, 16)
        self.assertEqual(self.state(), before)

    def test_domain_rejects_all_locked_and_uses_injected_random(self) -> None:
        all_locked = dict(self.accessory, locked_affixes=[0, 1, 2])
        self.assertEqual(plan_wash(all_locked, FixedRandom()).status, "too_many_locks")
        planned = plan_wash(self.accessory, FixedRandom())
        self.assertEqual(planned.accessory["affixes"][1]["value"], 0.16)


if __name__ == "__main__":
    unittest.main()
