from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base.xiangyuan_settlement_service import (
    XiangyuanSettlementService,
)
from tests.test_db_backend import db_backend


class XiangyuanClaimTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.sqlite3", root / "player.sqlite3"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (%s,0)", (("u1",), ("u2",)))
            conn.execute(
                "CREATE TABLE back (user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        self.service = XiangyuanSettlementService(self.game, self.player)
        self.legacy = {
            "last_id": 2,
            "gifts": {
                "1": {
                    "id": 1, "giver_id": "giver", "giver_name": "赠礼者",
                    "stone_amount": 100, "remaining_stone": 100,
                    "items": [{"goods_id": 101, "name": "符剑", "type": "装备", "quantity": 1}],
                    "receiver_count": 1, "received": 0, "receivers": [],
                    "create_time": "2026-07-13 12:00:00",
                }
            },
        }

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def claim(self, operation, user="u1"):
        return self.service.claim(operation, "group", 1, user, 100, (101,), 3, 1000, legacy_data=self.legacy)

    def state(self):
        with db_backend.connection(self.game) as conn:
            stones = {str(row[0]): int(row[1]) for row in conn.execute("SELECT user_id,stone FROM user_xiuxian").fetchall()}
            gift = tuple(map(int, conn.execute("SELECT remaining_stone,received FROM xiangyuan_gifts WHERE group_id='group' AND gift_id=1").fetchone()))
            item = conn.execute("SELECT user_id,goods_num,bind_num FROM back WHERE goods_id=101").fetchall()
            receivers = int(conn.execute("SELECT COUNT(*) FROM xiangyuan_receivers WHERE group_id='group' AND gift_id=1").fetchone()[0])
        return stones, gift, [tuple(row) for row in item], receivers

    def test_claim_applies_fixed_last_reward_inventory_pool_and_counter(self):
        result = self.claim("claim-1")

        self.assertEqual((result.status, result.stone, result.items), ("applied", 100, ((101, "符剑", 1),)))
        self.assertEqual(self.state(), ({"u1": 100, "u2": 0}, (0, 1), [("u1", 1, 1)], 1))
        with db_backend.connection(self.player) as conn:
            self.assertEqual(int(conn.execute("SELECT receive_count FROM xiangyuan_limit WHERE user_id='u1'").fetchone()[0]), 1)

    def test_duplicate_is_idempotent_and_stale_fixed_reward_is_rejected(self):
        first, duplicate = self.claim("repeat"), self.claim("repeat")
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(self.state()[0], {"u1": 100, "u2": 0})

        stale = self.service.claim("stale", "group", 1, "u2", 99, (101,), 3, 1000, legacy_data=self.legacy)
        self.assertEqual(stale.status, "unavailable")
        self.assertEqual(self.state()[3], 1)

    def test_concurrent_last_claim_has_exactly_one_winner(self):
        barrier = threading.Barrier(2)
        results = []

        def run(user):
            barrier.wait()
            service = XiangyuanSettlementService(self.game, self.player)
            results.append(service.claim(f"parallel-{user}", "group", 1, user, 100, (101,), 3, 1000, legacy_data=self.legacy))

        threads = [threading.Thread(target=run, args=(user,)) for user in ("u1", "u2")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sorted(result.status for result in results), ["applied", "unavailable"])
        state = self.state()
        self.assertEqual(sum(state[0].values()), 100)
        self.assertEqual((state[1], state[3], len(state[2])), ((0, 1), 1, 1))

    def test_inventory_or_operation_failure_rolls_back_everything(self):
        self.service.get_group("group", legacy_data=self.legacy)
        with db_backend.transaction(self.game) as conn:
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,%s,'','',%s)", ("u1", 101, "符剑", "装备", 1000, 1000))
        result = self.claim("full")
        self.assertEqual(result.status, "inventory_full")
        self.assertEqual(self.state()[1:], ((100, 0), [("u1", 1000, 1000)], 0))

        with db_backend.transaction(self.game) as conn:
            conn.execute("DELETE FROM back")
            conn.execute(
                "CREATE TRIGGER fail_xiangyuan_claim BEFORE INSERT ON xiangyuan_claim_operations "
                "BEGIN SELECT RAISE(ABORT, 'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.claim("failure")
        self.assertEqual(self.state(), ({"u1": 0, "u2": 0}, (100, 0), [], 0))


if __name__ == "__main__":
    unittest.main()
