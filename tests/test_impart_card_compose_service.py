import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import CardComposeService
from tests.test_db_backend import db_backend


class CardComposeServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "impart.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE impart_cards (user_id TEXT, card_name TEXT, quantity INTEGER, PRIMARY KEY(user_id, card_name))")
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "源卡", 6))
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "目标卡", 1))
        self.service = CardComposeService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.db) as conn:
            return dict(conn.execute("SELECT card_name,quantity FROM impart_cards WHERE user_id=%s", ("u",)).fetchall())

    def test_success_duplicate_and_conflict(self):
        first = self.service.compose("op", "u", "源卡", "目标卡", 6, 1)
        duplicate = self.service.compose("op", "u", "源卡", "目标卡", 6, 1)
        # mutable expected quantities no longer break same-op replay
        conflict = self.service.compose("op", "u", "源卡", "目标卡", 5, 2)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "duplicate"))
        self.assertEqual(self.state(), {"源卡": 1, "目标卡": 2})

    def test_rejections_and_stale_snapshot_change_nothing(self):
        self.assertEqual(self.service.compose("same", "u", "源卡", "源卡", 6, 6).status, "same_card")
        self.assertEqual(self.service.compose("stale", "u", "源卡", "目标卡", 5, 1).status, "state_changed")
        self.assertEqual(self.service.compose("missing", "u", "目标卡", "源卡", 1, 6).status, "card_missing")
        self.assertEqual(self.state(), {"源卡": 6, "目标卡": 1})

    def test_operation_failure_rolls_back_cards(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE impart_card_compose_operations (operation_id TEXT PRIMARY KEY,payload TEXT,source_quantity INTEGER,target_quantity INTEGER)")
            conn.execute("CREATE TRIGGER fail_compose BEFORE INSERT ON impart_card_compose_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.compose("fail", "u", "源卡", "目标卡", 6, 1)
        self.assertEqual(self.state(), {"源卡": 6, "目标卡": 1})


if __name__ == "__main__":
    unittest.main()
