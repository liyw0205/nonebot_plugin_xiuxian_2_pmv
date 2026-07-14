import json
import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training.event_service import TrainingEventService
from tests.test_db_backend import db_backend


class TrainingEventServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES (%s,%s,%s,%s,%s)", ("u", 100, 200, 80, 40))
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TIMESTAMP,update_time TIMESTAMP,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))")
        self.expected = {"progress": 0, "last_time": "None", "points": 0, "completed": 0, "max_progress": 0, "last_event": "", "weekly_purchases": {}}
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,progress TEXT,last_time TEXT,points TEXT,completed TEXT,max_progress TEXT,last_event TEXT,weekly_purchases TEXT)")
            conn.execute("INSERT INTO training VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", ("u",0,"None",0,0,0,"",json.dumps({})))
        self.service = TrainingEventService(self.game, self.player)

    def tearDown(self): self.temp.cleanup()

    def test_reward_duplicate_and_state_change(self):
        state = dict(self.expected, progress=2, last_time="2026-07-13 12:00:00", last_event="reward")
        first = self.service.apply("op", "u", self.expected, state, {"stone":100,"exp":200,"hp":80,"mp":40}, 10, 20, 0, [{"id":1,"name":"药","type":"药材","amount":1}], 99)
        duplicate = self.service.apply("op", "u", self.expected, state, {"stone":100,"exp":200,"hp":80,"mp":40}, 10, 20, 0, [{"id":1,"name":"药","type":"药材","amount":1}], 99)
        stale = self.service.apply("stale", "u", self.expected, state, {"stone":100,"exp":200,"hp":80,"mp":40}, 1, 0, 0, [], 99)
        self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
        self.assertEqual((first.message, duplicate.message), ("reward", "reward"))
        with db_backend.connection(self.game) as conn: self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp FROM user_xiuxian").fetchone()), (110,220,80))

    def test_compact_weekly_json_matches_semantic_snapshot(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE training SET weekly_purchases=%s WHERE user_id=%s",
                ('{"_last_reset":"2026-07-14"}', "u"),
            )
        expected = dict(self.expected, weekly_purchases={"_last_reset": "2026-07-14"})
        state = dict(expected, progress=1, last_event="compact")
        result = self.service.apply(
            "compact", "u", expected, state,
            {"stone": 100, "exp": 200, "hp": 80, "mp": 40},
        )
        self.assertEqual((result.status, result.message), ("applied", "compact"))

    def test_same_operation_replays_first_result_even_when_random_payload_differs(self):
        first_state = dict(self.expected, progress=1, last_event="first")
        self.assertEqual(
            self.service.apply(
                "replay", "u", self.expected, first_state,
                {"stone": 100, "exp": 200, "hp": 80, "mp": 40},
                10,
            ).status,
            "applied",
        )
        replay = self.service.apply(
            "replay", "u", dict(first_state), dict(first_state, last_event="other"),
            {"stone": 110, "exp": 200, "hp": 80, "mp": 40},
            999,
        )
        self.assertEqual((replay.status, replay.message), ("duplicate", "first"))
        with db_backend.connection(self.game) as conn:
            self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 110)

    def test_failure_rolls_back(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE training_event_operations(operation_id TEXT PRIMARY KEY,payload TEXT)")
            conn.execute("CREATE TRIGGER fail_training BEFORE INSERT ON training_event_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        state = dict(self.expected, progress=1, last_time="2026-07-13 12:00:00", last_event="loss")
        with self.assertRaises(db_backend.IntegrityError): self.service.apply("fail", "u", self.expected, state, {"stone":100,"exp":200,"hp":80,"mp":40}, -10, 0, -5, [], 99)
        with db_backend.connection(self.game) as conn: self.assertEqual(tuple(conn.execute("SELECT stone,hp FROM user_xiuxian").fetchone()), (100,80))


if __name__ == "__main__": unittest.main()
