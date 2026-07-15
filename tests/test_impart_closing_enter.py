from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.transaction_service import (
    ImpartClosingEnterService,
)
from tests.test_db_backend import db_backend


class ImpartClosingEnterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root_type TEXT NOT NULL)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", "天灵根"))
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("mortal", "伪灵根"))
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,"
                "create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute("INSERT INTO user_cd VALUES(%s,0,'0',NULL)", ("u",))
            conn.execute("INSERT INTO user_cd VALUES(%s,0,'0',NULL)", ("mortal",))
        self.service = ImpartClosingEnterService(self.game, self.player)
        self.started_at = "2026-07-13 12:34:56.123456"

    def tearDown(self):
        self.tmp.cleanup()

    def state(self, user_id="u"):
        with db_backend.connection(self.game) as conn:
            cd = tuple(
                conn.execute(
                    "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
            )
        with db_backend.connection(self.player) as conn:
            if not conn.table_exists("statistics"):
                count = None
            else:
                row = conn.execute(
                    'SELECT "虚神界闭关次数" FROM statistics WHERE user_id=%s', (user_id,)
                ).fetchone()
                count = None if row is None else int(row[0])
        return cd, count

    def test_success_and_duplicate_are_atomic(self):
        result = self.service.enter("enter", "u", self.started_at)
        duplicate = self.service.enter("enter", "u", self.started_at)

        self.assertEqual(("applied", self.started_at, 1), (
            result.status, result.started_at, result.entry_count
        ))
        self.assertEqual("duplicate", duplicate.status)
        self.assertEqual(((4, self.started_at, None), 1), self.state())

    def test_busy_and_changed_state_do_not_write(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_cd SET type=2 WHERE user_id=%s", ("u",))

        self.assertEqual("busy", self.service.enter("busy", "u", self.started_at).status)
        self.assertEqual(((2, "0", None), None), self.state())

    def test_qualification_and_operation_conflict(self):
        self.assertEqual(
            "ineligible", self.service.enter("mortal", "mortal", self.started_at).status
        )
        self.assertEqual(((0, "0", None), None), self.state("mortal"))
        self.assertEqual("applied", self.service.enter("same", "u", self.started_at).status)
        # started_at is outcome; same op with different clock still duplicate
        self.assertEqual(
            "duplicate",
            self.service.enter("same", "u", "2026-07-13 12:35:00.000000").status,
        )
        self.assertEqual(((4, self.started_at, None), 1), self.state())

    def test_failure_rolls_back_status_and_statistics(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE impart_closing_enter_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT,created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER reject_closing_enter BEFORE INSERT ON "
                "impart_closing_enter_operations BEGIN SELECT RAISE(ABORT,'reject'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.enter("rollback", "u", self.started_at)
        self.assertEqual(((0, "0", None), None), self.state())


if __name__ == "__main__":
    unittest.main()
