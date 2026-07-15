from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map.transaction_service import (
    MapHomeReturnService,
)
from tests.test_db_backend import db_backend


class MapHomeReturnTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.player = Path(self.temp_dir.name) / "player.sqlite3"
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE map_status("
                "user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT,"
                "visited_nodes TEXT)"
            )
            conn.execute(
                "CREATE TABLE dongfu_status("
                "user_id TEXT PRIMARY KEY,built INTEGER,realm TEXT,heaven TEXT,"
                "node_id TEXT,node_name TEXT)"
            )
            conn.executemany(
                "INSERT INTO map_status VALUES(%s,%s,%s,%s,%s)",
                [
                    ("u", "凡界", "一重天", "n1", json.dumps(["n1"])),
                    ("v", "凡界", "一重天", "n3", json.dumps(["n3"])),
                ],
            )
            conn.executemany(
                "INSERT INTO dongfu_status VALUES(%s,%s,%s,%s,%s,%s)",
                [
                    ("u", 1, "仙界", "九重天", "n9", "云宫"),
                    ("v", 0, None, None, None, None),
                ],
            )
        self.service = MapHomeReturnService(self.player)

    def tearDown(self):
        self.temp_dir.cleanup()

    def position(self, user_id="u"):
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                "SELECT realm,heaven,node_id,visited_nodes "
                "FROM map_status WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        return tuple(row[:3]), json.loads(row[3])

    def test_return_updates_complete_position_and_visited_nodes(self):
        result = self.service.return_home("event", "u")

        self.assertEqual("applied", result.status)
        self.assertEqual(("仙界", "九重天", "n9", "云宫"), (
            result.realm, result.heaven, result.node_id, result.node_name,
        ))
        self.assertEqual(
            (("仙界", "九重天", "n9"), ["n1", "n9"]), self.position()
        )

    def test_success_replay_does_not_move_player_again(self):
        self.assertEqual("applied", self.service.return_home("event", "u").status)
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE map_status SET realm='魔界',heaven='二重天',node_id='n2' "
                "WHERE user_id='u'"
            )

        replayed = self.service.return_home("event", "u")

        self.assertEqual("duplicate", replayed.status)
        self.assertEqual(("魔界", "二重天", "n2"), self.position()[0])

    def test_missing_dongfu_result_replays_after_dongfu_is_built(self):
        missing = self.service.return_home("missing", "v")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "UPDATE dongfu_status SET built=1,realm='仙界',heaven='九重天',"
                "node_id='n9',node_name='云宫' WHERE user_id='v'"
            )

        replayed = self.service.return_home("missing", "v")

        self.assertEqual("dongfu_missing", missing.status)
        self.assertEqual("dongfu_missing", replayed.status)
        self.assertEqual(("凡界", "一重天", "n3"), self.position("v")[0])

    def test_operation_identity_rejects_a_different_user(self):
        self.assertEqual("applied", self.service.return_home("event", "u").status)

        conflict = self.service.return_home("event", "v")

        self.assertEqual("operation_conflict", conflict.status)
        self.assertEqual(("凡界", "一重天", "n3"), self.position("v")[0])

    def test_operation_failure_rolls_back_position_and_visit(self):
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "CREATE TABLE map_home_return_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "result_status TEXT NOT NULL,realm TEXT NOT NULL DEFAULT '',"
                "heaven TEXT NOT NULL DEFAULT '',node_id TEXT NOT NULL DEFAULT '',"
                "node_name TEXT NOT NULL DEFAULT '',created_at TIMESTAMP)"
            )
            conn.execute(
                "CREATE TRIGGER fail_home_return "
                "BEFORE INSERT ON map_home_return_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaises(Exception):
            self.service.return_home("rollback", "u")

        self.assertEqual(
            (("凡界", "一重天", "n1"), ["n1"]), self.position()
        )


if __name__ == "__main__":
    unittest.main()
