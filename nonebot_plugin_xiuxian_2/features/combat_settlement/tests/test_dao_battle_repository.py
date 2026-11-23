import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ..repository import DaoBattleSqlRepository


class DaoBattleRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.player, self.game = root / "player.db", root / "game.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            conn.executemany("INSERT INTO user_xiuxian VALUES (?)", [("a",), ("b",)])
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)")
            conn.executemany("INSERT INTO map_status VALUES (?,?,?,?)", [("a", "凡界", "一重天", "n1"), ("b", "凡界", "一重天", "n1")])
            conn.execute("CREATE TABLE dao_record (user_id TEXT PRIMARY KEY,total INTEGER NOT NULL DEFAULT 0,win INTEGER NOT NULL DEFAULT 0,lose INTEGER NOT NULL DEFAULT 0)")
            conn.execute("CREATE TABLE map_dao_battle_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL)")
        self.repo = DaoBattleSqlRepository(self.player, self.game)

    def tearDown(self):
        self.tmp.cleanup()

    def test_symmetric_settlement_and_replay(self):
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        first = self.repo.settle("op", "a", "b", position, True)
        second = self.repo.settle("op", "a", "b", position, True)
        self.assertEqual(first["status"], "applied")
        self.assertEqual(second["status"], "duplicate")
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id").fetchall(), [("a", 1, 1, 0), ("b", 1, 0, 1)])

    def test_position_change_rejects_without_records(self):
        with sqlite3.connect(self.player) as conn:
            conn.execute("UPDATE map_status SET node_id='n2' WHERE user_id='b'")
        result = self.repo.settle("moved", "a", "b", {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}, True)
        self.assertEqual(
            (result["status"], result["reason"], result["subject"]),
            ("position_changed", "position_changed", "target"),
        )

    def test_missing_user_keeps_reason_and_subject_without_writing_records(self):
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        with sqlite3.connect(self.game) as conn:
            conn.execute("DELETE FROM user_xiuxian WHERE user_id='b'")

        result = self.repo.settle("missing", "a", "b", position, True)

        self.assertEqual(
            (result["status"], result["reason"], result["subject"]),
            ("user_missing", "user_missing", "target"),
        )
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM dao_record").fetchone()[0], 0)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM map_dao_battle_operations").fetchone()[0],
                0,
            )

    def test_missing_challenger_identifies_challenger_without_writing_records(self):
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        with sqlite3.connect(self.game) as conn:
            conn.execute("DELETE FROM user_xiuxian WHERE user_id='a'")

        result = self.repo.settle("missing-challenger", "a", "b", position, True)

        self.assertEqual(
            (result["status"], result["reason"], result["subject"]),
            ("user_missing", "user_missing", "challenger"),
        )

    def test_operation_conflict_keeps_actor_subject_without_writing_records(self):
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}
        self.repo.settle("conflict", "a", "b", position, True)

        result = self.repo.settle("conflict", "a", "b", position, False)

        self.assertEqual(
            (result["status"], result["reason"], result["subject"]),
            ("state_changed", "operation_conflict", "actor"),
        )
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(
                conn.execute("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id").fetchall(),
                [("a", 1, 1, 0), ("b", 1, 0, 1)],
            )

    def test_same_operation_concurrent_calls_settle_once(self):
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}

        def settle():
            return self.repo.settle("parallel", "a", "b", position, True)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = [future.result() for future in (executor.submit(settle), executor.submit(settle))]

        self.assertEqual(sorted(result["status"] for result in results), ["applied", "duplicate"])
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(
                conn.execute("SELECT user_id,total FROM dao_record ORDER BY user_id").fetchall(),
                [("a", 1), ("b", 1)],
            )

    def test_concurrent_application_retries_share_message_identity_not_random_outcome(self):
        from ..application import CombatSettlementApplication
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE operation_ledger(operation_id TEXT NOT NULL,action TEXT NOT NULL,"
                "request_hash TEXT NOT NULL,status TEXT NOT NULL,result_json TEXT,created_at TEXT NOT NULL,"
                "updated_at TEXT NOT NULL,PRIMARY KEY(operation_id,action))"
            )
            conn.execute(
                "CREATE TABLE operation_audit(audit_id INTEGER PRIMARY KEY AUTOINCREMENT,operation_id TEXT,"
                "action TEXT,status TEXT,before_json TEXT,after_json TEXT,consumed_json TEXT,granted_json TEXT,"
                "category TEXT,occurred_at TEXT,created_at TEXT)"
            )
        app = CombatSettlementApplication(self.game, self.player)
        position = {"realm": "凡界", "heaven": "一重天", "node_id": "n1"}

        def settle(won):
            return app.settle_dao_battle(
                operation_id="same-message", challenger_id="a", target_id="b",
                expected_position=position, challenger_won=won,
                requested_target="目标道友", success_message="winner=" + str(won),
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = [future.result() for future in (executor.submit(settle, True), executor.submit(settle, False))]
        self.assertTrue(all(outcome.ok for outcome in outcomes))
        self.assertEqual(sorted(outcome.status for outcome in outcomes), ["applied", "replayed"])
        self.assertEqual(len({outcome.message for outcome in outcomes}), 1)
        committed = next(outcome for outcome in outcomes if outcome.status == "applied")
        self.assertIn(committed.message, {"winner=True", "winner=False"})
        challenger_won = committed.message == "winner=True"
        with sqlite3.connect(self.player) as conn:
            saved = conn.execute(
                "SELECT status,result_json FROM operation_ledger WHERE operation_id=? AND action=?",
                ("same-message", "combat.dao_battle"),
            ).fetchone()
            self.assertEqual(saved[0], "applied")
            self.assertEqual(json.loads(saved[1])["message"], committed.message)
            self.assertEqual(
                conn.execute("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id").fetchall(),
                [
                    ("a", 1, int(challenger_won), int(not challenger_won)),
                    ("b", 1, int(not challenger_won), int(challenger_won)),
                ],
            )


if __name__ == "__main__":
    unittest.main()
