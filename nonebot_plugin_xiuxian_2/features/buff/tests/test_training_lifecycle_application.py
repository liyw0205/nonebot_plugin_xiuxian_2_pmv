import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ...tasks.migrations import apply_task_progress
from ..application import BuffApplication
from ..migrations import apply_normal_training_game, apply_normal_training_player
from ..training_complete_repository import NormalTrainingCompleteSqlRepository
from ..training_start_repository import NormalTrainingStartSqlRepository
from tests.test_db_backend import db_backend


class FixedClock:
    def now(self):
        return datetime(2026, 1, 4, 12, 0)


class NormalTrainingLifecycleApplicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT,exp INTEGER,stone INTEGER,"
                "hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
            )
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?)",
                [("cultivator", 100, 50, 1, 2, 3, 4), ("miner", 0, 20, 0, 0, 0, 0)],
            )
            conn.executemany(
                "INSERT INTO user_cd VALUES(?,0,0,NULL)",
                [("cultivator",), ("miner",)],
            )
        with DatabaseUnitOfWork(self.game) as uow:
            OperationLedger(clock=FixedClock()).ensure_schema(uow)
            apply_normal_training_game(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_normal_training_player(uow)
            apply_task_progress(uow)
        self.application = BuffApplication(
            self.game, self.player, clock=FixedClock()
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_cultivation_completion_is_replayable_with_stats_and_weekly_task(self):
        started = self.application.training_start(
            operation_id="cultivation-op", user_id="cultivator", kind="cultivation",
            expected_exp=100, expected_stone=50, reward=30, exp_cap=120,
            power_multiplier=2,
        )
        self.assertTrue(started.ok)
        self.assertEqual("started", started.data["status"])

        completed = self.application.training_complete(
            operation_id="cultivation-op", user_id="cultivator", task_period="2026-W01"
        )
        replay = self.application.training_complete(
            operation_id="cultivation-op", user_id="cultivator", task_period="2026-W02"
        )
        self.assertTrue(completed.ok)
        self.assertEqual(20, completed.data["exp_gain"])
        self.assertTrue(replay.ok)
        self.assertTrue(replay.replayed)

        with db_backend.connection(self.game) as conn:
            self.assertEqual(
                (120, 11, 7, 10, 240),
                tuple(conn.execute(
                    "SELECT exp,hp,mp,atk,power FROM user_xiuxian WHERE user_id='cultivator'"
                ).fetchone()),
            )
            self.assertEqual(0, conn.execute(
                "SELECT type FROM user_cd WHERE user_id='cultivator'"
            ).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(
                (1, 20),
                tuple(conn.execute(
                    'SELECT "修炼次数","修炼修为" FROM statistics WHERE user_id=?',
                    ("cultivator",),
                ).fetchone()),
            )
            task = conn.execute(
                "SELECT weekly_period,weekly_progress FROM xiuxian_tasks WHERE user_id=?",
                ("cultivator",),
            ).fetchone()
            self.assertEqual("2026-W01", task[0])
            self.assertEqual(1, json.loads(task[1])["weekly_out_closing"])

    def test_mining_completion_projects_legacy_stats_without_weekly_task(self):
        started = self.application.training_start(
            operation_id="mining-op", user_id="miner", kind="mining",
            expected_exp=0, expected_stone=20, reward=500, exp_cap=0,
            power_multiplier=1,
        )
        completed = self.application.training_complete(
            operation_id="mining-op", user_id="miner", task_period="2026-W01"
        )
        self.assertTrue(started.ok)
        self.assertTrue(completed.ok)
        self.assertEqual(500, completed.data["stone_gain"])
        with db_backend.connection(self.game) as conn:
            self.assertEqual(520, conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id='miner'"
            ).fetchone()[0])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(
                (1, 500),
                tuple(conn.execute(
                    'SELECT "凡人挖矿次数","灵石获取" FROM statistics WHERE user_id=?',
                    ("miner",),
                ).fetchone()),
            )
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM xiuxian_tasks WHERE user_id='miner'"
            ).fetchone())

    def test_late_receipt_failure_rolls_back_all_lifecycle_effects_and_can_retry(self):
        self.application.training_start(
            operation_id="late-failure", user_id="cultivator", kind="cultivation",
            expected_exp=100, expected_stone=50, reward=10, exp_cap=200,
            power_multiplier=2,
        )
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER fail_training_receipt "
                "BEFORE UPDATE OF status ON normal_training_operations "
                "WHEN NEW.status='applied' BEGIN SELECT RAISE(ABORT,'receipt failure'); END"
            )
        with self.assertRaises(Exception):
            self.application.training_complete(
                operation_id="late-failure", user_id="cultivator", task_period="2026-W01"
            )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(100, conn.execute(
                "SELECT exp FROM user_xiuxian WHERE user_id='cultivator'"
            ).fetchone()[0])
            self.assertEqual(5, conn.execute(
                "SELECT type FROM user_cd WHERE user_id='cultivator'"
            ).fetchone()[0])
            self.assertEqual("pending", conn.execute(
                "SELECT status FROM normal_training_operations WHERE operation_id='late-failure'"
            ).fetchone()[0])
            conn.execute("DROP TRIGGER fail_training_receipt")
        with db_backend.connection(self.player) as conn:
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM statistics WHERE user_id='cultivator'"
            ).fetchone())
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM xiuxian_tasks WHERE user_id='cultivator'"
            ).fetchone())

        retried = self.application.training_complete(
            operation_id="late-failure", user_id="cultivator", task_period="2026-W01"
        )
        self.assertTrue(retried.ok)
        self.assertEqual(10, retried.data["exp_gain"])


class NormalTrainingLifecycleSchemaTests(unittest.TestCase):
    def test_repositories_do_not_create_request_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp) / "game.db", Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,exp INTEGER,stone INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            with db_backend.transaction(player):
                pass
            start = NormalTrainingStartSqlRepository(game).start(
                "missing", "u", "cultivation", 0, 0, 1, 10, 1
            )
            complete = NormalTrainingCompleteSqlRepository(game, player).complete(
                "missing", "2026-W01", "u"
            )
            self.assertEqual("schema_missing", start.status)
            self.assertEqual("schema_missing", complete.status)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertIsNone(uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='normal_training_operations'"
                ))

    def test_missing_player_schema_does_not_poison_operation_and_can_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp) / "game.db", Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT,exp INTEGER,stone INTEGER,"
                    "hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                conn.execute(
                    "INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)"
                )
                conn.execute(
                    "CREATE TABLE user_cd(user_id TEXT,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                conn.execute("INSERT INTO user_cd VALUES('u',0,0,NULL)")
            with DatabaseUnitOfWork(game) as uow:
                OperationLedger(clock=FixedClock()).ensure_schema(uow)
                apply_normal_training_game(uow)
            with db_backend.transaction(player):
                pass

            application = BuffApplication(game, player, clock=FixedClock())
            started = application.training_start(
                operation_id="schema-retry", user_id="u", kind="cultivation",
                expected_exp=100, expected_stone=50, reward=10, exp_cap=200,
                power_multiplier=2,
            )
            self.assertTrue(started.ok)
            rejected = application.training_complete(
                operation_id="schema-retry", user_id="u", task_period="2026-W01"
            )
            self.assertFalse(rejected.ok)
            self.assertEqual("schema_missing", rejected.code)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                self.assertIsNone(uow.query_one(
                    "SELECT 1 FROM operation_ledger WHERE operation_id=? AND action='buff.training_complete'",
                    ("schema-retry",),
                ))
            with DatabaseUnitOfWork(player) as uow:
                apply_normal_training_player(uow)
                apply_task_progress(uow)
            retried = application.training_complete(
                operation_id="schema-retry", user_id="u", task_period="2026-W01"
            )
            self.assertTrue(retried.ok)
            self.assertEqual(10, retried.data["exp_gain"])


if __name__ == "__main__":
    unittest.main()
