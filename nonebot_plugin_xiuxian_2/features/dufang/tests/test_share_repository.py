from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, MigrationRunner
from ....plugin import apply_platform_schema, build_migrations, migrations_for_database
from ..application import DufangApplication
from ..migrations import apply_dufang_share, apply_dufang_share_player
from ..repository import DufangRepository
from ..share_repository import DufangShareSqlRepository


class DufangShareRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?)",
                (("source", "发起者", 1000), ("u1", "甲", 100), ("u2", "乙", 20), ("u3", "丙", 0)),
            )
        self.migrate()
        self.repository = DufangShareSqlRepository(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def migrate(self):
        with DatabaseUnitOfWork(self.game) as uow:
            apply_dufang_share(uow)
            apply_platform_schema(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_dufang_share_player(uow)

    def settle(self, operation="share", event_type="profit", amount=30, recipients=(("u1", "甲"), ("u2", "乙")), **kwargs):
        return self.repository.settle(
            operation_id=operation,
            source_id="source",
            event_type=event_type,
            event_title="福泽共享" if event_type == "profit" else "厄运共享",
            event_description="冻结事件描述",
            effect_amount=amount,
            bonus_percent=12,
            recipients=recipients,
            occurred_at="2026-07-14 07:00:00",
            **kwargs,
        )

    def stones(self):
        with DatabaseUnitOfWork(self.game) as uow:
            return {
                str(row["user_id"]): int(row["stone"])
                for row in uow.query_all("SELECT user_id,stone FROM user_xiuxian ORDER BY user_id")
            }

    def sharing_stats(self):
        with DatabaseUnitOfWork(self.player) as uow:
            return {
                str(row["user_id"]): tuple(int(row[name] or 0) for name in (
                    "shared_profit", "shared_loss", "received_profit", "received_loss"
                ))
                for row in uow.query_all(
                    "SELECT user_id,shared_profit,shared_loss,received_profit,received_loss "
                    "FROM unseal_data ORDER BY user_id"
                )
            }

    def test_profit_replay_freezes_recipients_and_updates_all_projections(self):
        first = self.settle()
        duplicate = self.settle(
            event_type="loss", amount=999, recipients=(("u3", "丙"),)
        )
        self.assertEqual((first.status, first.task_status, first.completed, first.total_amount), ("applied", "completed", 2, 60))
        self.assertEqual((duplicate.status, duplicate.event_type, duplicate.event_title), ("duplicate", "profit", "福泽共享"))
        self.assertEqual([item.user_id for item in duplicate.recipients], ["u1", "u2"])
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(
            self.sharing_stats(),
            {"source": (60, 0, 0, 0), "u1": (0, 0, 30, 0), "u2": (0, 0, 30, 0)},
        )
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(
                uow.query_one("SELECT COUNT(*) AS count FROM economy_log WHERE trace_id='share'")["count"],
                2,
            )

    def test_loss_is_capped_and_missing_or_empty_wallets_are_skipped(self):
        result = self.settle(
            operation="loss",
            event_type="loss",
            amount=50,
            recipients=(("u1", "甲"), ("u2", "乙"), ("missing", "失踪"), ("u3", "丙")),
        )
        self.assertEqual((result.task_status, result.completed, result.total_amount), ("completed", 4, 70))
        self.assertEqual(
            [(item.status, item.amount, item.wallet_stone) for item in result.recipients],
            [("applied", 50, 50), ("applied", 20, 0), ("skipped", 0, 0), ("skipped", 0, 0)],
        )
        self.assertEqual(self.stones()["u1"], 50)
        self.assertEqual(self.sharing_stats()["source"], (0, 70, 0, 0))

    def test_failed_target_rolls_back_and_retry_uses_frozen_batch(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TRIGGER fail_second_share BEFORE INSERT ON economy_log "
                "WHEN NEW.user_id='u2' BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.settle(operation="resume")
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 20, "u3": 0})
        self.assertEqual(self.sharing_stats(), {"source": (30, 0, 0, 0), "u1": (0, 0, 30, 0)})
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TRIGGER fail_second_share")
        resumed = self.settle(
            operation="resume", event_type="loss", amount=999, recipients=(("u3", "丙"),)
        )
        self.assertEqual((resumed.status, resumed.task_status, resumed.event_type), ("applied", "completed", "profit"))
        self.assertEqual([(item.user_id, item.status) for item in resumed.recipients], [("u1", "duplicate"), ("u2", "applied")])
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})

    def test_concurrent_repository_instances_apply_each_recipient_once(self):
        left = DufangShareSqlRepository(self.game, self.player)
        right = DufangShareSqlRepository(self.game, self.player)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda repo: self._concurrent_settle(repo), (left, right)))
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(sum(result.total_amount for result in results), 120)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS count FROM economy_log")["count"], 2)

    def test_player_database_failure_is_repaired_without_repeating_wallet_credit(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TRIGGER fail_second_stat BEFORE INSERT ON unseal_data "
                "WHEN NEW.user_id='u2' BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.settle(operation="player-retry")
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(self.sharing_stats(), {"source": (30, 0, 0, 0), "u1": (0, 0, 30, 0)})
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DROP TRIGGER fail_second_stat")

        resumed = self.settle(
            operation="player-retry", event_type="loss", amount=999, recipients=(("u3", "丙"),)
        )
        self.assertEqual((resumed.status, resumed.task_status, resumed.total_amount), ("duplicate", "completed", 60))
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(
            self.sharing_stats(),
            {"source": (60, 0, 0, 0), "u1": (0, 0, 30, 0), "u2": (0, 0, 30, 0)},
        )
        with DatabaseUnitOfWork(self.player) as uow:
            self.assertEqual(uow.query_one("SELECT COUNT(*) AS count FROM dufang_share_player_receipts")["count"], 2)

    def _concurrent_settle(self, repository):
        return repository.settle(
            operation_id="race", source_id="source", event_type="profit", event_title="福泽共享",
            event_description="冻结事件描述", effect_amount=30, bonus_percent=12,
            recipients=(("u1", "甲"), ("u2", "乙")), occurred_at="now",
        )

    def test_application_uses_stable_identity_and_recovers_failed_or_started_ledger(self):
        app = DufangApplication(self.game, self.player)
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TRIGGER fail_second_share BEFORE INSERT ON economy_log "
                "WHEN NEW.user_id='u2' BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self._app_settle(app, "ledger-retry")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TRIGGER fail_second_share")
        retry = self._app_settle(
            app, "ledger-retry", event_type="loss", amount=999, recipients=(("u3", "丙"),)
        )
        self.assertTrue(retry.ok)
        self.assertEqual(retry.data["event_type"], "profit")
        self.assertIsInstance(retry.data["recipients"][0], dict)
        self.assertTrue(app.share_exists("ledger-retry"))
        replayed_share = app.resume_share(operation_id="ledger-retry", user_id="source")
        self.assertTrue(replayed_share.ok)
        self.assertEqual(replayed_share.data["task_status"], "completed")
        self.assertFalse(app.share_exists("missing-share"))

        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            app.ledger.begin(uow, "stale", "dufang.share_settle", {"user_id": "source"})
        recovered = self._app_settle(app, "stale")
        self.assertTrue(recovered.ok)
        self.assertEqual(recovered.data["task_status"], "completed")

    def test_application_keeps_partial_batch_started_until_resume_finishes(self):
        class ChunkedRepository:
            calls = 0

            def execute(inner_self, operation_id, user_id, action, payload):
                inner_self.calls += 1
                return {
                    "status": "applied",
                    "task_status": "running" if inner_self.calls == 1 else "completed",
                }

            def share_exists(inner_self, operation_id):
                return True

        repository = ChunkedRepository()
        app = DufangApplication(self.game, repository=repository)
        first = app.share_settle(operation_id="chunked", user_id="source")
        resumed = app.resume_share(operation_id="chunked", user_id="source", settled_at="later")
        replayed = app.resume_share(operation_id="chunked", user_id="source", settled_at="again")

        self.assertTrue(first.ok)
        self.assertTrue(resumed.ok)
        self.assertFalse(resumed.replayed)
        self.assertTrue(replayed.replayed)
        self.assertEqual(repository.calls, 2)
        with DatabaseUnitOfWork(self.game) as uow:
            rows = uow.query_all(
                "SELECT action,status FROM operation_ledger WHERE operation_id='chunked'"
            )
        self.assertEqual([(row["action"], row["status"]) for row in rows], [("dufang.share_settle", "applied")])

    def _app_settle(self, app, operation, *, event_type="profit", amount=30, recipients=(("u1", "甲"), ("u2", "乙"))):
        return app.share_settle(
            operation_id=operation,
            user_id="source",
            event_type=event_type,
            title="福泽共享" if event_type == "profit" else "厄运共享",
            desc="冻结事件描述",
            effect_amount=amount,
            cost_bonus_percent=12,
            recipients=recipients,
            settled_at="now",
        )

    def test_request_path_reports_missing_schema_without_creating_tables(self):
        root = Path(self.temp.name)
        game = root / "partial.db"
        player = root / "partial-player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
        player.touch()
        repository = DufangShareSqlRepository(game, player)
        result = repository.settle(
            operation_id="missing-schema", source_id="source", event_type="profit",
            event_title="福泽共享", event_description="描述", effect_amount=1,
            bonus_percent=0, recipients=(("target", "道友"),), occurred_at="now",
        )
        self.assertEqual(result.status, "schema_missing")
        with DatabaseUnitOfWork(game) as uow:
            names = {row["name"] for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("dufang_share_operations", names)
        self.assertNotIn("economy_log", names)

    def test_migrations_route_idempotently_and_preserve_existing_rows(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TABLE economy_log")
            uow.execute(
                "CREATE TABLE economy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,"
                "source TEXT,action TEXT,stone_delta INTEGER,item_delta TEXT,detail TEXT,created_at TEXT)"
            )
            uow.execute("INSERT INTO economy_log(user_id,source,action,stone_delta,item_delta,detail,created_at) VALUES('old','legacy','old',1,'[]','{}','then')")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DROP TABLE unseal_data")
            uow.execute(
                "CREATE TABLE unseal_data(user_id TEXT PRIMARY KEY,count INTEGER,total_cost INTEGER,"
                "profit INTEGER,loss INTEGER,last_update TEXT)"
            )
            uow.execute("INSERT INTO unseal_data VALUES('old',2,40,5,6,'then')")

        migrations = build_migrations()
        game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
        player_versions = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("legacy.dufang.002", game_versions)
        self.assertNotIn("legacy.dufang.003", game_versions)
        self.assertIn("legacy.dufang.003", player_versions)
        self.assertNotIn("legacy.dufang.002", player_versions)
        game_migrations = tuple(item for item in migrations_for_database(migrations, "game_db") if item.version.startswith("legacy.dufang."))
        player_migrations = tuple(item for item in migrations_for_database(migrations, "player_db") if item.version.startswith("legacy.dufang."))
        with DatabaseUnitOfWork(self.game) as uow:
            runner = MigrationRunner(game_migrations)
            applied = runner.apply(uow)
            self.assertEqual(runner.apply(uow), [])
            self.assertEqual(applied, ["legacy.dufang.001", "legacy.dufang.002", "legacy.dufang.004"])
            row = uow.query_one("SELECT user_id,stone_delta,trace_id FROM economy_log WHERE user_id='old'")
            self.assertEqual((row["user_id"], row["stone_delta"], row["trace_id"]), ("old", 1, None))
        with DatabaseUnitOfWork(self.player) as uow:
            runner = MigrationRunner(player_migrations)
            self.assertEqual(runner.apply(uow), ["legacy.dufang.003"])
            self.assertEqual(runner.apply(uow), [])
            row = uow.query_one("SELECT count,total_cost,profit,loss,shared_profit,received_loss FROM unseal_data WHERE user_id='old'")
            self.assertEqual(tuple(row.values()), (2, 40, 5, 6, None, None))
            self.assertIsNotNone(
                uow.query_one(
                    "SELECT 1 AS present FROM sqlite_master "
                    "WHERE type='table' AND name='dufang_share_player_receipts'"
                )
            )


if __name__ == "__main__":
    unittest.main()
