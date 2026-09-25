from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema, build_migrations, migrations_for_database
from ..application import RiftApplication
from ..migrations import apply_rift_termination_operations
from ..termination_repository import RiftTerminationSqlRepository


class TestRiftTerminationRepository(unittest.TestCase):
    snapshot = {"name": "trial", "rank": 2, "time": 30}

    @classmethod
    def _prepare(cls, database: Path, *, migration: bool = True) -> None:
        with DatabaseUnitOfWork(database) as uow:
            apply_platform_schema(uow)
            if migration:
                apply_rift_termination_operations(uow)
            uow.execute(
                "CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)"
            )
            uow.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,"
                "create_time TEXT,scheduled_time TEXT)"
            )
            uow.execute(
                "INSERT INTO rift_entries VALUES(?,?,?)",
                ("u", json.dumps(cls.snapshot, ensure_ascii=False, sort_keys=True), "active"),
            )
            uow.execute("INSERT INTO user_cd VALUES('u',3,'now','30')")

    def test_default_application_terminates_atomically_and_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            application = RiftApplication(database, Path(temp) / "player.db")
            application.legacy_repository.invoke = lambda *args, **kwargs: self.fail(
                "legacy Rift repository used"
            )

            first = application.terminate(
                operation_id="termination-1", user_id="u", rift_data=self.snapshot
            )
            replay = application.terminate(
                operation_id="termination-1", user_id="u", rift_data=self.snapshot
            )
            service_replay = application.replay_termination(
                operation_id="termination-1", user_id="u"
            )

            self.assertTrue(first.ok)
            self.assertEqual(first.data["status"], "applied")
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(service_replay.status, "duplicate")
            self.assertEqual(service_replay.rift_name, "trial")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                entry = uow.query_one(
                    "SELECT status FROM rift_entries WHERE user_id='u'"
                )
                cooldown = uow.query_one(
                    "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id='u'"
                )
            self.assertEqual(entry["status"], "terminated")
            self.assertEqual(
                (cooldown["type"], cooldown["create_time"], cooldown["scheduled_time"]),
                (0, "0", None),
            )

    def test_repository_preserves_legacy_payload_and_duplicate_reply(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            repository = RiftTerminationSqlRepository(database)

            first = repository.terminate("termination-1", "u", self.snapshot)
            duplicate = repository.terminate("termination-1", "u", self.snapshot)
            replay = repository.replay("termination-1", "u")
            wrong_user = repository.replay("termination-1", "other")
            encoded_snapshot = json.dumps(
                self.snapshot, ensure_ascii=False, sort_keys=True
            )
            legacy_payload = json.dumps(["u", encoded_snapshot], ensure_ascii=True)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                stored = uow.query_one(
                    "SELECT payload FROM rift_termination_operations "
                    "WHERE operation_id='termination-1'"
                )

            self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
            self.assertEqual((replay.status, replay.rift_name), ("duplicate", "trial"))
            self.assertEqual(wrong_user.status, "state_changed")
            self.assertEqual(stored["payload"], legacy_payload)

    def test_existing_legacy_operation_payload_replays(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database, migration=False)
            encoded_snapshot = json.dumps(
                self.snapshot, ensure_ascii=False, sort_keys=True
            )
            legacy_payload = json.dumps(["u", encoded_snapshot], ensure_ascii=True)
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE rift_termination_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                uow.execute(
                    "INSERT INTO rift_termination_operations(operation_id,payload) "
                    "VALUES('old-termination',?)",
                    (legacy_payload,),
                )

            result = RiftTerminationSqlRepository(database).terminate(
                "old-termination", "u", self.snapshot
            )

            self.assertEqual(result.status, "duplicate")
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT status FROM rift_entries WHERE user_id='u'")[
                        "status"
                    ],
                    "active",
                )

    def test_missing_migration_returns_schema_missing_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database, migration=False)
            application = RiftApplication(database, Path(temp) / "player.db")

            result = application.terminate(
                operation_id="missing-schema", user_id="u", rift_data=self.snapshot
            )
            replay = application.replay_termination(
                operation_id="missing-schema", user_id="u"
            )

            self.assertFalse(result.ok)
            self.assertEqual(result.code, "schema_missing")
            self.assertIsNone(replay)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertIsNone(
                    uow.query_one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='rift_termination_operations'"
                    )
                )
                self.assertEqual(
                    uow.query_one("SELECT status FROM rift_entries WHERE user_id='u'")[
                        "status"
                    ],
                    "active",
                )

    def test_operation_insert_failure_rolls_back_entry_and_cooldown(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TRIGGER reject_termination BEFORE INSERT ON "
                    "rift_termination_operations BEGIN "
                    "SELECT RAISE(ABORT,'reject termination'); END"
                )

            with self.assertRaises(sqlite3.IntegrityError):
                RiftTerminationSqlRepository(database).terminate(
                    "failed-termination", "u", self.snapshot
                )

            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    uow.query_one("SELECT status FROM rift_entries WHERE user_id='u'")[
                        "status"
                    ],
                    "active",
                )
                self.assertEqual(
                    uow.query_one("SELECT type FROM user_cd WHERE user_id='u'")["type"],
                    3,
                )
                self.assertEqual(
                    uow.query_one(
                        "SELECT COUNT(*) AS count FROM rift_termination_operations"
                    )["count"],
                    0,
                )

    def test_old_application_ledger_payload_replays_without_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            database = Path(temp) / "game.db"
            self._prepare(database)

            class LegacyTerminationRepository:
                def invoke(self, action, operation_id, user_id, **kwargs):
                    return {"status": "applied", "rift_name": "trial"}

            old_application = RiftApplication(
                database,
                Path(temp) / "player.db",
                repository=LegacyTerminationRepository(),
            )
            old_result = old_application.terminate(
                operation_id="legacy-web-termination",
                user_id="u",
                rift_data=self.snapshot,
            )
            new_application = RiftApplication(database, Path(temp) / "player.db")
            replay = new_application.terminate(
                operation_id="legacy-web-termination",
                user_id="u",
                rift_data=self.snapshot,
            )

            self.assertTrue(old_result.ok)
            self.assertTrue(replay.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.data["rift_name"], "trial")

    def test_termination_handler_uses_feature_application(self):
        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[
            source.index("async def break_rift_(") : source.index("async def use_rift_key(")
        ]

        self.assertIn("rift_application.replay_termination(", handler)
        self.assertIn("rift_application.terminate(", handler)
        self.assertNotIn("_rift_termination_service()", handler)

    def test_termination_migration_is_game_only(self):
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }

        self.assertIn("rift.006", routed["game_db"])
        for database_key in ("player_db", "trade_db", "impart_db", "message_db"):
            self.assertNotIn("rift.006", routed[database_key])
