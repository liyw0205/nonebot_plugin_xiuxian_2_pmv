import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore
from ..migrations import apply_closing_settlement_game
from ..closing_repository import ClosingSettlementSqlRepository
from tests.test_db_backend import db_backend


ROOT = Path(__file__).resolve().parents[4]


class ClosingSettlementRepositoryTests(unittest.TestCase):
    def test_missing_user_is_stable(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            with DatabaseUnitOfWork(db) as uow:
                OperationLedger().ensure_schema(uow)
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)
            result = ClosingSettlementSqlRepository(db).settle("c1", "missing", "now", 1, 1, 1, 1, 1, 1)
            self.assertEqual(result.status, "user_missing")

    def test_core_settlement_and_effect_event_commit_once(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)")
                conn.execute("INSERT INTO user_cd VALUES('u',1,'start',NULL)")
            with DatabaseUnitOfWork(db) as uow:
                OperationLedger().ensure_schema(uow)
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)

            repository = ClosingSettlementSqlRepository(db)
            first = repository.settle("op", "u", "start", 20, 10, 30, 40, 5, 999, 45)
            replay = repository.settle("op", "u", "start", 20, 10, 30, 40, 5, 999, 45)

            self.assertEqual(("applied", "duplicate"), (first.status, replay.status))
            self.assertEqual(first.effects_event_id, replay.effects_event_id)
            with DatabaseUnitOfWork(db, read_only=True) as uow:
                self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS n FROM domain_outbox")["n"])
                event = uow.query_one("SELECT event_type,payload_json FROM domain_outbox WHERE event_id=?", (first.effects_event_id,))
                self.assertEqual("buff.closing.effects", event["event_type"])
                self.assertEqual(45, __import__("json").loads(event["payload_json"])["exp_time"])
                self.assertEqual(120, uow.query_one("SELECT exp FROM user_xiuxian WHERE user_id='u'")["exp"])

    def test_blank_create_time_does_not_block_type_clear(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                conn.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                conn.execute("INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)")
                conn.execute("INSERT INTO user_cd VALUES('u',1,NULL,NULL)")
            with DatabaseUnitOfWork(db) as uow:
                OperationLedger().ensure_schema(uow)
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)

            result = ClosingSettlementSqlRepository(db).settle(
                "bad-time", "u", "0", 5, 0, 10, 20, 11, 12, 0
            )
            self.assertEqual("applied", result.status)
            with db_backend.connection(db) as conn:
                self.assertEqual(
                    (0, "0"),
                    tuple(
                        conn.execute(
                            "SELECT type,create_time FROM user_cd WHERE user_id='u'"
                        ).fetchone()
                    ),
                )

    def test_missing_schema_fails_closed_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            result = ClosingSettlementSqlRepository(db).settle("op", "u", "start", 1, 0, 1, 1, 1, 1)
            self.assertEqual("schema_missing", result.status)
            with DatabaseUnitOfWork(db, read_only=True) as uow:
                self.assertIsNone(uow.query_one("SELECT 1 FROM sqlite_master WHERE name='closing_settlement_operations'"))

    def test_migration_routing_assigns_projection_schema_to_owners(self):
        # Import the complete plugin graph in a clean interpreter.  Importing
        # it in this unittest process can collide with partially initialized
        # feature modules loaded by neighboring repository tests.
        script = """
import json
import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
catalog = build_migrations()
print(json.dumps({
    key: [item.version for item in migrations_for_database(catalog, key)]
    for key in ("game_db", "player_db")
}, ensure_ascii=False))
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env={
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                "XIUXIAN_AUTO_DOWNLOAD_RESOURCES": "false",
                "XIUXIAN_WEB_STATUS": "false",
            },
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(result.stdout.splitlines()[-1])
        game = set(payload["game_db"])
        player = set(payload["player_db"])
        self.assertIn("buff.008", game)
        self.assertNotIn("buff.008", player)
        self.assertIn("buff.009", player)
        self.assertNotIn("buff.009", game)
        self.assertIn("activity_state.003", game)
        self.assertNotIn("activity_state.003", player)

    def test_migration_preserves_legacy_receipts_without_inventing_effects(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / "game.db"
            with db_backend.transaction(db) as conn:
                conn.execute(
                    "CREATE TABLE closing_settlement_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "INSERT INTO closing_settlement_operations(operation_id,payload,result_json) "
                    "VALUES('old','legacy','[1,2,3,4,5,6]')"
                )
                conn.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
                )
                conn.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
            with DatabaseUnitOfWork(db) as uow:
                OutboxStore().ensure_schema(uow)
                apply_closing_settlement_game(uow)

            with DatabaseUnitOfWork(db, read_only=True) as uow:
                legacy = uow.query_one(
                    "SELECT effects_event_id,exp_time FROM closing_settlement_operations WHERE operation_id='old'"
                )
                self.assertEqual((None, 0), tuple(legacy.values()))
                self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM domain_outbox")["n"])
