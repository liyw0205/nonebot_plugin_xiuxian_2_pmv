from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_rift_demon_token_player_schema, apply_rift_settlement_operations
from ..settlement_repository import RiftSettlementSqlRepository
from tests.test_db_backend import db_backend


class FixedClock:
    def __init__(self, hour: int = 12) -> None:
        self.value = datetime(2026, 9, 25, hour, tzinfo=timezone.utc)

    def now(self):
        return self.value


class RiftSettlementRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="rift-settlement-")
        root = Path(self.temp.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        self.rift = {"name": "settlement", "rank": 2, "time": 30}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1000,500,100,80)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'2026-09-25T10:00:00+00:00','30')")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active')", (json.dumps(self.rift),))
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE rift(user_id TEXT PRIMARY KEY,"legacy_field" TEXT)')
            conn.execute("INSERT INTO rift VALUES('u','preserved')")
            conn.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"existing_stat" INTEGER)')
            conn.execute("INSERT INTO statistics VALUES('u',7)")
        with DatabaseUnitOfWork(self.game_db) as uow:
            apply_rift_settlement_operations(uow)
        with DatabaseUnitOfWork(self.player_db) as uow:
            apply_rift_demon_token_player_schema(uow)
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('UPDATE rift SET "explore_count"=7 WHERE user_id=\'u\'')
        self.repository = RiftSettlementSqlRepository(self.game_db, self.player_db, clock=FixedClock())
        self.user = {"stone": 1000, "exp": 500, "hp": 100, "mp": 80}
        self.outcome = {
            "delta": {"stone": 250, "exp": 75, "hp": -30, "mp": -20},
            "items": [{"id": 300, "name": "loot", "type": "weapon", "amount": 1}],
            "statistics": {"秘境次数": 1},
            "message": "fixed settlement",
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def settle(self, operation_id="settle-op", **changes):
        values = {
            "user_id": "u",
            "expected_rift": self.rift,
            "expected_user": self.user,
            "expected_explore_count": 7,
            "outcome": self.outcome,
            "max_goods_num": 1000,
        }
        values.update(changes)
        return self.repository.settle(operation_id, **values)

    def test_settlement_is_atomic_and_replayable(self) -> None:
        first = self.settle()
        duplicate = self.settle()
        replay = self.repository.replay("settle-op")
        self.assertEqual((first.status, first.explore_count, duplicate.status, replay.status), ("applied", 8, "duplicate", "duplicate"))
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1250, 575, 70, 60))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
            self.assertEqual(conn.execute("SELECT type FROM user_cd").fetchone()[0], 0)
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 8)
            self.assertEqual(conn.execute('SELECT "秘境次数" FROM statistics').fetchone()[0], 1)

    def test_not_ready_and_missing_migration_do_not_mutate(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("UPDATE user_cd SET create_time='2026-09-25T11:50:00+00:00'")
        self.assertEqual(self.settle("not-ready").status, "not_ready")
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("DROP TABLE rift_settlement_operations")
            conn.execute("UPDATE user_cd SET create_time='2026-09-25T10:00:00+00:00'")
        self.assertEqual(self.settle("missing-migration").status, "schema_missing")
        with db_backend.connection(self.game_db) as conn:
            self.assertFalse(conn.table_exists("rift_settlement_operations"))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")

    def test_startup_migration_backfills_legacy_message_column(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("DROP TABLE rift_settlement_operations")
            conn.execute("CREATE TABLE rift_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,explore_count INTEGER NOT NULL,created_at TEXT)")
            conn.execute("INSERT INTO rift_settlement_operations(operation_id,payload,explore_count) VALUES('old','payload',4)")
        with DatabaseUnitOfWork(self.game_db) as uow:
            apply_rift_settlement_operations(uow)
        with db_backend.connection(self.game_db) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(rift_settlement_operations)")}
            self.assertIn("message", columns)
            self.assertEqual(tuple(conn.execute("SELECT payload,explore_count,message FROM rift_settlement_operations WHERE operation_id='old'").fetchone()), ("payload", 4, ""))

    def test_late_operation_failure_rolls_back_both_databases(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TRIGGER fail_settlement BEFORE INSERT ON rift_settlement_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.settle("rollback")
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1000, 500, 100, 80))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 7)


if __name__ == "__main__":
    unittest.main()
