from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..key_event_repository import RiftKeyEventSqlRepository
from ..migrations import apply_rift_demon_token_player_schema, apply_rift_key_event_operations
from tests.test_db_backend import db_backend


class FixedClock:
    def now(self):
        return datetime(2026, 9, 25, tzinfo=timezone.utc)


class RiftKeyEventRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="rift-key-event-")
        root = Path(self.temp.name)
        self.game_db, self.player_db = root / "game.db", root / "player.db"
        self.rift = {"name": "key", "rank": 2, "time": 30}
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER,hp INTEGER,mp INTEGER)")
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT,status TEXT)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1000,500,100,80)")
            conn.execute("INSERT INTO user_cd VALUES('u',3,'now','30')")
            conn.execute("INSERT INTO rift_entries VALUES('u',%s,'active')", (json.dumps(self.rift),))
            conn.execute("INSERT INTO back VALUES('u',20001,'key','item',1,'','',1)")
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('CREATE TABLE rift(user_id TEXT PRIMARY KEY,"legacy_field" TEXT)')
            conn.execute("INSERT INTO rift VALUES('u','preserved')")
            conn.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"existing_stat" INTEGER)')
            conn.execute("INSERT INTO statistics VALUES('u',7)")
        with DatabaseUnitOfWork(self.game_db) as uow:
            apply_rift_key_event_operations(uow)
        with DatabaseUnitOfWork(self.player_db) as uow:
            apply_rift_demon_token_player_schema(uow)
        with db_backend.transaction(self.player_db) as conn:
            conn.execute('UPDATE rift SET "explore_count"=9 WHERE user_id=\'u\'')
        self.repository = RiftKeyEventSqlRepository(self.game_db, self.player_db, clock=FixedClock())
        self.user = {"stone": 1000, "exp": 500, "hp": 100, "mp": 80}
        self.outcome = {
            "delta": {"stone": 250, "exp": 75, "hp": -30, "mp": -20},
            "items": [{"id": 300, "name": "loot", "type": "weapon", "amount": 1}],
            "progress_reward": {"id": 20018, "name": "token", "type": "item", "amount": 1},
            "statistics": {"秘境打怪": 1, "秘境次数": 1},
            "message": "fixed key event",
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def settle(self, operation_id="key-op", **changes):
        values = {
            "user_id": "u",
            "item_id": 20001,
            "expected_rift": self.rift,
            "expected_user": self.user,
            "expected_explore_count": 9,
            "outcome": self.outcome,
            "max_goods_num": 1000,
        }
        values.update(changes)
        return self.repository.settle(operation_id, **values)

    def test_settles_key_and_replays_legacy_payload(self) -> None:
        first = self.settle()
        duplicate = self.settle()
        replay = self.repository.replay("key-op")
        self.assertEqual((first.status, first.explore_count, duplicate.status, replay.status), ("applied", 0, "duplicate", "duplicate"))
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=20001").fetchone()), (0, 0))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "settled")
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1250, 575, 70, 60))
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 0)
            self.assertEqual(tuple(conn.execute('SELECT "秘境打怪","秘境次数" FROM statistics').fetchone()), (1, 1))

    def test_missing_migration_does_not_create_operation_table(self) -> None:
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("DROP TABLE rift_key_event_operations")
        result = self.settle("missing-migration")
        self.assertEqual(result.status, "schema_missing")
        with db_backend.connection(self.game_db) as conn:
            self.assertFalse(conn.table_exists("rift_key_event_operations"))

    def test_conflict_and_late_failure_preserve_all_state(self) -> None:
        self.assertEqual(self.settle("conflict", expected_user={**self.user, "hp": 99}).status, "state_changed")
        with db_backend.transaction(self.game_db) as conn:
            conn.execute("CREATE TRIGGER fail_key_event BEFORE INSERT ON rift_key_event_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.settle("rollback")
        with db_backend.connection(self.game_db) as conn:
            self.assertEqual(tuple(conn.execute("SELECT stone,exp,hp,mp FROM user_xiuxian").fetchone()), (1000, 500, 100, 80))
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=20001").fetchone()), (1, 1))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries").fetchone()[0], "active")
        with db_backend.connection(self.player_db) as conn:
            self.assertEqual(conn.execute('SELECT "explore_count" FROM rift').fetchone()[0], 9)


if __name__ == "__main__":
    unittest.main()
