from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..entry_repository import RiftEntrySqlRepository
from ..generation_repository import RiftGenerationSqlRepository
from ..migrations import apply_rift_entry_schema, apply_rift_world_generation
from tests.test_db_backend import db_backend


class RiftEntryRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="rift-entry-")
        self.database = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER DEFAULT 100)")
            conn.executemany("INSERT INTO user_xiuxian(user_id) VALUES(?)", [("u",), ("v",)])
            conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)")
            conn.executemany("INSERT INTO user_cd(user_id,type) VALUES(?,0)", [("u",), ("v",)])
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER DEFAULT 0,PRIMARY KEY(user_id,goods_id))")
            conn.executemany("INSERT INTO back(user_id,goods_id,goods_num,bind_num) VALUES(?,7,1,1)", [("u",), ("v",)])
        with DatabaseUnitOfWork(self.database) as uow:
            apply_rift_world_generation(uow)
            apply_rift_entry_schema(uow)
        self.plan = {
            "name": "entry",
            "rank": 1,
            "time": 60,
            "target_node_id": "trial-1",
            "target_node_name": "问心台",
        }
        self.world = RiftGenerationSqlRepository(self.database).generate("generation-1", "global", self.plan).state
        self.repository = RiftEntrySqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def enter(self, operation_id="entry-1", user_id="u", **changes):
        values = {
            "rift_key": "global",
            "rift_data": self.plan,
            "duration": 60,
            "expected_generation_id": self.world["generation_id"],
            "expected_revision": self.world["revision"],
            "stamina_cost": 6,
            "expected_stamina": 100,
        }
        values.update(changes)
        return self.repository.enter(operation_id, user_id, **values)

    def test_normal_entry_commits_stamina_cooldown_world_and_replays(self) -> None:
        first = self.enter()
        duplicate = self.enter(expected_stamina=94)
        self.assertEqual((first.status, duplicate.status, first.entries), ("applied", "duplicate", 1))
        self.assertEqual(("u",), first.world["participants"])
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 94)
            self.assertEqual(tuple(conn.execute("SELECT type,scheduled_time FROM user_cd WHERE user_id='u'").fetchone()), (3, "60"))
            self.assertEqual(conn.execute("SELECT status FROM rift_entries WHERE user_id='u'").fetchone()[0], "active")
            self.assertEqual(conn.execute("SELECT entry_count FROM rift_entry_counts WHERE user_id='u'").fetchone()[0], 1)

    def test_read_entry_projection_is_read_only_and_respects_active_filter(self) -> None:
        self.assertIsNone(self.repository.read_entry("u", active_only=True))
        self.enter()
        self.assertEqual(self.repository.read_entry("u", active_only=True), self.plan)
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE rift_entries SET status='terminated' WHERE user_id='u'")
        self.assertIsNone(self.repository.read_entry("u", active_only=True))
        self.assertEqual(self.repository.read_entry("u"), self.plan)

    def test_read_entry_missing_schema_does_not_create_tables(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE rift_entries")
        self.assertIsNone(self.repository.read_entry("u", active_only=True))
        with db_backend.connection(self.database) as conn:
            self.assertFalse(conn.table_exists("rift_entries"))

    def test_read_entry_legacy_columns_fall_back_without_request_ddl(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE rift_entries")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_data TEXT NOT NULL)")
            conn.execute("INSERT INTO rift_entries(user_id,rift_data) VALUES('u',?)", (json.dumps(self.plan),))
        self.assertIsNone(self.repository.read_entry("u", active_only=True))
        with db_backend.connection(self.database) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(rift_entries)")}
            self.assertEqual(columns, {"user_id", "rift_data"})

    def test_ticket_entry_consumes_item_and_merges_participants(self) -> None:
        first = self.enter("entry-u", "u", stamina_cost=0, expected_stamina=None)
        second = self.enter("entry-v", "v", ticket_id=7, expected_revision=2, stamina_cost=0, expected_stamina=None)
        self.assertEqual((first.status, second.status), ("applied", "applied"))
        self.assertEqual(("u", "v"), second.world["participants"])
        with db_backend.connection(self.database) as conn:
            self.assertEqual(tuple(conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='v' AND goods_id=7").fetchone()), (0, 0))

    def test_missing_migration_is_rejected_without_request_ddl(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE rift_entry_operations")
        result = self.enter("missing")
        self.assertEqual(result.status, "schema_missing")
        with db_backend.connection(self.database) as conn:
            self.assertFalse(conn.table_exists("rift_entry_operations"))

    def test_startup_migration_backfills_legacy_entry_columns(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE rift_entries")
            conn.execute("DROP TABLE rift_entry_counts")
            conn.execute("DROP TABLE rift_entry_operations")
            conn.execute("CREATE TABLE rift_entries(user_id TEXT PRIMARY KEY,rift_key TEXT NOT NULL,rift_data TEXT NOT NULL,status TEXT NOT NULL,duration INTEGER NOT NULL,created_at TEXT)")
            conn.execute("CREATE TABLE rift_entry_counts(user_id TEXT PRIMARY KEY,entry_count INTEGER NOT NULL)")
            conn.execute("CREATE TABLE rift_entry_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,entry_count INTEGER NOT NULL,created_at TEXT)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_rift_entry_schema(uow)
        with db_backend.connection(self.database) as conn:
            entry_columns = {row[1] for row in conn.execute("PRAGMA table_info(rift_entries)")}
            operation_columns = {row[1] for row in conn.execute("PRAGMA table_info(rift_entry_operations)")}
            self.assertIn("generation_id", entry_columns)
            self.assertTrue({"generation_id", "rift_data", "global_revision"}.issubset(operation_columns))

    def test_world_conflict_and_late_failure_roll_back(self) -> None:
        self.assertEqual(self.enter("stale", expected_revision=99).status, "rift_changed")
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TRIGGER fail_entry BEFORE INSERT ON rift_entry_operations BEGIN SELECT RAISE(ABORT,'fail'); END")
        with self.assertRaises(Exception):
            self.enter("rollback")
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id='u'").fetchone()[0], 100)
            self.assertEqual(conn.execute("SELECT type FROM user_cd WHERE user_id='u'").fetchone()[0], 0)
            self.assertIsNone(conn.execute("SELECT 1 FROM rift_entries WHERE user_id='u'").fetchone())
            self.assertEqual(conn.execute("SELECT revision FROM rift_world_state WHERE rift_key='global'").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
