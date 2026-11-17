from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from ..practice_repository import SectPracticeSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect


class SectPracticeRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.db"
        migrate_sect(self.database)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,"
                "sect_position INTEGER,stone INTEGER,atkpractice INTEGER,hppractice INTEGER,mppractice INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES('user',1,3,1000,2,3,4)")
            conn.execute(
                "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,"
                "sect_materials INTEGER,sect_used_stone INTEGER)"
            )
            conn.execute("INSERT INTO sects VALUES(1,'owner',5000,1000)")
        self.repository = SectPracticeSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def state(self):
        with db_backend.connection(self.database) as conn:
            return tuple(
                conn.execute(
                    "SELECT stone,atkpractice,hppractice,mppractice,sect_materials,sect_used_stone "
                    "FROM user_xiuxian JOIN sects USING(sect_id) WHERE user_id='user'"
                ).fetchone()
            )

    def test_all_practice_types_replay_and_deduct_assets(self):
        for name, level, next_level in [("attack", 2, 3), ("health", 3, 4), ("mana", 4, 5)]:
            self.assertEqual(
                self.repository.upgrade(f"op-{name}", "user", 1, name, level, next_level, 100, 1000)["status"],
                "upgraded",
            )
        self.assertEqual(
            self.repository.upgrade("op-attack", "user", 1, "attack", 2, 3, 100, 1000)["status"],
            "duplicate",
        )
        self.assertEqual(self.state(), (700, 3, 4, 5, 2000, 1000))

    def test_replay_uses_receipt_when_current_level_has_reached_target(self):
        self.repository.upgrade("op-advanced", "user", 1, "attack", 2, 3, 100, 1000)
        result = self.repository.upgrade("op-advanced", "user", 1, "attack", 3, 4, 100, 1000)
        self.assertEqual(result["status"], "duplicate")
        self.assertEqual(self.state(), (900, 3, 3, 4, 4000, 1000))

    def test_receipt_ignore_rolls_back_effects_and_recovers_same_operation(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE isolated_effect(n INTEGER NOT NULL)")
            conn.execute("INSERT INTO isolated_effect VALUES(0)")
            conn.execute(
                "CREATE TRIGGER practice_effect AFTER UPDATE OF atkpractice ON user_xiuxian "
                "BEGIN UPDATE isolated_effect SET n=n+1; END"
            )
            conn.execute(
                "CREATE TRIGGER ignore_practice_receipt BEFORE INSERT ON sect_practice_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        before = self.state()
        failed = self.repository.upgrade("recover", "user", 1, "attack", 2, 3, 100, 1000)
        self.assertEqual(failed["status"], "state_changed")
        self.assertEqual(failed["reason"], "sect_practice_receipt_write_rejected")
        self.assertEqual(failed["subject"], "recover")
        self.assertEqual(self.state(), before)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT n FROM isolated_effect").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sect_practice_operations").fetchone()[0], 0)
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER ignore_practice_receipt")
        recovered = self.repository.upgrade("recover", "user", 1, "attack", 2, 3, 100, 1000)
        self.assertEqual(recovered["status"], "upgraded")
        self.assertEqual(self.state(), (900, 3, 3, 4, 4000, 1000))

    def test_receipt_rewrite_is_rejected_and_rolled_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_practice_receipt AFTER INSERT ON sect_practice_operations "
                "BEGIN UPDATE sect_practice_operations SET to_level=99 WHERE operation_id=NEW.operation_id; END"
            )
        before = self.state()
        result = self.repository.upgrade("rewrite", "user", 1, "attack", 2, 3, 100, 1000)
        self.assertEqual(result["reason"], "sect_practice_receipt_write_rejected")
        self.assertEqual(self.state(), before)
        with db_backend.connection(self.database) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sect_practice_operations").fetchone()[0], 0)

    def test_assets_rewrite_is_rejected_and_rolled_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_practice_assets AFTER UPDATE OF sect_materials ON sects "
                "BEGIN UPDATE sects SET sect_materials=NEW.sect_materials+1 WHERE sect_id=NEW.sect_id; END"
            )
        before = self.state()
        result = self.repository.upgrade("assets", "user", 1, "attack", 2, 3, 100, 1000)
        self.assertEqual(result["reason"], "sect_practice_assets_write_rejected")
        self.assertEqual(self.state(), before)

    def test_abort_is_transparent_and_rolls_back(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER abort_practice_receipt BEFORE INSERT ON sect_practice_operations "
                "BEGIN SELECT RAISE(ABORT,'practice receipt failed'); END"
            )
        before = self.state()
        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.upgrade("abort", "user", 1, "attack", 2, 3, 100, 1000)
        self.assertEqual(self.state(), before)

    def test_same_operation_conflict_is_explicit(self):
        self.assertEqual(
            self.repository.upgrade("conflict", "user", 1, "attack", 2, 3, 100, 1000)["status"],
            "upgraded",
        )
        result = self.repository.upgrade("conflict", "user", 1, "attack", 2, 4, 100, 1000)
        self.assertEqual(
            (result["status"], result["reason"], result["subject"]),
            ("state_changed", "operation_conflict", "conflict"),
        )


if __name__ == "__main__":
    unittest.main()
