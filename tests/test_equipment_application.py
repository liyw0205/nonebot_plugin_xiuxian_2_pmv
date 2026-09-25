from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.back.equipment_application import EquipmentApplication
from nonebot_plugin_xiuxian_2.features.back.migrations import apply_equipment
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class EquipmentApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "equipment.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
                "goods_num INTEGER NOT NULL,state INTEGER DEFAULT 0,update_time TEXT,"
                "action_time TEXT,UNIQUE(user_id,goods_id))"
            )
            conn.execute(
                "CREATE TABLE BuffInfo (user_id TEXT PRIMARY KEY,faqi_buff INTEGER DEFAULT 0,"
                "armor_buff INTEGER DEFAULT 0)"
            )
            conn.execute("INSERT INTO BuffInfo VALUES (%s,%s,%s)", ("u1", 101, 0))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("u1", 101, 1, 1))
            conn.execute("INSERT INTO back VALUES (%s,%s,%s,%s,NULL,NULL)", ("u1", 102, 1, 0))
        with DatabaseUnitOfWork(self.database) as uow:
            apply_equipment(uow)
        self.application = EquipmentApplication(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_equipment_replacement_updates_slot_and_item_states(self) -> None:
        result = self.application.change("equip-1", "u1", 102, "法器", equip=True)

        self.assertEqual((result.status, result.previous_id), ("equipped", 101))
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 102)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 0)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 102)), 1)

    def test_unequip_and_duplicate_replay(self) -> None:
        unequipped = self.application.change("unequip-1", "u1", 101, "法器", equip=False)
        first = self.application.change("equip-repeat", "u1", 102, "法器", equip=True)
        second = self.application.change("equip-repeat", "u1", 102, "法器", equip=True)

        self.assertEqual(unequipped.status, "unequipped")
        self.assertEqual((first.status, second.status), ("equipped", "duplicate"))
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 102)

    def test_conflict_and_invalid_transitions_do_not_mutate(self) -> None:
        first = self.application.change("conflict", "u1", 102, "法器", equip=True)
        conflict = self.application.change("conflict", "u1", 101, "法器", equip=True)
        missing = self.application.change("missing", "u1", 999, "法器", equip=True)
        wrong = self.application.change("wrong", "u1", 101, "法器", equip=False)

        self.assertEqual((first.status, conflict.status, missing.status, wrong.status), ("equipped", "state_changed", "item_missing", "not_equipped"))
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 102)

    def test_trigger_failure_rolls_back(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_equipment BEFORE UPDATE ON BuffInfo "
                "BEGIN SELECT RAISE(ABORT,'equipment failed'); END"
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.application.change("equip-fail", "u1", 102, "法器", equip=True)
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 101)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 1)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 102)), 0)

    def test_request_does_not_create_missing_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.sqlite3"
            with db_backend.transaction(database) as conn:
                conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,state INTEGER)")
                conn.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,faqi_buff INTEGER)")
                conn.execute("INSERT INTO BuffInfo VALUES ('u',0)")
                conn.execute("INSERT INTO back VALUES ('u',1,1,0)")
            with self.assertRaises(sqlite3.OperationalError):
                EquipmentApplication(database).change("missing-schema", "u", 1, "法器", equip=True)
            with DatabaseUnitOfWork(database) as uow:
                self.assertIsNone(uow.query_one("SELECT name FROM sqlite_master WHERE name='equipment_operations'"))

    def test_migration_is_routed_only_to_game_database(self) -> None:
        migrations = build_migrations()
        routed = {
            key: {migration.version for migration in migrations_for_database(migrations, key)}
            for key in ("game_db", "player_db", "trade_db", "impart_db", "message_db")
        }
        self.assertIn("back.013", routed["game_db"])
        for key in routed:
            if key != "game_db":
                self.assertNotIn("back.013", routed[key])


if __name__ == "__main__":
    unittest.main()
