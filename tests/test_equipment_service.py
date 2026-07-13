from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.equipment_service import (
    EquipmentService,
)
from tests.test_db_backend import db_backend


class EquipmentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "equipment.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TABLE back (
                    user_id TEXT NOT NULL,
                    goods_id INTEGER NOT NULL,
                    goods_num INTEGER NOT NULL,
                    state INTEGER DEFAULT 0,
                    update_time TEXT,
                    action_time TEXT,
                    UNIQUE (user_id, goods_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE BuffInfo (
                    user_id TEXT PRIMARY KEY,
                    faqi_buff INTEGER DEFAULT 0,
                    armor_buff INTEGER DEFAULT 0
                )
                """
            )
            conn.execute("INSERT INTO BuffInfo VALUES (%s, %s, %s)", ("u1", 101, 0))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, NULL, NULL)", ("u1", 101, 1, 1))
            conn.execute("INSERT INTO back VALUES (%s, %s, %s, %s, NULL, NULL)", ("u1", 102, 1, 0))
        self.service = EquipmentService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def scalar(self, sql, params=()):
        with db_backend.connection(self.database) as conn:
            row = conn.execute(sql, params).fetchone()
            return row[0] if row else None

    def test_equipment_replacement_updates_slot_and_both_item_states(self) -> None:
        result = self.service.change("equip-1", "u1", 102, "法器", equip=True)

        self.assertEqual(result.status, "equipped")
        self.assertEqual(result.previous_id, 101)
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 102)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 0)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 102)), 1)

    def test_unequip_clears_slot_and_item_state(self) -> None:
        result = self.service.change("unequip-1", "u1", 101, "法器", equip=False)

        self.assertEqual(result.status, "unequipped")
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 0)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 0)

    def test_repeated_event_is_idempotent(self) -> None:
        first = self.service.change("equip-repeat", "u1", 102, "法器", equip=True)
        second = self.service.change("equip-repeat", "u1", 102, "法器", equip=True)

        self.assertEqual(first.status, "equipped")
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 102)

    def test_reused_operation_with_different_payload_is_rejected(self) -> None:
        first = self.service.change("conflict", "u1", 102, "法器", equip=True)
        conflict = self.service.change("conflict", "u1", 101, "法器", equip=True)
        self.assertEqual((first.status, conflict.status), ("equipped", "state_changed"))
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 102)

    def test_missing_item_and_wrong_unequip_do_not_mutate_state(self) -> None:
        missing = self.service.change("missing", "u1", 999, "法器", equip=True)
        wrong = self.service.change("wrong", "u1", 102, "法器", equip=False)

        self.assertEqual(missing.status, "item_missing")
        self.assertEqual(wrong.status, "not_equipped")
        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 101)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 1)

    def test_database_failure_rolls_back_previous_and_new_equipment(self) -> None:
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                """
                CREATE TRIGGER fail_buff BEFORE UPDATE ON BuffInfo
                BEGIN SELECT RAISE(ABORT, 'buff failed'); END
                """
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.change("equip-fail", "u1", 102, "法器", equip=True)

        self.assertEqual(self.scalar("SELECT faqi_buff FROM BuffInfo WHERE user_id=%s", ("u1",)), 101)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 101)), 1)
        self.assertEqual(self.scalar("SELECT state FROM back WHERE user_id=%s AND goods_id=%s", ("u1", 102)), 0)


if __name__ == "__main__":
    unittest.main()
