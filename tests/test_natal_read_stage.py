from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch


def _modules():
    import nonebot

    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init(command_start={"", "/"}, log_level="ERROR")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure import natal_data
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import player_data_manager

    return natal_data, player_data_manager


class NatalReadStageTests(unittest.TestCase):
    def setUp(self):
        self.natal, module = _modules()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        manager = object.__new__(module.PlayerDataManager)
        manager.database_path = Path(temporary.name) / "player.db"
        manager.conn = module.db_backend.connect(manager.database_path)
        self.addCleanup(manager.conn.close)
        manager._conn_lock = threading.RLock()
        manager._ensured_tables = set()
        manager._ensured_fields = set()
        manager._field_list_cache = {}
        manager._field_list_cache_charged_bytes = 0
        self.manager = manager
        binding = patch.object(self.natal, "_player_data_manager", return_value=manager)
        binding.start()
        self.addCleanup(binding.stop)

    def _seed(self, user_id="u", *, form=1):
        treasure = self.natal.NatalTreasure(user_id)
        treasure._ensure_record()
        self.manager.update_or_write_data(user_id, treasure.table, "form", form, data_type="INTEGER")
        return treasure

    @contextmanager
    def _reads(self):
        statements = []

        def trace(statement):
            normalized = " ".join(statement.upper().split())
            if normalized.startswith("SELECT ") and (
                'FROM "NATAL_TREASURE"' in normalized
                or "FROM NATAL_TREASURE" in normalized
            ):
                statements.append(statement)

        self.manager.conn._raw.set_trace_callback(trace)
        try:
            yield statements
        finally:
            self.manager.conn._raw.set_trace_callback(None)

    def test_complete_exists_and_data_each_use_one_select_without_writes(self):
        treasure = self._seed()
        self.manager.update_or_write_data("u", treasure.table, "effect1_base_value", 0.5, data_type="REAL")
        before = self.manager.get_fields("u", treasure.table)
        changes = self.manager.conn._raw.total_changes

        with self._reads() as statements:
            self.assertTrue(treasure.exists())
        self.assertEqual(1, len(statements), statements)
        with self._reads() as statements:
            result = treasure.get_data()
        self.assertEqual(1, len(statements), statements)
        self.assertEqual(0.5, result["effect1_base_value"])
        self.assertEqual({}, result["soul_summon_count"])
        with self._reads() as statements:
            self.assertIs(result, treasure.get_data())
        self.assertEqual([], statements)
        self.assertEqual(changes, self.manager.conn._raw.total_changes)
        self.assertEqual(before, self.manager.get_fields("u", treasure.table))

    def test_exists_preserves_scalar_decoding_without_decoding_combat_fields(self):
        self.manager.conn.execute(
            "CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form TEXT)"
        )
        treasure = self._seed()
        self.manager.update_or_write_data("u", treasure.table, "soul_summon_count", "invalid-json")
        for stored, expected in (
            ("0", False), ("1", True), ("1.00000000000000000001e20", True),
            (sqlite3.Binary(b"0"), False), ("", True), ("unknown", True),
        ):
            with self.subTest(stored=stored):
                self.manager.conn.execute(
                    "UPDATE natal_treasure SET form=%s WHERE user_id='u'", (stored,)
                )
                self.manager.conn.commit()
                scalar = self.manager.get_field_data("u", treasure.table, "form")
                self.assertEqual(expected, scalar is not None and scalar != 0)
                with (
                    self._reads() as statements,
                    patch.object(treasure, "_decode_data", side_effect=AssertionError("combat decode")),
                ):
                    self.assertEqual(expected, treasure.exists())
                self.assertEqual(1, len(statements), statements)

    def test_missing_record_and_columns_read_actual_repaired_projection(self):
        self.manager.conn.execute(
            "CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form INTEGER)"
        )
        self.manager.conn.execute("INSERT INTO natal_treasure VALUES('partial',1)")
        self.manager.conn.execute(
            "CREATE TRIGGER inserted_form AFTER INSERT ON natal_treasure "
            "BEGIN UPDATE natal_treasure SET form=2 WHERE user_id=NEW.user_id; END"
        )
        self.manager.conn.commit()
        with self._reads() as statements:
            created = self.natal.NatalTreasure("missing").get_data()
        self.assertEqual(2, len(statements), statements)
        self.assertEqual(2, created["form"])
        self.assertIn("effect3_level", created)
        with self._reads() as statements:
            self.assertTrue(self.natal.NatalTreasure("missing_exists").exists())
        self.assertEqual(2, len(statements), statements)

        self.manager.conn.execute(
            "CREATE TRIGGER repaired_level AFTER UPDATE OF level ON natal_treasure "
            "WHEN NEW.level=0 BEGIN "
            "UPDATE natal_treasure SET level=7 WHERE user_id=NEW.user_id; END"
        )
        self.manager.conn.commit()
        with self._reads() as statements:
            repaired = self.natal.NatalTreasure("partial").get_data()
        self.assertEqual(2, len(statements), statements)
        self.assertEqual(1, repaired["form"])
        self.assertEqual(7, repaired["level"])
        self.assertEqual({}, repaired["enlightenment_count"])

    def test_null_form_exists_reads_back_repair_instead_of_assuming_default(self):
        treasure = self._seed()
        self.manager.conn.execute("UPDATE natal_treasure SET form=NULL WHERE user_id='u'")
        self.manager.conn.execute(
            "CREATE TRIGGER repaired_form AFTER UPDATE OF form ON natal_treasure "
            "WHEN NEW.form=0 BEGIN "
            "UPDATE natal_treasure SET form=3 WHERE user_id=NEW.user_id; END"
        )
        self.manager.conn.commit()
        with self._reads() as statements:
            self.assertTrue(treasure.exists())
        self.assertEqual(2, len(statements), statements)
        self.assertEqual(3, self.manager.get_field_data("u", treasure.table, "form"))

    def test_read_denial_is_not_hidden_and_strict_battle_does_not_attempt_repair(self):
        self._seed()
        before = self.manager.get_fields("u", "natal_treasure")
        for method in ("exists", "get_data", "read_battle_data"):
            with self.subTest(method=method):
                self.manager.conn._raw.set_authorizer(
                    lambda action, table, *_: sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "natal_treasure"
                    else sqlite3.SQLITE_OK
                )
                try:
                    with (
                        patch.object(
                            self.manager, "update_or_write_data", wraps=self.manager.update_or_write_data
                        ) as writes,
                        self.assertRaises(sqlite3.DatabaseError),
                    ):
                        getattr(self.natal.NatalTreasure("u"), method)()
                    if method == "read_battle_data":
                        writes.assert_not_called()
                finally:
                    self.manager.conn._raw.set_authorizer(None)
                    self.manager.conn.rollback()
                self.assertEqual(before, self.manager.get_fields("u", "natal_treasure"))
        self.assertTrue(self.natal.NatalTreasure("u").exists())

    def test_repair_write_error_propagates_without_caching_unverified_defaults(self):
        treasure = self._seed()
        self.manager.conn.execute("UPDATE natal_treasure SET form=NULL WHERE user_id='u'")
        self.manager.conn.execute(
            "CREATE TRIGGER refuse_repair BEFORE UPDATE OF form ON natal_treasure "
            "BEGIN SELECT RAISE(ABORT, 'repair refused'); END"
        )
        self.manager.conn.commit()
        for method in ("exists", "get_data"):
            with self.subTest(method=method):
                try:
                    with self.assertRaisesRegex(sqlite3.IntegrityError, "repair refused"):
                        getattr(treasure, method)()
                    self.assertIsNone(treasure._natal_data_cache)
                finally:
                    self.manager.conn.rollback()
        self.manager.conn.execute("DROP TRIGGER refuse_repair")
        self.manager.conn.commit()
        self.assertFalse(treasure.exists())

    def test_new_requests_and_exists_read_live_and_own_writes_clear_existing_cache(self):
        treasure = self._seed()
        first = treasure.get_data()
        with sqlite3.connect(self.manager.database_path) as connection:
            connection.execute("UPDATE natal_treasure SET level=4,form=0 WHERE user_id='u'")
        self.assertIs(first, treasure.get_data())
        fresh = self.natal.NatalTreasure("u").get_data()
        self.assertEqual((4, 0), (fresh["level"], fresh["form"]))
        self.assertFalse(treasure.exists())
        self.assertEqual(4, treasure.get_data()["level"])
        treasure.update_data("level", 9)
        self.assertIsNone(treasure._natal_data_cache)
        self.assertEqual(9, treasure.get_data()["level"])
        other = self._seed("v")
        self.assertEqual(0, other.get_data()["level"])

    def test_strict_battle_still_reads_live_and_rereads_repairs(self):
        treasure = self._seed()
        self.assertEqual(0, treasure.get_data()["level"])
        self.manager.update_or_write_data("u", treasure.table, "level", 6, data_type="INTEGER")
        with self._reads() as statements:
            self.assertEqual(6, treasure.read_battle_data()["level"])
        self.assertEqual(1, len(statements), statements)
        self.manager.conn.execute("UPDATE natal_treasure SET form=NULL WHERE user_id='u'")
        self.manager.conn.commit()
        with self._reads() as statements:
            self.assertIsNone(treasure.read_battle_data())
        self.assertEqual(2, len(statements), statements)
        self.assertEqual(0, self.manager.get_field_data("u", treasure.table, "form"))


if __name__ == "__main__":
    unittest.main()
