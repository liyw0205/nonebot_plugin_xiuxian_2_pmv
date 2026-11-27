"""Batch activity reads preserve identity, parsing and live settlement checks."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class SectMaintenanceReadStageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / "sect.db"
        self.app = SectApplication(self.database)

    def activity_table(self, affinity="TEXT"):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(f"CREATE TABLE user_cd(user_id {affinity},last_check_info_time)")
            uow.execute("CREATE INDEX activity_identity ON user_cd(user_id)")

    def test_batch_matches_live_single_reader_with_missing_bad_and_duplicate_times(self):
        self.activity_table()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO user_cd VALUES(?,?)", [
                ("u", "2026-11-27 12:01:02.123456"), ("second", "2026-11-27 12:01:02"),
                ("u", "1999-01-01 00:00:00"), ("bad", "invalid"), ("empty", ""), ("null", None),
            ])
        identities = ["u", "second", "bad", "empty", "null", "missing", "u"]
        expected = {key: self.app.get_last_check_info_time(key) for key in identities}
        self.assertEqual(expected, self.app.get_last_check_info_times(iter(identities)))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE user_cd SET last_check_info_time='2026-11-28 00:00:00' WHERE user_id='u'")
        self.assertNotEqual(expected["u"], self.app.get_last_check_info_times(["u"])["u"])

    def test_integer_affinity_retains_each_requested_identity_and_first_duplicate(self):
        self.activity_table("INTEGER")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO user_cd VALUES(?,?)", [
                (1, "2026-11-27 00:00:00"), (1, "2026-11-28 00:00:00"),
                ("opaque", "2026-11-29 00:00:00"),
            ])
        identities = ["1", "01", "1e0", "opaque", "missing"]
        self.assertEqual({key: self.app.get_last_check_info_time(key) for key in identities},
                         self.app.get_last_check_info_times(identities))

    def test_empty_reads_no_database_and_large_batch_uses_one_readonly_uow(self):
        with patch.object(DatabaseUnitOfWork, "__enter__", side_effect=AssertionError("empty IO")):
            self.assertEqual({}, self.app.get_last_check_info_times([]))
        self.activity_table()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO user_cd VALUES(?,?)", [
                (str(index), "2026-11-27 00:00:00") for index in range(1001)])
        enter, query = DatabaseUnitOfWork.__enter__, DatabaseUnitOfWork.query_all
        opens, chunks = [], []

        def opened(uow):
            self.assertTrue(uow.read_only)
            opens.append(uow)
            return enter(uow)

        def queried(uow, sql, params=()):
            chunks.append(len(params))
            return query(uow, sql, params)

        with patch.object(DatabaseUnitOfWork, "__enter__", opened), patch.object(
                DatabaseUnitOfWork, "query_all", queried):
            result = self.app.get_last_check_info_times(str(index) for index in range(1001))
        self.assertEqual(1, len(opens))
        self.assertEqual([500, 500, 1], chunks)
        self.assertEqual(1001, len(result))
        self.assertTrue(all(value == datetime(2026, 11, 27).astimezone() for value in result.values()))

    def test_required_read_failure_propagates_and_recovers_without_writes(self):
        self.activity_table()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO user_cd VALUES('u','2026-11-27 00:00:00')")
        enter = DatabaseUnitOfWork.__enter__

        def denied(uow):
            result = enter(uow)
            uow.connection.set_authorizer(lambda action, table, *_:
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "user_cd"
                else sqlite3.SQLITE_OK)
            return result

        with patch.object(DatabaseUnitOfWork, "__enter__", denied), self.assertRaises(sqlite3.DatabaseError):
            self.app.get_last_check_info_times(["u"])
        self.assertEqual(self.app.get_last_check_info_time("u"), self.app.get_last_check_info_times(["u"])["u"])

    def test_actual_batch_plan_seeks_text_and_integer_identity_index(self):
        for affinity in ("TEXT", "INTEGER"):
            with self.subTest(affinity=affinity):
                if self.database.exists():
                    with DatabaseUnitOfWork(self.database) as uow:
                        uow.execute("DROP TABLE user_cd")
                if affinity == "TEXT":
                    with DatabaseUnitOfWork(self.database) as uow:
                        uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time)")
                else:
                    self.activity_table(affinity)
                query, statements = DatabaseUnitOfWork.query_all, []

                def collect(uow, sql, params=()):
                    statements.append((sql, params))
                    return query(uow, sql, params)

                with patch.object(DatabaseUnitOfWork, "query_all", collect):
                    self.app.get_last_check_info_times(["1", "01", "opaque"])
                self.assertEqual(1, len(statements))
                sql, params = statements[0]
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    details = [row["detail"] for row in uow.query_all("EXPLAIN QUERY PLAN " + sql, params)]
                expected_index = "sqlite_autoindex_user_cd_1" if affinity == "TEXT" else "activity_identity"
                self.assertTrue(any("SEARCH c USING INDEX " + expected_index in detail for detail in details))
                self.assertFalse(any("SCAN c" in detail for detail in details))

    def scheduled(self, branch):
        from tests.test_scheduled_job_failure_visibility import _sect_database, _execute_job
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect
        _sect_database(self.database, branch)
        binding = patch.object(sect, "sect_application", self.app)
        binding.start()
        self.addCleanup(binding.stop)
        clock = patch.object(sect, "runtime_clock", SimpleNamespace(
            now=lambda: datetime(2026, 11, 17, 12, tzinfo=timezone.utc)))
        clock.start()
        self.addCleanup(clock.stop)
        return lambda: _execute_job(sect.auto_handle_inactive_sect_owners)

    def test_scheduler_batches_candidates_and_avoids_unused_aggregate(self):
        fire = self.scheduled("inherit")
        with DatabaseUnitOfWork(self.database) as uow:
            for index in range(16):
                actor = f"candidate{index:02d}"
                uow.execute("INSERT INTO user_xiuxian VALUES(?,?,1,1,?,3,1)", (actor, actor, 100 + index))
                uow.execute("INSERT INTO user_cd VALUES(?,'2026-11-16 00:00:00')", (actor,))
        with patch.object(self.app, "list_sects_with_member_count", side_effect=AssertionError("unused aggregate")), \
                patch.object(self.app, "get_last_check_info_time", side_effect=AssertionError("N+1")), \
                patch.object(self.app, "get_last_check_info_times", wraps=self.app.get_last_check_info_times) as batch:
            result = fire()
        self.assertIsNone(result.exception if hasattr(result, "exception") else None)
        self.assertEqual(1, batch.call_count)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual("candidate15", uow.query_one("SELECT sect_owner FROM sects WHERE sect_id=1")["sect_owner"])
            self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS n FROM sect_owner_inherit_operations")["n"])

    def test_activity_changes_after_read_stage_still_block_stale_disband(self):
        fire = self.scheduled("no_successor")
        original = self.app.get_last_check_info_times

        def changed(identities):
            result = original(identities)
            with DatabaseUnitOfWork(self.database) as uow:
                uow.execute("UPDATE user_cd SET last_check_info_time='2026-11-16 00:00:00'")
            return result

        with patch.object(self.app, "get_last_check_info_times", changed):
            result = fire()
        self.assertIsNone(result.exception if hasattr(result, "exception") else None)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(1, uow.query_one("SELECT closed FROM sects WHERE sect_id=1")["closed"])
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM sect_inactive_disband_operations")["n"])
        fire()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual("candidate", uow.query_one("SELECT sect_owner FROM sects WHERE sect_id=1")["sect_owner"])


if __name__ == "__main__":
    unittest.main()
