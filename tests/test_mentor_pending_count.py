"""Count pending applications without dropping expiry or lifecycle semantics."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tests.test_mentor_application_lifecycle import MentorApplicationService, db_backend


class MentorPendingCountTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "player.db"
        self.service = MentorApplicationService(self.database)
        with db_backend.transaction(self.database) as conn:
            self.service._ensure_schema(conn)
            conn.executemany(
                "INSERT INTO mentor_applications VALUES(%s,%s,%s,%s,%s,%s,%s)",
                [("live", "m", "a", "pending", 1, 101, None),
                 ("edge", "m", "b", "pending", 2, 100, None),
                 ("other-expired", "other", "c", "pending", 3, 99, None),
                 ("other-live", "other", "d", "pending", 4, 200, None),
                 ("accepted", "m", "e", "accepted", 5, 1, 7),
                 ("rejected", "m", "f", "rejected", 6, 1, 8)],
            )

    def snapshot(self):
        with db_backend.connection(self.database) as conn:
            return conn.execute(
                "SELECT invite_id,status,resolved_at FROM mentor_applications ORDER BY invite_id"
            ).fetchall()

    def test_count_preserves_global_expiry_and_its_exact_boundary(self):
        with patch.object(self.service, "_application", side_effect=AssertionError("materialized list")):
            self.assertEqual(1, self.service.count_pending("m", now=100))
        rows = {row[0]: tuple(row[1:]) for row in self.snapshot()}
        self.assertEqual(("expired", 100), rows["edge"])
        self.assertEqual(("expired", 100), rows["other-expired"])
        self.assertEqual(("pending", None), rows["live"])
        self.assertEqual(("pending", None), rows["other-live"])
        self.assertEqual(("accepted", 7), rows["accepted"])
        self.assertEqual(("rejected", 8), rows["rejected"])
        self.assertEqual(0, self.service.count_pending("m", now=101))
        self.assertEqual(("expired", 100),
                         {row[0]: tuple(row[1:]) for row in self.snapshot()}["edge"])

    def test_new_service_sees_external_resolutions_and_matches_live_list(self):
        self.assertEqual(1, self.service.count_pending("m", now=100))
        self.assertEqual("applied", self.service.resolve("live", "m", "a", "rejected", now=100).status)
        recovered = MentorApplicationService(self.database)
        self.assertEqual(0, recovered.count_pending("m", now=100))
        self.assertEqual(len(recovered.list_pending("other", now=100)),
                         recovered.count_pending("other", now=100))
        self.assertEqual(0, recovered.count_pending("missing", now=100))

    def test_expired_unique_slot_is_reusable_by_existing_create_path(self):
        self.assertEqual(1, self.service.count_pending("m", now=100))
        created = self.service.create("new", "other", "b", now=100, ttl_seconds=60)
        self.assertEqual("applied", created.status)
        self.assertEqual(2, self.service.count_pending("other", now=100))
        self.assertEqual("new", self.service.find_pending_by_apprentice("b", now=100).invite_id)

    def test_count_failure_rolls_back_expiry_and_recovers(self):
        before = self.snapshot()
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER deny_expiry BEFORE UPDATE ON mentor_applications "
                "WHEN NEW.status='expired' BEGIN SELECT RAISE(ABORT,'deny expiry'); END"
            )
        with self.assertRaisesRegex(Exception, "deny expiry"):
            self.service.count_pending("m", now=100)
        self.assertEqual(before, self.snapshot())
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER deny_expiry")
        self.assertEqual(1, self.service.count_pending("m", now=100))

    def test_count_read_failure_rolls_back_already_executed_global_expiry(self):
        before = self.snapshot()
        expire = self.service._expire_stale

        def expire_then_deny(conn, now):
            expire(conn, now)
            self.assertEqual("expired", conn.execute(
                "SELECT status FROM mentor_applications WHERE invite_id='other-expired'"
            ).fetchone()[0])
            conn._raw.set_authorizer(
                lambda action, table, *_: sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "mentor_applications" else sqlite3.SQLITE_OK
            )

        with patch.object(self.service, "_expire_stale", side_effect=expire_then_deny):
            with self.assertRaises(sqlite3.DatabaseError):
                self.service.count_pending("m", now=100)
        self.assertEqual(before, self.snapshot())
        self.assertEqual(1, self.service.count_pending("m", now=100))

    def test_new_database_and_historical_same_apprentice_preserve_count_equivalence(self):
        empty = MentorApplicationService(self.database.with_name("empty.db"))
        self.assertEqual(0, empty.count_pending("m", now=100))
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO mentor_applications VALUES('historical','m','a','accepted',0,1,2)"
            )
        self.assertEqual(1, self.service.count_pending("m", now=100))
        self.assertEqual(len(self.service.list_pending("m", now=100)),
                         self.service.count_pending("m", now=100))

    def test_expiry_count_and_ordered_listing_use_bounded_index_ranges(self):
        with db_backend.transaction(self.database) as conn:
            conn.executemany(
                "INSERT INTO mentor_applications VALUES(%s,%s,%s,'accepted',0,1,2)",
                [(f"history-{i}", f"mentor-{i % 4}", f"old-{i}") for i in range(256)],
            )
            self.service._ensure_schema(conn)
            queries = [
                ("UPDATE mentor_applications SET status='expired',resolved_at=%s "
                 "WHERE status='pending' AND expires_at<=%s", (100, 100),
                 "mentor_application_pending_expiry"),
                ("SELECT COUNT(*) FROM mentor_applications WHERE mentor_id=%s AND status='pending'",
                 ("m",), "mentor_application_pending_mentor"),
                ("SELECT invite_id FROM mentor_applications WHERE mentor_id=%s "
                 "AND status='pending' ORDER BY created_at", ("m",),
                 "mentor_application_pending_mentor"),
            ]
            for query, parameters, index in queries:
                with self.subTest(query=query):
                    plan = conn.execute("EXPLAIN QUERY PLAN " + query, parameters).fetchall()
                    detail = " ".join(str(row[3]) for row in plan)
                    self.assertIn(index, detail)
                    self.assertNotIn("TEMP B-TREE", detail)
                    self.assertIn("SEARCH", detail)

    def test_display_count_does_not_fetch_or_materialize_invitation_rows(self):
        from tests.test_relationship_display_reads import modules
        partner, _, _ = modules()
        with patch.object(partner, "_mentor_application_service", return_value=self.service), \
                patch.object(partner, "_get_pending_mentor_invites", side_effect=AssertionError("list")), \
                patch.object(self.service, "count_pending", return_value=128) as count:
            self.assertEqual(128, partner._count_pending_mentor_invites("m"))
            count.assert_called_once_with("m")
