from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest

from ..history_repository import MessageHistoryRepository


class MessageHistoryRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="message-history-")
        self.root = Path(self.temp.name)
        self.database = self.root / "message.db"
        self._create_database()
        self.repository = MessageHistoryRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _create_database(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.execute(
                "CREATE TABLE messages("
                "id INTEGER PRIMARY KEY, adapter TEXT, bot_id TEXT, direction TEXT, scene TEXT, "
                "message_id TEXT, reference_id TEXT, source_message_id TEXT, group_id TEXT, "
                "group_name TEXT, user_id TEXT, username TEXT, nickname TEXT, avatar TEXT, "
                "content TEXT, reply_used_count INTEGER, created_at TEXT)"
            )
            conn.executemany(
                "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1, "QQ", "b1", "recv", "group", "m1", "", "", "g1", "Group One", "u1", "Alice", "", "", "needle first", 0, "2026-10-08 10:00:00"),
                    (2, "QQ", "b1", "recv", "group", "m2", "", "", "g2", "Group Two", "u2", "Bob", "", "", "needle second", 0, "2026-10-08 11:00:00"),
                    (3, "QQ", "b1", "recv", "group", "m3", "", "", "g1", "Group One", "u1", "Alice", "", "", "older id newer row", 0, "2026-10-08 09:00:00"),
                    (4, "QQ", "b1", "recv", "private", "m4", "", "", "", "", "u3", "Carol", "C", "", "private needle", 0, "2026-10-08 12:00:00"),
                    (5, "OneBot V11", "b2", "recv", "group", "m5", "", "", "g1", "Other Adapter", "u1", "Alice", "", "", "needle other adapter", 0, "2026-10-08 14:00:00"),
                    (6, "QQ", "b1", "send", "group", "s6", "", "m1", "g1", "Group One", "", "Bot", "Bot", "", "sent needle", 0, "2026-10-08 13:00:00"),
                    (7, "QQ", "b1", "recv", "private", "m7", "", "", "", "", "u3", "Carol", "C", "", "private later", 0, "2026-10-07 12:00:00"),
                    (8, "QQ", "b1", "recv", "group", "m8", "", "", "g1", "Group One", "u1", "Alice", "", "", "previous date", 0, "2026-10-07 10:00:00"),
                ],
            )

    def _insert_batch_rows(self, start_id: int, count: int) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.executemany(
                "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        row_id, "QQ", "b1", "recv", "group", f"m{row_id}", "", "",
                        "g1", "Group One", "u1", "Alice", "", "", "batch item", 0,
                        "2026-10-08 15:00:00",
                    )
                    for row_id in range(start_id, start_id + count)
                ],
            )

    def test_list_messages_filters_and_orders_like_legacy_route(self) -> None:
        result = self.repository.list_messages(
            scene="group",
            direction="recv",
            keyword="needle",
            group_id="g1",
            adapter="QQ",
            start="2026-10-08T09:59:00",
            end="2026-10-08T11:00:00",
            date="2026-10-08",
            page=1,
            page_size=10,
        )

        self.assertEqual(result["total"], 1)
        self.assertFalse(result["has_more"])
        self.assertEqual([row["id"] for row in result["rows"]], [1])

    def test_list_messages_preserves_page_size_floor_and_optional_total(self) -> None:
        with sqlite3.connect(self.database) as conn:
            conn.executemany(
                "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (row_id, "QQ", "b1", "recv", "group", f"m{row_id}", "", "", "g1", "Group One", "u1", "Alice", "", "", "page", 0, f"2026-10-08 16:{row_id:02d}:00")
                    for row_id in range(20, 32)
                ],
            )

        first = self.repository.list_messages(
            scene="group", group_id="g1", page_size=1, include_total="0"
        )
        second = self.repository.list_messages(
            scene="group", group_id="g1", page=2, page_size=1, include_total=True
        )

        self.assertEqual(len(first["rows"]), 10)
        self.assertTrue(first["has_more"])
        self.assertEqual(first["total"], 10)
        self.assertEqual(second["page_size"], 10)
        self.assertEqual(second["total"], 17)
        self.assertFalse(second["has_more"])

    def test_dates_counts_and_distinct_mode(self) -> None:
        counted = self.repository.dates(
            scene="group", target_id="g1", adapter="QQ", today="2026-10-08"
        )
        distinct = self.repository.dates(
            scene="group", target_id="g1", adapter="QQ", include_counts="0", today="2026-10-08"
        )

        self.assertEqual(
            counted["rows"],
            [
                {"date": "2026-10-08", "label": "今天", "count": 3},
                {"date": "2026-10-07", "label": "10月07日", "count": 1},
            ],
        )
        self.assertEqual(
            distinct["rows"],
            [
                {"date": "2026-10-08", "label": "今天", "count": None},
                {"date": "2026-10-07", "label": "10月07日", "count": None},
            ],
        )
        self.assertEqual(
            self.repository.dates(scene="invalid", target_id="g1"),
            {"success": False, "error": "无效 scene"},
        )
        self.assertEqual(
            self.repository.dates(scene="group", target_id=""),
            {"success": False, "error": "缺少 target_id"},
        )

    def test_sessions_use_max_id_per_target_and_legacy_sort_order(self) -> None:
        groups = self.repository.sessions(scene="group", adapter="QQ")
        private = self.repository.sessions(scene="private", adapter="QQ")

        self.assertEqual([row["target_id"] for row in groups["rows"]], ["g2", "g1"])
        self.assertEqual([row["last_row_id"] for row in groups["rows"]], [2, 8])
        self.assertEqual(groups["rows"][1]["last_time"], "2026-10-07 10:00:00")
        self.assertEqual(groups["last_row_id"], 8)
        self.assertEqual(private["rows"][0]["target_id"], "u3")
        self.assertEqual(private["rows"][0]["last_row_id"], 7)
        self.assertEqual(private["rows"][0]["title"], "Carol")

    def test_sessions_since_filters_before_grouping_and_sorts_by_id(self) -> None:
        result = self.repository.sessions_since(
            scene="group", adapter="QQ", after_id="1"
        )

        self.assertEqual([row["last_row_id"] for row in result["rows"]], [8, 2])
        self.assertEqual(result["last_row_id"], 8)

    def test_list_since_filters_and_returns_ascending_incremental_rows(self) -> None:
        result = self.repository.list_since(
            scene="group",
            target_id="g1",
            adapter="QQ",
            date="2026-10-08",
            last_row_id=1,
        )

        self.assertEqual([row["id"] for row in result["rows"]], [3, 6])
        self.assertEqual(result["last_row_id"], 6)
        self.assertEqual(
            self.repository.list_since(scene="invalid", target_id="g1"),
            {"success": False, "error": "无效 scene"},
        )

    def test_list_before_caps_page_and_reports_has_more(self) -> None:
        self._insert_batch_rows(100, 52)

        first = self.repository.list_before(
            scene="group", target_id="g1", keyword="batch", before_row_id=200, page_size=1
        )
        second = self.repository.list_before(
            scene="group", target_id="g1", keyword="batch", before_row_id=first["oldest_row_id"], page_size=50
        )

        self.assertEqual(len(first["rows"]), 50)
        self.assertTrue(first["has_more"])
        self.assertEqual(first["rows"][0]["id"], 151)
        self.assertEqual(first["oldest_row_id"], 102)
        self.assertEqual([row["id"] for row in second["rows"]], [101, 100])
        self.assertFalse(second["has_more"])

    def test_presenter_reuses_active_read_only_connection(self) -> None:
        seen = {}

        def present(rows, connection):
            seen["count"] = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("UPDATE messages SET content='changed' WHERE id=1")
            for row in rows:
                row["presented"] = True
            return rows

        result = self.repository.list_messages(scene="group", page_size=10, presenter=present)

        self.assertEqual(seen["count"], 8)
        self.assertTrue(all(row["presented"] for row in result["rows"]))
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(conn.execute("SELECT content FROM messages WHERE id=1").fetchone()[0], "needle first")

    def test_missing_database_or_table_fails_without_initialization(self) -> None:
        missing = self.root / "missing.db"
        with self.assertRaises(FileNotFoundError):
            MessageHistoryRepository(missing).list_messages()
        self.assertFalse(missing.exists())

        empty = self.root / "empty.db"
        sqlite3.connect(empty).close()
        with self.assertRaisesRegex(RuntimeError, "缺少 messages 表"):
            MessageHistoryRepository(empty).dates(scene="group", target_id="g1")
        with sqlite3.connect(empty) as conn:
            self.assertEqual(
                conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), []
            )


if __name__ == "__main__":
    unittest.main()
