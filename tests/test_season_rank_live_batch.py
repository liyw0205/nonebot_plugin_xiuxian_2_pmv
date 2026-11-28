from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import season_rank_service as ranks
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.json_store import safe_json_loads
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
from tests.test_db_backend import db_backend


class SeasonRankLiveBatchTests(unittest.TestCase):
    now = date(2026, 11, 28)

    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / "game.db"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE season_rank(rank_key TEXT,mode TEXT,period_key TEXT,"
                "rank_type TEXT,user_id TEXT,sect_id INTEGER,score,extra TEXT,updated_at TEXT)"
            )
            conn.execute("CREATE INDEX rank_subject ON season_rank(rank_key,user_id)")
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
        self.connection = db_backend.connect(self.database)
        self.addCleanup(self.connection.close)
        self.calls = []
        self.manager = SimpleNamespace(
            database_path=self.database,
            conn=self.connection,
            lock=threading.RLock(),
            _read_query=self._read,
        )
        manager_patch = patch.object(ranks, "_sql_message", return_value=self.manager)
        manager_patch.start()
        self.addCleanup(manager_patch.stop)

    def _query(self, sql, params=(), *, one=False, dict_row=False):
        cursor = self.connection.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return (rows[0] if rows else None) if one else rows

    def _read(self, sql, params=(), *, one=False, dict_row=False):
        self.calls.append((sql, params))
        return self._query(sql, params, one=one, dict_row=dict_row)

    def _insert(
        self, rank_type, score, *, mode="monthly", user_id="player", sect_id=0,
        updated_at="b", extra='{"source":"fixture"}',
    ):
        rank_key, normalized, period = ranks._build_rank_identity(rank_type, mode, self.now)
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "INSERT INTO season_rank VALUES(?,?,?,?,?,?,?,?,?)",
                (rank_key, normalized, period, rank_type, user_id, sect_id, score, extra, updated_at),
            )
        return rank_key

    def _reference(self, user_id, rank_type, mode="monthly"):
        # Independent legacy two-query oracle, including its target coercion.
        rank_key = ranks._build_rank_identity(rank_type, mode, self.now)[0]
        row = self._query(
            "SELECT r.rank_key,r.mode,r.period_key,r.rank_type,r.user_id,r.sect_id,"
            "r.score,r.extra,r.updated_at,COALESCE(u.user_name,r.user_id) AS user_name "
            "FROM season_rank AS r LEFT JOIN user_xiuxian AS u "
            "ON u.user_id=r.user_id AND r.user_id<>'' "
            "WHERE r.rank_key=? AND r.user_id=? "
            "ORDER BY xiuxian_int_text(r.score) COLLATE xiuxian_integer DESC,"
            "r.updated_at ASC LIMIT 1",
            (rank_key, str(user_id)), one=True,
        )
        if row is None:
            return None
        position = self._query(
            "SELECT COUNT(1)+1 AS rank_value FROM season_rank "
            "WHERE rank_key=? AND xiuxian_int_cmp(score,?)>0",
            (rank_key, as_int_like(row["score"])), one=True,
        )
        row["rank"] = position["rank_value"]
        row["score"] = as_int_like(row["score"])
        row["extra_data"] = safe_json_loads(row["extra"], {})
        return row

    def test_default_fifteen_boards_share_one_schema_check_and_one_business_read(self):
        expected = []
        for mode in ranks.SEASON_RANK_MODES:
            for rank_type in ranks.DEFAULT_SEASON_RANK_TYPES:
                self._insert(rank_type, "10", mode=mode)
                self._insert(rank_type, "11", mode=mode, user_id="higher")
                expected.append(self._reference("player", rank_type, mode))
        statements = []
        self.connection._raw.set_trace_callback(statements.append)
        with patch.object(ranks, "ensure_season_rank_table", wraps=ranks.ensure_season_rank_table) as ensure:
            actual = ranks.get_user_current_season_entries("player", now=self.now)
        self.assertEqual(expected, actual)
        self.assertEqual(15, len(actual))
        self.assertEqual(1, ensure.call_count)
        self.assertEqual(1, len(self.calls))
        self.assertEqual(1, sum("PRAGMA schema_version" in sql for sql in statements))
        self.assertEqual(30, len(self.calls[0][1]))

    def test_exact_legacy_score_coercion_for_large_text_blob_invalid_null_and_ties(self):
        values = (
            str(2**63), str(2**63 + 1), "100000000000000000000000000000001",
            b"1e30", "1e3", "12.9", "not-number", None, b"\xff", "-7", "NaN", "Infinity",
        )
        rank_types = []
        expected = []
        for index, value in enumerate(values):
            rank_type = f"numeric-{index}"
            rank_types.append(rank_type)
            self._insert(rank_type, value)
            self._insert(rank_type, value, user_id="tie")
            for other_index, other in enumerate(("-8", "0", "1", str(2**63 + 2), "NaN", None)):
                self._insert(rank_type, other, user_id=f"other-{other_index}")
            expected.append(self._reference("player", rank_type))
        actual = ranks.get_user_current_season_entries(
            "player", modes=("monthly",), rank_types=rank_types, now=self.now,
        )
        self.assertEqual(expected, actual)
        self.assertEqual(1, len(self.calls))

    def test_duplicate_subject_sect_and_join_names_keep_highest_then_oldest_row(self):
        with db_backend.transaction(self.database) as conn:
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?)",
                (("player", "first-name"), ("player", "second-name")),
            )
        self._insert("duplicates", "9", sect_id=1, updated_at="a")
        self._insert("duplicates", "20", sect_id=2, updated_at="z")
        self._insert("duplicates", b"2e1", sect_id=3, updated_at="a", extra="bad-json")
        self._insert("duplicates", "21", user_id="other", sect_id=4)
        self._insert("duplicates", "21", user_id="other", sect_id=5)
        expected = self._reference("player", "duplicates")
        actual = ranks.get_user_season_rank("player", "duplicates", now=self.now)
        self.assertEqual(expected, actual)
        self.assertEqual((3, 3, 20, {}), (actual["sect_id"], actual["rank"], actual["score"], actual["extra_data"]))
        self.assertEqual(1, len(self.calls))

    def test_missing_rows_are_omitted_and_missing_join_name_falls_back_to_user_id(self):
        self._insert("present", 1)
        rows = ranks.get_user_current_season_entries(
            "player", modes=("monthly",), rank_types=("absent", "present"), now=self.now,
        )
        self.assertEqual([self._reference("player", "present")], rows)
        self.assertEqual("player", rows[0]["user_name"])
        self.assertIsNone(ranks.get_user_season_rank("nobody", "present", now=self.now))

    def test_modes_and_rank_type_generator_are_materialized_once_and_keep_requested_order(self):
        produced = []

        def rank_types():
            for rank_type in (" second ", "first", "", "second"):
                produced.append(rank_type)
                yield rank_type

        for mode in ("weekly", "monthly"):
            self._insert("first", 1, mode=mode)
            self._insert("second", 2, mode=mode)
        rows = ranks.get_user_current_season_entries(
            "player", modes=("weekly", "monthly", "weekly"), rank_types=rank_types(), now=self.now,
        )
        self.assertEqual([" second ", "first", "", "second"], produced)
        self.assertEqual(
            [(mode, rank_type) for mode in ("weekly", "monthly") for rank_type in ("second", "first", "second")],
            [(row["mode"], row["rank_type"]) for row in rows],
        )
        self.assertEqual(1, len(self.calls))
        self.assertEqual(8, len(self.calls[0][1]))
        self.assertIsNot(rows[0], rows[2])
        self.assertIsNot(rows[0]["extra_data"], rows[2]["extra_data"])

    def test_batch_is_bounded_and_preserves_order_across_chunks(self):
        rank_types = tuple(f"board-{index}" for index in range(167))
        for index, rank_type in enumerate(rank_types):
            self._insert(rank_type, str(2**63 + index))
        rows = ranks.get_user_current_season_entries(
            "player", modes=("monthly",), rank_types=rank_types, now=self.now,
        )
        self.assertEqual(list(rank_types), [row["rank_type"] for row in rows])
        self.assertEqual([160, 160, 14], [len(params) for _, params in self.calls])
        self.assertTrue(all(len(params) <= 160 for _, params in self.calls))

    def test_legacy_rank_key_collations_keep_requested_lookup_and_stored_projection(self):
        for collation, stored_key in (("NOCASE", lambda key: key.upper()),
                                      ("RTRIM", lambda key: key + "  ")):
            with self.subTest(collation=collation):
                with db_backend.transaction(self.database) as conn:
                    conn.execute("DROP TABLE season_rank")
                    conn.execute(
                        f"CREATE TABLE season_rank(rank_key TEXT COLLATE {collation},"
                        "mode TEXT,period_key TEXT,rank_type TEXT,user_id TEXT,sect_id INTEGER,"
                        "score,extra TEXT,updated_at TEXT)"
                    )
                key = self._insert("legacy", "10")
                self._insert("legacy", "20", user_id="other")
                with db_backend.transaction(self.database) as conn:
                    conn.execute("UPDATE season_rank SET rank_key=?", (stored_key(key),))
                expected = self._reference("player", "legacy")
                self.assertEqual(stored_key(key), expected["rank_key"])
                self.assertEqual(2, expected["rank"])
                self.assertEqual(expected, ranks.get_user_season_rank("player", "legacy", now=self.now))
                self.assertEqual([expected, expected], ranks.get_user_current_season_entries(
                    "player", modes=("monthly",), rank_types=("legacy", "missing", "legacy"), now=self.now,
                ))

    def test_external_score_changes_are_live_without_caching_personal_values(self):
        key = self._insert("live", "10")
        self._insert("live", "20", user_id="other")
        first = ranks.get_user_current_season_entries(
            "player", modes=("monthly",), rank_types=("live",), now=self.now,
        )
        self.assertEqual((2, 10), (first[0]["rank"], first[0]["score"]))
        with db_backend.transaction(self.database) as conn:
            conn.execute("UPDATE season_rank SET score=? WHERE rank_key=? AND user_id=?", ("30", key, "player"))
            conn.execute("INSERT INTO user_xiuxian VALUES('player','fresh-name')")
        second = ranks.get_user_current_season_entries(
            "player", modes=("monthly",), rank_types=("live",), now=self.now,
        )
        self.assertEqual((1, 30, "fresh-name"), (second[0]["rank"], second[0]["score"], second[0]["user_name"]))
        self.assertEqual(2, len(self.calls))

    def test_target_score_and_position_share_one_statement_before_external_write(self):
        key = self._insert("consistent", "10")
        self._insert("consistent", "20", user_id="other")

        def read_then_change(sql, params=(), **kwargs):
            result = self._read(sql, params, **kwargs)
            with db_backend.transaction(self.database) as conn:
                conn.execute("UPDATE season_rank SET score='30' WHERE rank_key=? AND user_id='player'", (key,))
            return result

        with patch.object(self.manager, "_read_query", side_effect=read_then_change):
            first = ranks.get_user_season_rank("player", "consistent", now=self.now)
        self.assertEqual((2, 10), (first["rank"], first["score"]))
        self.assertEqual(1, len(self.calls))
        second = ranks.get_user_season_rank("player", "consistent", now=self.now)
        self.assertEqual((1, 30), (second["rank"], second["score"]))

    def test_absent_or_incomplete_schema_degrades_without_creating_tables(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TABLE season_rank")
        self.assertEqual([], ranks.get_user_current_season_entries("player", now=self.now))
        self.assertIsNone(ranks.get_user_season_rank("player", "missing", now=self.now))
        with db_backend.transaction(self.database) as conn:
            conn.execute("CREATE TABLE season_rank(rank_key TEXT)")
        self.assertEqual([], ranks.get_user_current_season_entries("player", now=self.now))
        self.assertEqual([], self.calls)
        columns = self._query("PRAGMA table_info(season_rank)")
        self.assertEqual(["rank_key"], [row["name"] for row in columns])

    def test_read_denial_remains_an_error_and_recovers_without_stale_snapshot(self):
        self._insert("denied", "10")

        def authorize(action, table, column, *_):
            if action == sqlite3.SQLITE_READ and table == "season_rank" and column == "score":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        self.connection._raw.set_authorizer(authorize)
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                ranks.get_user_current_season_entries(
                    "player", modes=("monthly",), rank_types=("denied",), now=self.now,
                )
        finally:
            self.connection._raw.set_authorizer(None)
        self.assertEqual(
            [self._reference("player", "denied")],
            ranks.get_user_current_season_entries(
                "player", modes=("monthly",), rank_types=("denied",), now=self.now,
            ),
        )


if __name__ == "__main__":
    unittest.main()
