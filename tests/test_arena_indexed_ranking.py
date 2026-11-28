"""Live arena index reads retain the legacy sort and personal-rank rules."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_exact_rank_indexes
from nonebot_plugin_xiuxian_2.features.arena.state_repository import ArenaStateRepository
from nonebot_plugin_xiuxian_2.features.base.indexed_ranking import (
    ARENA_SCORE_ORDER_INDEX,
    ARENA_SCORE_RANK_INDEX,
    rank_index_available,
)
from nonebot_plugin_xiuxian_2.features.base.ranking_position import PersonalRank
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


LEGACY_TOP = (
    "SELECT user_id,COALESCE(score,0) AS score FROM arena "
    "ORDER BY xiuxian_int_text(COALESCE(score,0)) COLLATE xiuxian_integer DESC LIMIT ?"
)
LEGACY_RANK = (
    "WITH target AS ("
    "SELECT EXISTS(SELECT 1 FROM arena WHERE user_id=?) AS present,"
    "COALESCE((SELECT score FROM arena WHERE user_id=? ORDER BY rowid ASC LIMIT 1),0) AS value"
    ") SELECT target.value AS value,"
    "COUNT(CASE WHEN xiuxian_int_cmp(COALESCE(arena.score,0),0)>0 THEN 1 END) AS total,"
    "COUNT(CASE WHEN target.present=1 AND "
    "xiuxian_int_cmp(COALESCE(arena.score,0),target.value)>0 THEN 1 END) AS better "
    "FROM target LEFT JOIN arena ON 1=1 GROUP BY target.present,target.value"
)


class ArenaIndexedRankingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            # Legacy databases can retain duplicate IDs and untyped scores.
            uow.execute("CREATE TABLE arena(user_id,score)")
        self.repository = ArenaStateRepository(self.database)

    def _seed(self, rows):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO arena(user_id,score) VALUES(?,?)", rows)

    def _migrate(self):
        with DatabaseUnitOfWork(self.database) as uow:
            apply_arena_exact_rank_indexes(uow)

    def _legacy_top(self, limit):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(LEGACY_TOP, (max(0, int(limit)),))
        return tuple((str(row["user_id"]), as_int_like(row["score"])) for row in rows)

    def _legacy_rank(self, user_id):
        user_id = str(user_id).strip()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(LEGACY_RANK, (user_id, user_id)) or {}
        value = max(0, as_int_like(row.get("value")))
        return PersonalRank(
            int(row.get("better") or 0) + 1 if value > 0 else 0,
            int(row.get("total") or 0), value,
        )

    def _numeric_rows(self):
        return (
            ("leader", b"100000000000000000000000000000000007"),
            ("tie-a", "1e3"), ("tie-b", 1000), ("viewer", 900.9),
            ("duplicate", 200), ("duplicate", 5000),
            ("negative", -5), ("zero", 0), ("null", None),
            ("invalid", "not-an-integer"), ("invalid-bytes", b"\xff"),
            ("control-positive", "\x1c3000"), ("decimal", "9e2"),
            ("infinity", "Infinity"), ("negative-huge", b"-9999999999999999999999999"),
            (None, 1000), (b"bytes", 1000), ("None", 998),
            (" spaced ", 997), ("b'bytes'", 996),
        )

    def test_numeric_sort_and_personal_rank_match_original_sql_with_legacy_rows(self):
        self._seed(self._numeric_rows())
        expected_top = {limit: self._legacy_top(limit) for limit in (0, 1, 5, 50, 1000)}
        actors = [row[0] for row in self._numeric_rows()] + ["missing", " viewer "]
        expected_rank = {str(actor): self._legacy_rank(actor) for actor in actors}
        self._migrate()

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertTrue(rank_index_available(uow, self.database, ARENA_SCORE_ORDER_INDEX))
            self.assertTrue(rank_index_available(uow, self.database, ARENA_SCORE_RANK_INDEX))
        for limit, expected in expected_top.items():
            with self.subTest(limit=limit):
                self.assertEqual(expected, self.repository.ranking(limit))
        for actor in actors:
            with self.subTest(actor=actor):
                expected = expected_rank[str(actor)]
                self.assertEqual(expected, self.repository.rank_of(actor))
                rows, rank = self.repository.ranking_with_rank(5, actor)
                self.assertEqual(expected_top[5], rows)
                self.assertEqual(expected, rank)
        self.assertEqual(200, self.repository.rank_of("duplicate").value)
        self.assertEqual(self.repository.rank_of("tie-a").rank, self.repository.rank_of("tie-b").rank)

    def test_top1000_preserves_rowid_ties_duplicate_first_match_and_outside_zero(self):
        self._seed((str(index), 1000) for index in range(1, 1003))
        self._seed((("2", 2000),))
        original = self._legacy_top(1000)
        self._migrate()
        indexed = self.repository.ranking(1000)
        self.assertEqual(original, indexed)

        def ordinal(actor):
            return next((position for position, (candidate, _score) in enumerate(indexed, 1)
                         if str(candidate) == str(actor)), 0)

        for actor, expected in (("2", 1), ("1", 2), ("999", 1000),
                                ("1000", 0), ("1001", 0), ("missing", 0), (" 1 ", 0)):
            with self.subTest(actor=actor):
                self.assertEqual(expected, ordinal(actor))

    def test_live_mutations_refresh_top_and_personal_rank_without_cached_values(self):
        self._seed(self._numeric_rows())
        self._migrate()
        before = self.repository.ranking_with_rank(5, "viewer")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE arena SET score=6000 WHERE user_id='viewer'")
            uow.execute("INSERT INTO arena(user_id,score) VALUES('new-rival',7000)")
        rows, rank = self.repository.ranking_with_rank(5, "viewer")
        self.assertNotEqual(before, (rows, rank))
        self.assertEqual(self._legacy_top(5), rows)
        self.assertEqual(self._legacy_rank("viewer"), rank)

    def test_missing_and_mismatched_indexes_keep_original_queries_then_recover(self):
        self._seed(self._numeric_rows())
        expected = self._legacy_top(1000), self._legacy_rank("viewer")
        self.assertEqual(expected, self.repository.ranking_with_rank(1000, "viewer"))
        with DatabaseUnitOfWork(self.database) as uow:
            for spec in (ARENA_SCORE_ORDER_INDEX, ARENA_SCORE_RANK_INDEX):
                uow.execute(f'CREATE INDEX "{spec.name}" ON arena(user_id)')
        self.assertEqual(expected, self.repository.ranking_with_rank(1000, "viewer"))
        with DatabaseUnitOfWork(self.database) as uow:
            for spec in (ARENA_SCORE_ORDER_INDEX, ARENA_SCORE_RANK_INDEX):
                self.assertFalse(rank_index_available(uow, self.database, spec))
                uow.execute(f'DROP INDEX "{spec.name}"')
        self._migrate()
        self._migrate()
        self.assertEqual(expected, self.repository.ranking_with_rank(1000, "viewer"))

    def test_indexed_queries_use_score_ranges_without_an_order_sort(self):
        self._seed(self._numeric_rows())
        self._migrate()
        statements = []
        original_execute = DatabaseUnitOfWork.execute

        def record(uow, sql, params=()):
            if sql.startswith("SELECT user_id,COALESCE(score,0)") or sql.startswith("WITH target AS"):
                statements.append((sql, params))
            return original_execute(uow, sql, params)

        with patch.object(DatabaseUnitOfWork, "execute", record):
            self.repository.ranking_with_rank(1000, "viewer")
        self.assertEqual(2, len(statements))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            plans = [[str(row[3]) for row in uow.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()]
                     for sql, params in statements]
        self.assertTrue(any(ARENA_SCORE_ORDER_INDEX.name in step for step in plans[0]))
        self.assertTrue(any(ARENA_SCORE_RANK_INDEX.name in step for step in plans[1]))
        self.assertFalse(any("TEMP B-TREE FOR ORDER BY" in step for plan in plans for step in plan))

    def test_indexed_reads_are_readonly_and_read_denials_propagate_then_recover(self):
        self._seed(self._numeric_rows())
        self._migrate()
        expected = self._legacy_top(1000), self._legacy_rank("viewer")
        original_enter = DatabaseUnitOfWork.__enter__
        seen = []

        def enter_readonly(uow):
            result = original_enter(uow)
            seen.append(uow.read_only)
            return result

        with patch.object(DatabaseUnitOfWork, "__enter__", enter_readonly):
            self.assertEqual(expected, self.repository.ranking_with_rank(1000, "viewer"))
        self.assertTrue(seen)
        self.assertTrue(all(seen))

        def deny_score(uow):
            result = original_enter(uow)
            uow.connection.set_authorizer(
                lambda action, table, column, _database, _trigger:
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "arena"
                and column == "score" else sqlite3.SQLITE_OK
            )
            return result

        with patch.object(DatabaseUnitOfWork, "__enter__", deny_score):
            for read in (lambda: self.repository.ranking(1000), lambda: self.repository.rank_of("viewer")):
                with self.subTest(read=read), self.assertRaises(sqlite3.DatabaseError):
                    read()
        self.assertEqual(expected, self.repository.ranking_with_rank(1000, "viewer"))
        self.assertEqual(expected[0], self._legacy_top(1000))


if __name__ == "__main__":
    unittest.main()
