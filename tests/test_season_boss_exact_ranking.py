"""Exact range indexes remove population UDF work without caching live ranks."""
from datetime import date
from contextlib import nullcontext
from pathlib import Path
import json
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, sqlite_int_compare, sqlite_int_sort_key
from nonebot_plugin_xiuxian_2.features.base.indexed_ranking import (
    BOSS_INTEGRAL_ORDER_INDEX, BOSS_INTEGRAL_RANK_INDEX,
    SEASON_SCORE_ORDER_INDEX, SEASON_SCORE_RANK_INDEX, rank_index_available,
)
from nonebot_plugin_xiuxian_2.features.boss.integral_repository import BossIntegralSqlRepository
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_exact_rank_indexes
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, message_store
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import season_rank_service as season
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
from tests.test_db_backend import db_backend
from tests.test_grouped_ranking_read_stage import _legacy


class SeasonBossExactRankingTests(unittest.TestCase):
    now = date(2026, 11, 28)

    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.game, self.player = (Path(temporary.name) / name for name in ("game.db", "player.db"))
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
            uow.execute("CREATE TABLE sects(sect_id INTEGER,sect_name TEXT)")
            message_store.apply_season_rank_exact_indexes(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_boss_exact_rank_indexes(uow)
        self.connection = db_backend.connect(self.game)
        self.addCleanup(self.connection.close)
        self.statements = []
        self.manager = SimpleNamespace(conn=self.connection, database_path=self.game,
                                       lock=threading.RLock(), _read_query=self.read,
                                       _repository_read_connection=lambda: nullcontext(self.connection))
        active = patch.object(season, "_sql_message", return_value=self.manager)
        active.start()
        self.addCleanup(active.stop)
        self.boss = BossIntegralSqlRepository(self.player)

    def read(self, sql, params=(), *, dict_row=False, one=False):
        self.statements.append((sql, params))
        cursor = self.connection.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return (rows[0] if rows else None) if one else rows

    def seed(self):
        values = [str(10**80 + 1).encode(), str(10**80 + 1), str(10**80 + 2),
                  "1e3", "19.9", 19.9, 0, "-4", "bad", None, b"\xff", "NaN", "Infinity"]
        keys = []
        with DatabaseUnitOfWork(self.game) as uow:
            nullable_score = not next(row["notnull"] for row in uow.query_all("PRAGMA table_info(season_rank)")
                                      if row["name"] == "score")
            for mode in season.SEASON_RANK_MODES:
                for kind in season.DEFAULT_SEASON_RANK_TYPES:
                    key, normalized, period = season._build_rank_identity(kind, mode, self.now)
                    keys.append(key)
                    # Raw score affinity is intentionally absent in this legacy shape.
                    for index, value in enumerate(values):
                        uow.execute("INSERT INTO season_rank VALUES(?,?,?,?,?,0,?,'{}','a')",
                                    (key, normalized, period, kind, str(index),
                                     value if value is not None or nullable_score else 0))
        with DatabaseUnitOfWork(self.player) as uow:
            for index, value in enumerate(values):
                # NULL is represented as zero by the standard NOT NULL boss schema.
                uow.execute("INSERT INTO boss_limit VALUES(?,?)", (str(index), value if value is not None else 0))
        return keys

    def legacy_season(self, actor, key):
        row = self.read(
            "SELECT r.*,COALESCE(u.user_name,r.user_id) AS user_name FROM season_rank r "
            "LEFT JOIN user_xiuxian u ON u.user_id=r.user_id AND r.user_id<>'' "
            "WHERE r.rank_key=? AND r.user_id=? ORDER BY xiuxian_int_text(r.score) "
            "COLLATE xiuxian_integer DESC,r.updated_at ASC LIMIT 1", (key, actor), one=True)
        if row is None:
            return None
        row["rank"] = self.read("SELECT COUNT(*)+1 AS n FROM season_rank WHERE rank_key=? "
                                "AND xiuxian_int_cmp(score,?)>0", (key, as_int_like(row["score"])), one=True)["n"]
        row["score"] = as_int_like(row["score"])
        # Independent of the product hydrator: stored extra JSON; empty/malformed -> {}.
        try:
            parsed = json.loads(row["extra"]) if row["extra"] else {}
        except (TypeError, ValueError):
            parsed = {}
        row["extra_data"] = parsed
        return row

    def test_season_large_malformed_null_and_ties_match_legacy_without_population_callbacks(self):
        # Retain raw legacy values instead of coercing scientific TEXT through INTEGER affinity.
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TABLE season_rank")
            uow.execute("CREATE TABLE season_rank(rank_key TEXT,mode TEXT,period_key TEXT,"
                        "rank_type TEXT,user_id TEXT,sect_id INTEGER,score,extra TEXT,updated_at TEXT,"
                        "PRIMARY KEY(rank_key,user_id,sect_id))")
            message_store.apply_season_rank_exact_indexes(uow)
        keys = self.seed()
        for actor in [str(index) for index in range(13)] + ["missing"]:
            expected = [row for key in keys if (row := self.legacy_season(actor, key)) is not None]
            self.assertEqual(expected, season.get_user_current_season_entries(actor, now=self.now))
        calls = []
        self.connection._raw.create_function("xiuxian_int_cmp", 2,
            lambda left, right: calls.append((left, right)) or sqlite_int_compare(left, right), deterministic=True)
        traced = []
        self.connection._raw.set_trace_callback(traced.append)
        season.get_user_current_season_entries("0", now=self.now)
        self.connection._raw.set_trace_callback(None)
        self.assertEqual([], calls, "indexed counts must not normalize the population per viewer")
        sql = next(sql for sql in reversed(traced) if sql.lstrip().startswith("WITH targets"))
        plan = self.read("EXPLAIN QUERY PLAN " + sql)
        self.assertTrue(any(SEASON_SCORE_RANK_INDEX.name in row["detail"] and "SEARCH" in row["detail"]
                            for row in plan))

    def test_boss_live_rank_and_top_match_legacy_with_index_ranges(self):
        self.seed()
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            for actor in [str(index) for index in range(13)] + ["missing"]:
                top, expected = _legacy(uow, "boss_limit", "integral", actor)
                self.assertEqual(expected, self.boss._integral_rank_in_uow(uow, actor))
                self.assertEqual([(str(row["user_id"]), as_int_like(row["value"])) for row in top],
                                 self.boss._top_integrals_in_uow(uow, 50))
            calls = []
            uow.connection.create_function("xiuxian_int_cmp", 2,
                lambda left, right: calls.append((left, right)) or sqlite_int_compare(left, right), deterministic=True)
            self.boss._integral_rank_in_uow(uow, "0")
            self.assertLessEqual(len(calls), 1)
            spec = BOSS_INTEGRAL_RANK_INDEX
            plan = uow.query_all(f"EXPLAIN QUERY PLAN SELECT COUNT(*) FROM boss_limit WHERE "
                f"{spec.predicate} AND {spec.key}>xiuxian_int_sort_key_compact(0)")
            self.assertTrue(any(spec.name in row["detail"] and "SEARCH" in row["detail"] for row in plan))

    def test_index_updates_rollback_and_stale_public_snapshots_keep_personal_values_live(self):
        keys = self.seed()
        old_top = season.get_top_season_rank("战力", "weekly", now=self.now)
        boss_top, old_boss_rank = self.boss.top_integrals_snapshot_with_rank(50, "0")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE season_rank SET score=? WHERE user_id='0'", (str(10**81).encode(),))
        self.assertEqual(1, season.get_user_current_season_entries("0", now=self.now)[0]["rank"])
        self.assertEqual(old_top, season.get_top_season_rank("战力", "weekly", now=self.now))
        result = self.boss.grant_integral("0", 10**81)
        self.assertTrue(result.succeeded)
        same_top, fresh_rank = self.boss.top_integrals_snapshot_with_rank(50, "0")
        self.assertEqual(boss_top, same_top)
        self.assertEqual(1, fresh_rank.rank)
        self.assertEqual(old_boss_rank.value + 10**81, fresh_rank.value)
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with DatabaseUnitOfWork(self.game) as uow:
                uow.execute("UPDATE season_rank SET score=-1 WHERE user_id='0'")
                raise RuntimeError("rollback")
        self.assertEqual(self.legacy_season("0", keys[0]),
                         season.get_user_season_rank("0", "交易活跃", "weekly", now=self.now))

    def test_public_season_index_order_matches_legacy_join_duplicates_and_null(self):
        self.seed()
        with DatabaseUnitOfWork(self.game) as uow:
            uow.executemany("INSERT INTO user_xiuxian VALUES(?,?)", (("0", "first"), ("0", "second")))
        key = season._build_rank_identity("战力", "weekly", self.now)[0]
        expected = self.read(
            "SELECT r.*,COALESCE(u.user_name,r.user_id) AS user_name,"
            "COALESCE(s.sect_name,CAST(r.sect_id AS TEXT)) AS sect_name "
            "FROM season_rank r INDEXED BY sqlite_autoindex_season_rank_1 "
            "LEFT JOIN user_xiuxian u ON u.user_id=r.user_id AND r.user_id<>'' "
            "LEFT JOIN sects s ON s.sect_id=r.sect_id AND r.sect_id<>0 WHERE r.rank_key=? "
            "ORDER BY xiuxian_int_text(r.score) COLLATE xiuxian_integer DESC,r.updated_at ASC LIMIT 100", (key,))
        calls, traced = [], []
        self.connection._raw.create_function("xiuxian_int_sort_key_compact", 1,
            lambda value: calls.append(value) or sqlite_int_sort_key(value), deterministic=True)
        self.connection._raw.set_trace_callback(traced.append)
        self.assertEqual(season._hydrate_rank_rows(expected),
                         season.get_top_season_rank("战力", "weekly", limit=100, now=self.now))
        self.connection._raw.set_trace_callback(None)
        self.assertEqual([], calls, "public order must reuse stored keys")
        sql = next(sql for sql in reversed(traced) if "FROM season_rank AS r" in sql)
        plan = self.read("EXPLAIN QUERY PLAN " + sql)
        self.assertTrue(any(SEASON_SCORE_ORDER_INDEX.name in row["detail"] for row in plan))
        self.assertFalse(any("TEMP B-TREE" in row["detail"] for row in plan))

    def test_boss_legacy_duplicate_identity_and_wrong_index_use_existing_fallback(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DROP TABLE boss_limit")
            uow.execute("CREATE TABLE boss_limit(user_id TEXT COLLATE NOCASE,integral)")
            uow.executemany("INSERT INTO boss_limit VALUES(?,?)", (("Ab", 7), ("ab", 100), ("x", 8)))
            apply_boss_exact_rank_indexes(uow)
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            for actor in ("ab", "x"):
                top, rank = _legacy(uow, "boss_limit", "integral", actor)
                self.assertEqual(rank, self.boss._integral_rank_in_uow(uow, actor))
                self.assertEqual([(str(row["user_id"]), as_int_like(row["value"])) for row in top],
                                 self.boss._top_integrals_in_uow(uow, 50))
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(f'DROP INDEX "{SEASON_SCORE_RANK_INDEX.name}"')
            uow.execute(f'CREATE INDEX "{SEASON_SCORE_RANK_INDEX.name}" ON season_rank(score)')
            self.assertFalse(rank_index_available(uow, self.game, SEASON_SCORE_RANK_INDEX))

    def test_personal_season_preserves_primary_key_ties_across_sects_and_name_duplicates(self):
        keys = self.seed()
        with DatabaseUnitOfWork(self.game) as uow:
            # Insert in reverse sect order so rowid order cannot pass this check.
            uow.executemany("INSERT INTO season_rank SELECT rank_key,mode,period_key,rank_type,"
                            "user_id,?,score,?,updated_at FROM season_rank WHERE user_id='0' AND sect_id=0",
                            ((9, '{"tie":9}'), (-2, '{"tie":-2}')))
            uow.executemany("INSERT INTO user_xiuxian VALUES('0',?)", (("first",), ("second",)))
        expected = [self.legacy_season("0", key) for key in keys]
        actual = season.get_user_current_season_entries("0", now=self.now)
        self.assertEqual(expected, actual)
        self.assertTrue(all(row["sect_id"] == -2 for row in actual))

    def test_season_proof_and_query_use_the_borrowed_reader_not_the_managers_writer(self):
        keys = self.seed()
        other = db_backend.connect(self.game.with_name("other.db"))
        self.addCleanup(other.close)
        self.manager.conn = other
        # Table readiness has its own writer/schema contract. This check isolates
        # the subsequent ranking read stage and its index proof.
        with patch.object(season, "ensure_season_rank_table", return_value=True), \
                patch.object(season, "rank_index_available", wraps=rank_index_available) as proof, \
                patch.object(self.manager, "_read_query", side_effect=AssertionError("different reader")):
            self.assertEqual(self.legacy_season("0", keys[0]),
                             season.get_user_season_rank("0", "交易活跃", "weekly", now=self.now))
            self.assertIs(proof.call_args.args[0], self.connection)

    def test_nonstandard_season_migration_does_not_change_legacy_tie_traversal(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TABLE season_rank")
            uow.execute("CREATE TABLE season_rank(rank_key TEXT,mode TEXT,period_key TEXT,"
                        "rank_type TEXT,user_id TEXT,sect_id INTEGER,score,extra TEXT,updated_at TEXT)")
            message_store.apply_season_rank_exact_indexes(uow)
            indexes = {row["name"] for row in uow.query_all("PRAGMA index_list(season_rank)")}
            self.assertNotIn(SEASON_SCORE_ORDER_INDEX.name, indexes)
            self.assertNotIn(SEASON_SCORE_RANK_INDEX.name, indexes)
        self.seed()
        key = season._build_rank_identity("战力", "weekly", self.now)[0]
        self.assertEqual(self.legacy_season("0", key),
                         season.get_user_season_rank("0", "战力", "weekly", now=self.now))

    def test_new_migrations_skip_existing_incomplete_legacy_tables(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DROP TABLE season_rank")
            uow.execute("CREATE TABLE season_rank(rank_key TEXT,user_id TEXT,sect_id INTEGER,"
                        "PRIMARY KEY(rank_key,user_id,sect_id))")
            message_store.apply_season_rank_exact_indexes(uow)
            self.assertFalse(rank_index_available(uow, self.game, SEASON_SCORE_RANK_INDEX))
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DROP TABLE boss_limit")
            uow.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY)")
            apply_boss_exact_rank_indexes(uow)
            self.assertFalse(rank_index_available(uow, self.player, BOSS_INTEGRAL_RANK_INDEX))

    def test_collated_season_schema_keeps_legacy_key_matching_even_with_new_indexes(self):
        for collation, change in (("NOCASE", str.upper), ("RTRIM", lambda key: key + "  ")):
            with self.subTest(collation=collation):
                with DatabaseUnitOfWork(self.game) as uow:
                    uow.execute("DROP TABLE season_rank")
                    uow.execute(f"CREATE TABLE season_rank(rank_key TEXT COLLATE {collation},"
                                "mode TEXT,period_key TEXT,rank_type TEXT,user_id TEXT,sect_id INTEGER,"
                                "score,extra TEXT,updated_at TEXT)")
                    message_store.apply_season_rank_exact_indexes(uow)
                    key, mode, period = season._build_rank_identity("战力", "weekly", self.now)
                    for actor, value in (("one", -1), ("two", None), ("three", 1)):
                        uow.execute("INSERT INTO season_rank VALUES(?,?,?,?,?,0,?,'{}','a')",
                                    (change(key), mode, period, "战力", actor, value))
                    self.assertFalse(rank_index_available(uow, self.game, SEASON_SCORE_RANK_INDEX))
                self.assertEqual(self.legacy_season("one", key),
                                 season.get_user_season_rank("one", "战力", "weekly", now=self.now))

    def test_migration_routing_idempotence_and_read_denial(self):
        migrations = build_migrations()
        for key, version, forbidden in (("player_db", "boss.008", "game_db"),
                                        ("game_db", "season_rank.002", "player_db")):
            selected = {row.version: row for row in migrations_for_database(migrations, key)}
            self.assertIn(version, selected)
            self.assertNotIn(version, {row.version for row in migrations_for_database(migrations, forbidden)})
            with DatabaseUnitOfWork(self.player if key == "player_db" else self.game) as uow:
                before = uow.query_all("SELECT name,sql FROM sqlite_master ORDER BY name")
                selected[version].apply(uow)
                self.assertEqual(before, uow.query_all("SELECT name,sql FROM sqlite_master ORDER BY name"))
        self.connection._raw.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and table == "sqlite_master" else sqlite3.SQLITE_OK)
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                season.get_user_current_season_entries("0", now=self.now)
        finally:
            self.connection._raw.set_authorizer(None)


def test_boss_purchase_indexes_follow_attached_transaction_rollback_retry_and_replay(tmp_path):
    from tests.test_boss_purchase_command_ingress import _context, _handler, _state

    context = _context(tmp_path, weekly=0)
    with DatabaseUnitOfWork(context.player) as uow:
        apply_boss_exact_rank_indexes(uow)
        uow.execute("INSERT INTO boss_limit VALUES('other',95)")
    repository = BossIntegralSqlRepository(context.player)
    initial = repository.integral_rank("u")
    assert (initial.rank, initial.value) == (1, 100)
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute("CREATE TRIGGER reject_inventory AFTER INSERT ON back "
                    "BEGIN SELECT RAISE(ABORT,'isolated rollback'); END")
    handler = _handler(context)
    before = _state(context)
    assert "异常" in handler.run("1 1")
    assert repository.integral_rank("u") == initial
    assert _state(context)[:3] == before[:3]
    with DatabaseUnitOfWork(context.game) as uow:
        uow.execute("DROP TRIGGER reject_inventory")
    assert "成功兑换获得" in handler.run("1 1")
    current = repository.integral_rank("u")
    assert (current.rank, current.value) == (2, 90)
    committed = _state(context)
    assert "已经处理" in handler.run("1 1")
    assert _state(context) == committed
    assert repository.integral_rank("u") == current


if __name__ == "__main__":
    unittest.main()
