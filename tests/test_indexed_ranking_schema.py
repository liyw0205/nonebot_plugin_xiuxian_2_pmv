"""Index evidence is bounded schema metadata; live rank values stay in SQLite."""

from dataclasses import replace
from pathlib import Path
import sqlite3
import tempfile
import unittest

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_compare
from nonebot_plugin_xiuxian_2.features.base.indexed_ranking import (
    ARENA_SCORE_ORDER_INDEX,
    TRAINING_RANK_INDEXES,
    indexed_personal_rank,
    rank_index_available,
)
from nonebot_plugin_xiuxian_2.features.base.ranking_position import PersonalRank
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache


class IndexedRankingSchemaTests(unittest.TestCase):
    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.database = Path(temp.name) / "player.db"
        self.spec = TRAINING_RANK_INDEXES["points"]
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,points,completed)")
            uow.execute(self.spec.create_sql)

    def available(self, *, unique=True):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return rank_index_available(uow, self.database, self.spec, require_unique_user=unique)

    def test_schema_version_rejects_replaced_index_then_accepts_repair(self):
        self.assertTrue(self.available())
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(f'DROP INDEX "{self.spec.name}"')
            uow.execute(f'CREATE INDEX "{self.spec.name}" ON training(points DESC)')
        self.assertFalse(self.available())
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(f'DROP INDEX "{self.spec.name}"')
            uow.execute(self.spec.create_sql)
        self.assertTrue(self.available())

    def test_unknown_identity_semantics_never_enable_deduplication_shortcut(self):
        shapes = (
            ("user_id TEXT,points,completed", ""),
            ("user_id TEXT,points,completed", "CREATE UNIQUE INDEX partial_identity ON training(user_id) WHERE points>0"),
            ("user_id TEXT COLLATE NOCASE,points,completed", "CREATE UNIQUE INDEX binary_identity ON training(user_id COLLATE BINARY)"),
            ("user_id TEXT PRIMARY KEY,points,completed,rowid INTEGER", ""),
        )
        for columns, identity in shapes:
            with self.subTest(columns=columns, identity=identity):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("DROP TABLE training")
                    uow.execute(f"CREATE TABLE training({columns})")
                    if identity:
                        uow.execute(identity)
                    uow.execute(self.spec.create_sql)
                self.assertFalse(self.available())

    def test_without_rowid_table_stays_on_legacy_path(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE training")
            uow.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,points,completed) WITHOUT ROWID")
            uow.execute(self.spec.create_sql)
        self.assertFalse(self.available(unique=False))

    def test_metadata_cache_does_not_hide_writes_or_recompute_numeric_population(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany("INSERT INTO training(user_id,points) VALUES(?,?)",
                            ((f"u{index}", index) for index in range(128)))
        self.assertTrue(self.available())
        numeric_calls = []
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            uow.connection.create_function(
                "xiuxian_int_cmp", 2,
                lambda left, right: numeric_calls.append((left, right)) or sqlite_int_compare(left, right),
                deterministic=True,
            )
            self.assertEqual(PersonalRank(65, 127, 63), indexed_personal_rank(uow, self.spec, "u63"))
            plan = uow.query_all(
                f"EXPLAIN QUERY PLAN SELECT COUNT(*) FROM training WHERE {self.spec.predicate} "
                f"AND {self.spec.key}>xiuxian_int_sort_key_compact(0)"
            )
        self.assertLessEqual(len(numeric_calls), 2, "population must use stored keys, not Python per row")
        self.assertTrue(any("SEARCH" in row["detail"] and self.spec.name in row["detail"] for row in plan))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE training SET points=1000 WHERE user_id='u63'")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertTrue(rank_index_available(uow, self.database, self.spec, require_unique_user=True))
            self.assertEqual(PersonalRank(1, 127, 1000), indexed_personal_rank(uow, self.spec, "u63"))

    def test_attached_schema_evidence_uses_player_identity(self):
        game = self.database.with_name("game.db")
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE unrelated(value)")
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            uow.attach_database(self.database, "player_data")
            self.assertTrue(rank_index_available(
                uow, self.database, self.spec, schema="player_data", require_unique_user=True,
            ))
            self.assertFalse(rank_index_available(uow, game, self.spec, require_unique_user=True))

    def test_schema_proof_is_bound_to_connection_even_when_path_hint_and_version_match(self):
        self.assertTrue(self.available())
        replacement = self.database.with_name("replacement.db")
        with DatabaseUnitOfWork(replacement) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT,points,completed)")
            uow.execute(self.spec.create_sql)
        with DatabaseUnitOfWork(self.database, read_only=True) as original, \
                DatabaseUnitOfWork(replacement, read_only=True) as other:
            self.assertEqual(original.query_one("PRAGMA schema_version"), other.query_one("PRAGMA schema_version"))
            # A reader of an unlinked old file can have the same pathname hint
            # as a new connection. Only its own schema proves uniqueness.
            self.assertFalse(rank_index_available(other, self.database, self.spec, require_unique_user=True))
            self.assertTrue(rank_index_available(original, self.database, self.spec, require_unique_user=True))

    def test_rolled_back_ddl_cannot_poison_a_reused_schema_version(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DROP TABLE training")
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with DatabaseUnitOfWork(self.database) as uow:
                uow.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,points,completed)")
                uow.execute(self.spec.create_sql)
                self.assertTrue(rank_index_available(uow, self.database, self.spec, require_unique_user=True))
                raise RuntimeError("rollback")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT,points,completed)")
            uow.execute(self.spec.create_sql)
            self.assertFalse(rank_index_available(uow, self.database, self.spec, require_unique_user=True))

    def test_schema_read_denial_is_not_cached_as_absence(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            uow.connection.set_authorizer(
                lambda action, table, *_: sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "sqlite_master" else sqlite3.SQLITE_OK
            )
            with self.assertRaises(sqlite3.DatabaseError):
                rank_index_available(uow, self.database, self.spec, require_unique_user=True)
        self.assertTrue(self.available())

    def test_only_controlled_specifications_and_schemas_are_accepted(self):
        invalid = ((replace(self.spec, table="training; SELECT 1"), "main"),
                   (self.spec, "temp"))
        for spec, schema in invalid:
            with self.subTest(spec=spec, schema=schema):
                with self.assertRaises(ValueError):
                    rank_index_available(None, self.database, spec, schema=schema)
        with self.assertRaises(ValueError):
            indexed_personal_rank(None, ARENA_SCORE_ORDER_INDEX, "u")


class ExactRankMigrationRoutingTests(unittest.TestCase):
    def test_new_index_migrations_are_player_only_and_idempotent(self):
        migrations = build_migrations()
        wanted = {"arena.011", "training.007"}
        player = {row.version: row for row in migrations_for_database(migrations, "player_db")}
        game = {row.version for row in migrations_for_database(migrations, "game_db")}
        self.assertTrue(wanted <= player.keys())
        self.assertFalse(wanted & game)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "player.db"
            with DatabaseUnitOfWork(path) as uow:
                for version in sorted(wanted):
                    player[version].apply(uow)
                first = uow.query_all("SELECT name,sql FROM sqlite_master WHERE type='index' ORDER BY name")
                for version in sorted(wanted):
                    player[version].apply(uow)
                self.assertEqual(first, uow.query_all("SELECT name,sql FROM sqlite_master WHERE type='index' ORDER BY name"))
                self.assertTrue(rank_index_available(uow, path, TRAINING_RANK_INDEXES["points"], require_unique_user=True))
                self.assertTrue(rank_index_available(uow, path, ARENA_SCORE_ORDER_INDEX))


if __name__ == "__main__":
    unittest.main()
