"""Live training rank needs only player data, preserving the attached cold read."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
from nonebot_plugin_xiuxian_2.features.base.ranking_position import PersonalRank
from nonebot_plugin_xiuxian_2.features.training.leaderboard_repository import TrainingLeaderboardSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache


class TrainingRankDatabaseBoundaryTests(unittest.TestCase):
    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.game = Path(self.temp.name) / "game.db"
        self.player = Path(self.temp.name) / "player.db"
        self.huge = 10**100
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT,points TEXT,completed TEXT)")
            rows = (("viewer", self.huge + 1, "2"), ("viewer", self.huge + 100, "100"),
                    ("rival", self.huge + 10, "3"), ("tie", self.huge + 1, "2"),
                    ("zero", 0, "0"), ("negative", -1, "-1"), (None, self.huge + 500, "500"),
                    (" ", self.huge + 600, "600"))
            uow.executemany("INSERT INTO training VALUES(?,?,?)",
                            ((actor, bind_sqlite_param(value, exact=True), completed)
                             for actor, value, completed in rows))
        self.repository = TrainingLeaderboardSqlRepository(self.game, self.player)

    def test_main_and_attached_rank_match_for_first_duplicate_ties_and_huge_values(self):
        for field, value in (("points", self.huge + 1), ("completed", 2)):
            with self.subTest(field=field):
                direct = self.repository.rank_of(field, "viewer")
                with DatabaseUnitOfWork(self.game, read_only=True) as uow:
                    uow.attach_database(self.player, "player_data")
                    attached = self.repository._rank_in_uow(uow, field, "viewer")
                self.assertEqual(PersonalRank(2, 3, value), direct)
                self.assertEqual(attached, direct)
                self.assertEqual(PersonalRank(0, 3, 0), self.repository.rank_of(field, "zero"))
                self.assertEqual(PersonalRank(0, 3, 0), self.repository.rank_of(field, "absent"))

    def test_hot_rank_only_opens_player_readonly_and_observes_external_writes(self):
        original_enter = DatabaseUnitOfWork.__enter__
        opened = []
        def enter(uow):
            self.assertEqual(self.player, uow.database)
            self.assertTrue(uow.read_only)
            opened.append(uow.database)
            return original_enter(uow)
        with patch.object(DatabaseUnitOfWork, "__enter__", enter), \
                patch.object(DatabaseUnitOfWork, "attach_database", side_effect=AssertionError("unused attach")):
            self.assertEqual(2, self.repository.rank_of("completed", "viewer").rank)
        self.assertEqual([self.player], opened)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("UPDATE training SET completed=4 WHERE user_id='viewer'")
        self.assertEqual(PersonalRank(1, 3, 4), self.repository.rank_of("completed", "viewer"))

    def test_actual_player_read_failure_propagates_and_next_read_recovers(self):
        original_enter = DatabaseUnitOfWork.__enter__
        def enter(uow):
            result = original_enter(uow)
            uow.connection.set_authorizer(lambda action, table, *_:
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "training"
                else sqlite3.SQLITE_OK)
            return result
        with patch.object(DatabaseUnitOfWork, "__enter__", enter):
            with self.assertRaises(sqlite3.DatabaseError):
                self.repository.rank_of("points", "viewer")
        self.assertEqual(PersonalRank(2, 3, self.huge + 1), self.repository.rank_of("points", "viewer"))

    def test_missing_game_or_player_and_blank_user_do_not_create_databases(self):
        missing = self.game.parent / "missing.db"
        for game, player, actor in ((missing, self.player, "viewer"),
                                    (self.game, missing, "viewer"),
                                    (self.game, self.player, " ")):
            with self.subTest(game=game, player=player, actor=actor):
                self.assertEqual(PersonalRank(), TrainingLeaderboardSqlRepository(game, player).rank_of("points", actor))
        self.assertFalse(missing.exists())

    def test_unrelated_corrupt_game_contents_do_not_block_live_player_rank(self):
        self.game.write_bytes(b"invalid unrelated game contents")
        self.assertEqual(PersonalRank(2, 3, 2), self.repository.rank_of("completed", "viewer"))

    def test_invalid_schema_is_rejected_before_sql(self):
        with self.assertRaisesRegex(ValueError, "unsupported SQLite schema"):
            self.repository._rank_in_uow(None, "points", "viewer", schema="unsafe;SELECT 1")
