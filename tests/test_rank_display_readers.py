"""Live standing and natal projections reuse handles without caching player values."""
from contextlib import ExitStack
from contextvars import copy_context
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init(command_start={"", "/"}, log_level="ERROR")

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import register_numeric_functions
from nonebot_plugin_xiuxian_2.features.activity import read_model_repository as activity
from nonebot_plugin_xiuxian_2.features.base.indexed_ranking import BOSS_INTEGRAL_RANK_INDEX, TRAINING_RANK_INDEXES
from nonebot_plugin_xiuxian_2.features.base.ranking_position import PersonalRank
from nonebot_plugin_xiuxian_2.features.boss import integral_repository as boss
from nonebot_plugin_xiuxian_2.features.info import profile_repository
from nonebot_plugin_xiuxian_2.features.title import application as title
from nonebot_plugin_xiuxian_2.features.training import leaderboard_repository as training
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as database
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure import natal_data
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class RankDisplayReaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.game = Path(directory.name) / "game.db"
        self.player = self.game.with_name("player.db")
        self.write(self.game, [
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,exp,stone)",
            "INSERT INTO user_xiuxian VALUES('a','Alpha',100,200)",
            "CREATE TABLE activity_user(user_id TEXT PRIMARY KEY,sign_days,total_sign_days,last_sign_date)",
            "INSERT INTO activity_user VALUES('a',3,5,'2026-01-01'),('b',4,6,'2026-01-01')",
            "CREATE TABLE activity_boss_damage(activity_key,user_id TEXT,total_damage,PRIMARY KEY(activity_key,user_id))",
            "INSERT INTO activity_boss_damage VALUES('boss','a',100),('boss','b',200)",
        ])
        self.write(self.player, [
            "CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked,equipped)",
            "INSERT INTO title VALUES('a','[\"1\"]','1')",
            "CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form,name,level)",
            "INSERT INTO natal_treasure VALUES('a',1,'Original',3)",
            "CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral)",
            "INSERT INTO boss_limit VALUES('a',100),('b',200)",
            BOSS_INTEGRAL_RANK_INDEX.create_sql,
            "CREATE TABLE training(user_id TEXT PRIMARY KEY,points,completed)",
            "INSERT INTO training VALUES('a',10,1),('b',20,2)",
            TRAINING_RANK_INDEXES['points'].create_sql,
        ])
        self.game_owner, self.player_owner = database.ReadOnlyConnectionOwner(), database.ReadOnlyConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.addCleanup(self.game_owner.close)
        self.addCleanup(self.player_owner.close)
        self.stack.enter_context(patch.object(activity, "LIVE_PROJECTION_READ_OWNER", self.game_owner))
        self.stack.enter_context(patch.object(profile_repository, "LIVE_PROJECTION_READ_OWNER", self.game_owner))
        self.stack.enter_context(patch.object(natal_data, "PLAYER_PROJECTION_READ_OWNER", self.player_owner))
        self.stack.enter_context(patch.object(title, "PLAYER_PROJECTION_READ_OWNER", self.player_owner))
        self.stack.enter_context(patch.object(boss, "PLAYER_PROJECTION_READ_OWNER", self.player_owner))
        self.stack.enter_context(patch.object(training, "PLAYER_PROJECTION_READ_OWNER", self.player_owner))
        self.stack.enter_context(patch.object(natal_data, "get_paths", return_value=SimpleNamespace(player_db=self.player)))
        self.connections = []

        def connect(*args, **kwargs):
            connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        self.repository = activity.ActivityReadModelSqlRepository(self.game)

    @staticmethod
    def write(path, statements):
        connection = REAL_CONNECT(path)
        try:
            register_numeric_functions(connection)
            for sql in statements:
                connection.execute(sql)
            connection.commit()
        finally:
            connection.close()

    def test_standings_share_profile_reader_and_external_scores_are_live(self):
        profile = profile_repository.PlayerProfileSqlRepository(self.game)
        self.assertEqual(200, profile.get_user_profile("a")["stone"])
        self.assertEqual({"sign": (2, 2, 3), "boss": (2, 2, 100)},
                         self.repository.board_standings(("sign", "boss"), "boss", "a"))
        self.write(self.game, ["UPDATE activity_user SET sign_days=8 WHERE user_id='a'",
                               "UPDATE activity_boss_damage SET total_damage=300 WHERE user_id='a'"])
        self.assertEqual((1, 2, 8), self.repository.board_standing("sign", "boss", "a"))
        self.assertEqual((1, 2, 300), self.repository.board_standing("boss", "boss", "a"))
        self.assertEqual(1, len(self.connections))
        reader = self.connections[0]
        self.assertEqual(4, reader.statements.count("BEGIN"))
        self.assertEqual(4, reader.commits)
        self.assertFalse(reader.in_transaction)
        self.assertFalse(reader.closed)

    def test_natal_shares_player_reader_but_changes_schema_and_missing_tables_are_live(self):
        self.assertEqual("1", title.TitleApplication(self.player).get_state("a")["equipped"])
        self.assertEqual({"form": 1, "name": "Original", "level": 3}, natal_data.NatalTreasure.read_existing_data("a"))
        self.write(self.player, ["UPDATE natal_treasure SET name='Updated',level=7 WHERE user_id='a'"])
        self.assertEqual("Updated", natal_data.NatalTreasure.read_existing_data("a")["name"])
        self.assertEqual(1, len(self.connections))
        reader = self.connections[0]
        self.assertEqual(1, sum("table_info" in sql for sql in reader.statements))
        self.assertFalse(any("sqlite_master" in sql for sql in reader.statements))
        self.write(self.player, ["DROP TABLE natal_treasure"])
        self.assertIsNone(natal_data.NatalTreasure.read_existing_data("a"))
        self.write(self.player, ["CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form)",
                                 "INSERT INTO natal_treasure VALUES('a','2')"])
        self.assertEqual({"form": 2}, natal_data.NatalTreasure.read_existing_data("a"))
        self.write(self.player, ["UPDATE natal_treasure SET form=0"])
        self.assertIsNone(natal_data.NatalTreasure.read_existing_data("a"))
        self.write(self.player, ["DROP TABLE natal_treasure", "CREATE TABLE natal_treasure(user_id TEXT)"])
        with self.assertRaises(DomainError) as caught:
            natal_data.NatalTreasure.read_existing_data("a")
        self.assertEqual("natal_treasure_read_failed", caught.exception.code)
        self.assertEqual("player_natal_treasure", caught.exception.details["subject"])
        self.assertTrue(reader.closed)

    def test_read_denials_discard_handles_and_recovery_preserves_contracts(self):
        self.repository.board_standing("sign", "boss", "a")
        game_reader = self.connections[-1]
        game_reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                                   if action == sqlite3.SQLITE_READ and table == "activity_user" else sqlite3.SQLITE_OK)
        self.assertEqual((0, 0, 0), self.repository.board_standing("sign", "boss", "a"))
        self.assertTrue(game_reader.closed)
        self.assertEqual((2, 2, 3), self.repository.board_standing("sign", "boss", "a"))
        natal_data.NatalTreasure.read_existing_data("a")
        player_reader = self.connections[-1]
        player_reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                                     if action == sqlite3.SQLITE_READ and table == "natal_treasure" else sqlite3.SQLITE_OK)
        with self.assertRaises(DomainError):
            natal_data.NatalTreasure.read_existing_data("a")
        self.assertTrue(player_reader.closed)
        self.assertEqual(3, natal_data.NatalTreasure.read_existing_data("a")["level"])

    def test_scope_and_copied_worker_use_independent_handles(self):
        self.repository.board_standing("sign", "boss", "a")
        natal_data.NatalTreasure.read_existing_data("a")
        retained = list(self.connections)
        results, errors = [], []

        def work():
            try:
                results.append(self.repository.board_standing("boss", "boss", "a"))
                results.append(natal_data.NatalTreasure.read_existing_data("a"))
            except BaseException as exc:
                errors.append(exc)

        with database.DatabaseUnitOfWork.connection_scope():
            self.assertEqual((2, 2, 3), self.repository.board_standing("sign", "boss", "a"))
            self.assertEqual(3, natal_data.NatalTreasure.read_existing_data("a")["level"])
            scoped = self.connections[2:]
            worker = threading.Thread(target=copy_context().run, args=(work,))
            worker.start()
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertTrue(all(connection.closed for connection in self.connections[4:]))
            # Both scoped handles stay idle (distinct keys) until scope exit.
            self.assertTrue(all(not c.closed and not c.in_transaction for c in scoped[:2]))
            self.assertTrue(all(c not in retained for c in scoped))
        self.assertTrue(all(c.closed for c in scoped))
        self.assertTrue(all(not c.closed for c in retained))
        self.assertEqual([], errors)
        self.assertEqual((2, 2, 100), results[0])
        self.assertTrue(all(connection.closed for connection in scoped))
        self.assertTrue(all(not connection.closed for connection in retained))

    def test_replacements_and_missing_paths_never_return_old_values_or_create_files(self):
        natal_data.NatalTreasure.read_existing_data("a")
        original = self.connections[-1]
        replacement = self.player.with_name("replacement.db")
        self.write(replacement, ["CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form,name,level)",
                                 "INSERT INTO natal_treasure VALUES('a',1,'Replacement',9)"])
        replacement.replace(self.player)
        self.assertEqual("Replacement", natal_data.NatalTreasure.read_existing_data("a")["name"])
        self.assertTrue(original.closed)
        self.player.unlink()
        self.assertIsNone(natal_data.NatalTreasure.read_existing_data("a"))
        self.assertFalse(self.player.exists())
        self.repository.board_standing("sign", "boss", "a")
        self.game.unlink()
        self.assertEqual((0, 0, 0), self.repository.board_standing("sign", "boss", "a"))
        self.assertFalse(self.game.exists())

    def test_boss_and_training_share_player_handle_with_independent_live_transactions(self):
        integral = boss.BossIntegralSqlRepository(self.player)
        leaderboard = training.TrainingLeaderboardSqlRepository(self.game, self.player)
        self.assertEqual(100, integral.get_integral("a").integral)
        self.assertEqual(PersonalRank(2, 2, 100), integral.integral_rank("a"))
        self.assertEqual(PersonalRank(2, 2, 10), leaderboard.rank_of("points", "a"))
        huge = 10 ** 40 + 1
        self.write(self.player, [f"UPDATE boss_limit SET integral='{huge}' WHERE user_id='a'",
                                 f"UPDATE training SET points='{huge}' WHERE user_id='a'"])
        self.assertEqual(PersonalRank(1, 2, huge), integral.integral_rank("a"))
        self.assertEqual(PersonalRank(1, 2, huge), leaderboard.rank_of("points", "a"))
        self.assertEqual([("a", huge), ("b", 200)], integral.top_integrals())
        self.assertEqual(1, len(self.connections))
        reader = self.connections[0]
        self.assertEqual(6, reader.statements.count("BEGIN"))
        self.assertEqual(6, reader.commits)
        self.assertFalse(reader.in_transaction)
        self.assertIn("PRAGMA query_only=ON", reader.statements)
        # The index content proof remains live on each rank call.
        self.assertEqual(5, sum("SELECT type,name,sql" in sql for sql in reader.statements))

    def test_feature_rank_read_denial_discards_reader_and_recovers(self):
        integral = boss.BossIntegralSqlRepository(self.player)
        leaderboard = training.TrainingLeaderboardSqlRepository(self.game, self.player)
        self.assertEqual(2, integral.integral_rank("a").rank)
        reader = self.connections[-1]
        reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ and table == "boss_limit" else sqlite3.SQLITE_OK)
        with self.assertRaises(sqlite3.DatabaseError):
            integral.integral_rank("a")
        self.assertTrue(reader.closed)
        self.assertEqual(2, leaderboard.rank_of("points", "a").rank)
        self.assertEqual(2, integral.integral_rank("a").rank)
        self.assertEqual(2, len(self.connections))

    def test_feature_rank_scope_and_worker_preserve_owner_lifetime(self):
        integral = boss.BossIntegralSqlRepository(self.player)
        leaderboard = training.TrainingLeaderboardSqlRepository(self.game, self.player)
        integral.integral_rank("a")
        retained = self.connections[0]
        results, errors = [], []

        def read():
            try:
                results.append(integral.integral_rank("a"))
                results.append(leaderboard.rank_of("points", "a"))
            except BaseException as exc:
                errors.append(exc)

        with database.DatabaseUnitOfWork.connection_scope():
            read()
            scoped = self.connections[1]
            worker = threading.Thread(target=copy_context().run, args=(read,))
            worker.start()
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertFalse(scoped.closed)
            self.assertTrue(all(connection.closed for connection in self.connections[2:]))
        self.assertTrue(scoped.closed)
        self.assertFalse(retained.closed)
        self.assertEqual([], errors)
        self.assertEqual([PersonalRank(2, 2, 100), PersonalRank(2, 2, 10)] * 2, results)
        self.assertEqual(4, len(self.connections))

    def test_feature_rank_reader_tracks_replaced_index_file_and_missing_schema(self):
        integral = boss.BossIntegralSqlRepository(self.player)
        self.assertEqual(2, integral.integral_rank("a").rank)
        retained = self.connections[-1]
        self.write(self.player, ["DROP INDEX boss_integral_exact_rank_v1",
                                 "CREATE INDEX boss_integral_exact_rank_v1 ON boss_limit(integral DESC)",
                                 "UPDATE boss_limit SET integral=300 WHERE user_id='a'"])
        self.assertEqual(PersonalRank(1, 2, 300), integral.integral_rank("a"))
        self.write(self.player, ["ALTER TABLE boss_limit RENAME COLUMN integral TO old_integral"])
        self.assertEqual("schema_missing", integral.get_integral("a").status)
        replacement = self.player.with_name("feature-replacement.db")
        self.write(replacement, ["CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral)",
                                 "INSERT INTO boss_limit VALUES('a',700)"])
        replacement.replace(self.player)
        self.assertEqual(PersonalRank(1, 1, 700), integral.integral_rank("a"))
        self.assertTrue(retained.closed)
        self.player.unlink()
        self.assertEqual("schema_missing", integral.get_integral("a").status)
        self.assertFalse(self.player.exists())


if __name__ == "__main__":
    unittest.main()
