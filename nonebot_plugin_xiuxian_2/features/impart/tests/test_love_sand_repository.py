import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..love_sand_repository import LoveSandSqlRepository
from ..migrations import (
    apply_impart_love_sand_operations,
    apply_impart_love_sand_player_statistics,
)
from tests.test_db_backend import db_backend


class LoveSandRepositoryTests(unittest.TestCase):
    def test_apply_replay_and_state_conflict(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp) / name for name in ("game.db", "impart.db", "player.db"))
            with db_backend.transaction(game) as c:
                c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
                c.execute("INSERT INTO user_xiuxian VALUES('u')")
                c.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,UNIQUE(user_id,goods_id))"
                )
                c.execute("INSERT INTO back VALUES('u',1,3,3)")
            with db_backend.transaction(impart) as c:
                c.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)")
                c.execute("INSERT INTO xiuxian_impart VALUES('u',7)")
            with DatabaseUnitOfWork(game) as uow:
                apply_impart_love_sand_operations(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_impart_love_sand_player_statistics(uow)

            repository = LoveSandSqlRepository(game, impart, player)
            first = repository.apply("x", "u", 1, 1, 20, 3, 7)
            duplicate = repository.apply("x", "u", 1, 1, 20, 3, 7)
            stale = repository.apply("y", "u", 1, 1, 20, 3, 7)

            self.assertEqual((first.status, duplicate.status, stale.status), ("applied", "duplicate", "state_changed"))
            with db_backend.connection(game) as c:
                self.assertEqual(tuple(c.execute("SELECT goods_num,bind_num FROM back").fetchone()), (2, 2))
            with db_backend.connection(impart) as c:
                self.assertEqual(c.execute("SELECT stone_num FROM xiuxian_impart").fetchone()[0], 27)
            with db_backend.connection(player) as c:
                self.assertEqual(
                    tuple(c.execute('SELECT "思恋流沙使用","思恋结晶获取" FROM statistics').fetchone()),
                    (1, 20),
                )

    def test_missing_migrations_do_not_create_request_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp) / name for name in ("game.db", "impart.db", "player.db"))
            with db_backend.transaction(game) as c:
                c.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,UNIQUE(user_id,goods_id))"
                )
            with db_backend.transaction(impart) as c:
                c.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)")
            with db_backend.transaction(player):
                pass

            repository = LoveSandSqlRepository(game, impart, player)
            self.assertEqual(repository.apply("x", "u", 1, 1, 20, 1, 7).status, "schema_missing")
            with db_backend.connection(game) as c:
                self.assertIsNone(
                    c.execute("SELECT 1 FROM sqlite_master WHERE name='love_sand_operations'").fetchone()
                )
            with db_backend.connection(player) as c:
                self.assertIsNone(
                    c.execute("SELECT 1 FROM sqlite_master WHERE name='statistics'").fetchone()
                )

            with DatabaseUnitOfWork(game) as uow:
                apply_impart_love_sand_operations(uow)
            self.assertEqual(repository.apply("y", "u", 1, 1, 20, 1, 7).status, "schema_missing")
            with db_backend.connection(player) as c:
                self.assertIsNone(
                    c.execute("SELECT 1 FROM sqlite_master WHERE name='statistics'").fetchone()
                )

    def test_startup_migrations_preserve_legacy_operation_and_statistics_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp) / name for name in ("game.db", "impart.db", "player.db"))
            with db_backend.transaction(game) as c:
                c.execute(
                    "CREATE TABLE love_sand_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,gained INTEGER NOT NULL,"
                    "stone_num INTEGER NOT NULL,item_remaining INTEGER NOT NULL)"
                )
                c.execute(
                    "INSERT INTO love_sand_operations VALUES('old','[\"u\",1,1]',20,27,2)"
                )
            with db_backend.transaction(impart):
                pass
            with db_backend.transaction(player) as c:
                c.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY,old_total INTEGER)")
                c.execute("INSERT INTO statistics VALUES('u',9)")
            with DatabaseUnitOfWork(game) as uow:
                apply_impart_love_sand_operations(uow)
            with DatabaseUnitOfWork(player) as uow:
                apply_impart_love_sand_player_statistics(uow)

            result = LoveSandSqlRepository(game, impart, player).apply("old", "u", 1, 1, 20, 3, 7)

            self.assertEqual((result.status, result.gained, result.stone_num, result.item_remaining), ("duplicate", 20, 27, 2))
            with db_backend.connection(player) as c:
                self.assertEqual(
                    tuple(c.execute('SELECT old_total,"思恋流沙使用","思恋结晶获取" FROM statistics').fetchone()),
                    (9, 0, 0),
                )

    def test_operation_write_failure_rolls_back_all_attached_assets(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp) / name for name in ("game.db", "impart.db", "player.db"))
            with db_backend.transaction(game) as c:
                c.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                    "bind_num INTEGER,UNIQUE(user_id,goods_id))"
                )
                c.execute("INSERT INTO back VALUES('u',1,3,3)")
            with db_backend.transaction(impart) as c:
                c.execute("CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,stone_num INTEGER)")
                c.execute("INSERT INTO xiuxian_impart VALUES('u',7)")
            with DatabaseUnitOfWork(game) as uow:
                apply_impart_love_sand_operations(uow)
                uow.execute(
                    "CREATE TRIGGER fail_love_sand BEFORE INSERT ON love_sand_operations "
                    "BEGIN SELECT RAISE(ABORT,'failed'); END"
                )
            with DatabaseUnitOfWork(player) as uow:
                apply_impart_love_sand_player_statistics(uow)

            with self.assertRaises(db_backend.IntegrityError):
                LoveSandSqlRepository(game, impart, player).apply("x", "u", 1, 1, 20, 3, 7)

            with db_backend.connection(game) as c:
                self.assertEqual(tuple(c.execute("SELECT goods_num,bind_num FROM back").fetchone()), (3, 3))
                self.assertEqual(c.execute("SELECT COUNT(*) FROM love_sand_operations").fetchone()[0], 0)
            with db_backend.connection(impart) as c:
                self.assertEqual(c.execute("SELECT stone_num FROM xiuxian_impart").fetchone()[0], 7)
            with db_backend.connection(player) as c:
                self.assertIsNone(c.execute("SELECT user_id FROM statistics").fetchone())
