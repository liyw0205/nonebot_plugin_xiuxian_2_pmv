import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import ImpartApplication
from ..migrations import apply_impart_prayer_operations, apply_impart_prayer_player_statistics
from tests.test_db_backend import db_backend


class ImpartPrayerRepositoryTests(unittest.TestCase):
    def test_request_requires_startup_schemas_and_does_not_create_them(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp)/name for name in ('game.db', 'impart.db', 'player.db'))
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))')
                c.execute("INSERT INTO back VALUES('u',9,1,1)")
            with db_backend.transaction(impart) as c:
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)')
                c.execute("INSERT INTO xiuxian_impart(user_id) VALUES('u')")
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
            with db_backend.transaction(player):
                pass

            result = ImpartApplication(game, impart_database=impart).prayer_settle(
                operation_id='p', user_id='u', game_database=game, player_database=player,
                item_id=9, quantity=1, cards=['A'], card_definitions={'A': {}},
            )

            self.assertEqual(result.status, 'schema_missing')
            with db_backend.connection(game) as c:
                self.assertIsNone(c.execute("SELECT 1 FROM sqlite_master WHERE name='impart_prayer_operations'").fetchone())
            with db_backend.connection(player) as c:
                self.assertIsNone(c.execute("SELECT 1 FROM sqlite_master WHERE name='statistics'").fetchone())

    def test_settle_replay_and_item_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp)/name for name in ('game.db', 'impart.db', 'player.db'))
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))')
                c.execute("INSERT INTO back VALUES('u',9,2,2)")
            with DatabaseUnitOfWork(game) as uow:
                apply_impart_prayer_operations(uow)
            with db_backend.transaction(impart) as c:
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)')
                c.execute("INSERT INTO xiuxian_impart(user_id) VALUES('u')")
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
            with DatabaseUnitOfWork(player) as uow:
                apply_impart_prayer_player_statistics(uow)
            app = ImpartApplication(game, impart_database=impart)
            first = app.prayer_settle(
                operation_id='p', user_id='u', game_database=game, player_database=player,
                item_id=9, quantity=1, cards=['A'], card_definitions={'A': {}},
            )
            duplicate = app.prayer_settle(
                operation_id='p', user_id='u', game_database=game, player_database=player,
                item_id=9, quantity=1, cards=['A'], card_definitions={'A': {}},
            )
            missing = app.prayer_settle(
                operation_id='q', user_id='u', game_database=game, player_database=player,
                item_id=9, quantity=2, cards=['A', 'A'], card_definitions={'A': {}},
            )
            self.assertEqual((first.status,duplicate.status,missing.status),('applied','duplicate','item_missing'))
            with db_backend.connection(player) as c:
                stats = tuple(c.execute(
                    'SELECT "祈愿石使用","传承新卡","传承重复卡" FROM statistics WHERE user_id=?', ('u',)
                ).fetchone())
            self.assertEqual(stats, (1, 1, 0))

    def test_stats_failure_rolls_back_prayer_and_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            game, impart, player = (Path(temp)/name for name in ('game.db', 'impart.db', 'player.db'))
            with db_backend.transaction(game) as c:
                c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))')
                c.execute("INSERT INTO back VALUES('u',9,2,2)")
            with DatabaseUnitOfWork(game) as uow:
                apply_impart_prayer_operations(uow)
            with db_backend.transaction(impart) as c:
                c.execute('CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,impart_two_exp REAL DEFAULT 0,impart_exp_up REAL DEFAULT 0,impart_atk_per REAL DEFAULT 0,impart_hp_per REAL DEFAULT 0,impart_mp_per REAL DEFAULT 0,boss_atk REAL DEFAULT 0,impart_know_per REAL DEFAULT 0,impart_burst_per REAL DEFAULT 0,impart_mix_per REAL DEFAULT 0,impart_reap_per REAL DEFAULT 0)')
                c.execute("INSERT INTO xiuxian_impart(user_id) VALUES('u')")
                c.execute('CREATE TABLE impart_cards(user_id TEXT,card_name TEXT,quantity INTEGER,UNIQUE(user_id,card_name))')
            with DatabaseUnitOfWork(player) as uow:
                apply_impart_prayer_player_statistics(uow)
            with db_backend.transaction(player) as c:
                c.execute(
                    'CREATE TRIGGER fail_prayer_stats BEFORE INSERT ON statistics '
                    "BEGIN SELECT RAISE(ABORT,'failed'); END"
                )

            with self.assertRaises(db_backend.IntegrityError):
                ImpartApplication(game, impart_database=impart).prayer_settle(
                    operation_id='p', user_id='u', game_database=game, player_database=player,
                    item_id=9, quantity=1, cards=['A'], card_definitions={'A': {}},
                )

            with db_backend.connection(game) as c:
                self.assertEqual(tuple(c.execute('SELECT goods_num,bind_num FROM back').fetchone()), (2, 2))
                self.assertEqual(c.execute('SELECT COUNT(*) FROM impart_prayer_operations').fetchone()[0], 0)
            with db_backend.connection(impart) as c:
                self.assertEqual(c.execute('SELECT COUNT(*) FROM impart_cards').fetchone()[0], 0)
                self.assertEqual(tuple(c.execute('SELECT impart_atk_per FROM xiuxian_impart').fetchone()), (0.0,))
