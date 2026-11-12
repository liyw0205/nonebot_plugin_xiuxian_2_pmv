from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from threading import RLock
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
from nonebot_plugin_xiuxian_2.features.arena.state_repository import ArenaStateRepository
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
from nonebot_plugin_xiuxian_2.features.boss.integral_repository import BossIntegralSqlRepository
from nonebot_plugin_xiuxian_2.features.sect.scheduled_material_repository import SectScheduledMaterialSqlRepository
from nonebot_plugin_xiuxian_2.features.training.leaderboard_repository import TrainingLeaderboardSqlRepository
from nonebot_plugin_xiuxian_2.features.training.migrations import apply_training_state
from nonebot_plugin_xiuxian_2.features.training.state_repository import TrainingStateRepository
from nonebot_plugin_xiuxian_2.features.tower.migrations import apply_tower_state
from nonebot_plugin_xiuxian_2.features.tower.state_repository import TowerStateRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class LegacyNumericReadModelsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.game = Path(self.temp.name) / 'game.db'
        self.player = Path(self.temp.name) / 'player.db'

    def boss(self, affinity='TEXT'):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(f'CREATE TABLE boss_limit(user_id TEXT,integral {affinity})')
            uow.executemany('INSERT INTO boss_limit VALUES(?,?)', [
                ('small', '9'), ('science', '1e3'),
                ('large', '100000000000000000001'), ('larger', '100000000000000000002'),
                ('larger', '900000000000000000000'), ('zero', None),
            ])
        return BossIntegralSqlRepository(self.player)

    def snapshot(self):
        result = []
        for database in (self.game, self.player):
            if database.exists():
                with sqlite3.connect(database) as conn:
                    result.append(list(conn.iterdump()))
        return result

    def test_boss_snapshot_and_ranking_use_canonical_integers(self):
        repository = self.boss()
        self.assertEqual(1000, repository.get_integral('science').integral)
        self.assertEqual([('larger', 100000000000000000002), ('large', 100000000000000000001),
                          ('science', 1000), ('small', 9), ('zero', 0)], repository.top_integrals())
        rank = repository.integral_rank('large')
        self.assertEqual((2, 4, 100000000000000000001), (rank.rank, rank.total, rank.value))

    def test_boss_grant_accepts_legacy_text_and_preserves_one_digit_changes(self):
        repository = self.boss()
        result = repository.grant_integral('large', 1)
        self.assertEqual(('applied', 100000000000000000002), (result.status, result.value))
        self.assertEqual(100000000000000000002, repository.get_integral('large').integral)
        self.assertEqual(1, repository.integral_rank('large').rank)

    def test_boss_unrecoverable_real_refuses_and_exact_large_balance_is_usable(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute('CREATE TABLE boss_limit(user_id TEXT,integral INTEGER)')
            uow.execute('INSERT INTO boss_limit VALUES(?,?)', ('real', 1e29))
        repository = BossIntegralSqlRepository(self.player)
        before = self.snapshot()
        for action in (lambda: repository.get_integral('real'), lambda: repository.grant_integral('real', 10**28)):
            with self.assertRaises(DomainError) as caught:
                action()
            self.assertEqual('boss_integral_precision_lost_real', caught.exception.details['reason'])
            self.assertEqual('real', caught.exception.details['subject'])
            self.assertEqual(before, self.snapshot())
        expected = 10**29 + 1
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute('INSERT INTO boss_limit VALUES(?,?)', ('exact', bind_sqlite_param(expected, exact=True)))
        self.assertEqual(expected, repository.get_integral('exact').integral)
        result = repository.grant_integral('exact', 10**28)
        self.assertEqual(('applied', expected + 10**28), (result.status, result.value))
        self.assertEqual(expected + 10**28, repository.get_integral('exact').integral)

    def test_boss_write_rejection_does_not_mutate_first_or_duplicate_rows(self):
        repository = self.boss()
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TRIGGER reject_grant BEFORE UPDATE ON boss_limit BEGIN SELECT RAISE(IGNORE); END")
        before = self.snapshot()
        self.assertEqual('state_changed', repository.grant_integral('large', 1).status)
        self.assertEqual(before, self.snapshot())

    def test_training_scientific_text_ranks_and_values_are_exact(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute('CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)')
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute('CREATE TABLE training(user_id TEXT,points TEXT,completed TEXT)')
            uow.executemany('INSERT INTO training VALUES(?,?,?)', [
                ('a', '1e3', '1e2'), ('b', '900', '99'),
                ('c', '100000000000000000001', '100000000000000000001'),
                ('d', '100000000000000000002', '100000000000000000002'),
            ])
        repository = TrainingLeaderboardSqlRepository(self.game, self.player)
        for field, value in (('points', 1000), ('completed', 100)):
            with self.subTest(field=field):
                top = repository.top(field)
                self.assertEqual(['d', 'c', 'a', 'b'], [row['user_id'] for row in top])
                self.assertEqual(value, top[2]['value'])
                rank = repository.rank_of(field, 'c')
                self.assertEqual((2, 4, 100000000000000000001), (rank.rank, rank.total, rank.value))

    def test_training_state_preserves_scientific_numeric_fields(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute('CREATE TABLE training(user_id TEXT PRIMARY KEY,points TEXT,completed TEXT,progress TEXT,max_progress TEXT)')
            apply_training_state(uow)
            uow.execute("INSERT INTO training(user_id,points,completed,progress,max_progress,weekly_purchases) VALUES('a','1e3','1e2','3e2','4e2',?)", ('{"_last_reset":"2026-11-11"}',))
        state = TrainingStateRepository(self.player).initialize('a', date(2026, 11, 11))
        self.assertEqual((1000, 100, 300, 400), tuple(state[k] for k in ('points', 'completed', 'progress', 'max_progress')))

    def test_arena_and_tower_ranking_keep_scientific_text_and_large_integers(self):
        for table, migrate, cls in (('arena', apply_arena_state, ArenaStateRepository),
                                    ('tower', apply_tower_state, TowerStateRepository)):
            with self.subTest(table=table):
                path = Path(self.temp.name) / (table + '.db')
                with DatabaseUnitOfWork(path) as uow:
                    uow.execute(f'CREATE TABLE {table}(user_id TEXT PRIMARY KEY,score TEXT)')
                    migrate(uow)
                    uow.executemany(f'INSERT INTO {table}(user_id,score) VALUES(?,?)', [
                        ('small', '900'), ('science', '1e3'),
                        ('large', '100000000000000000001'), ('larger', '100000000000000000002'),
                    ])
                repo = cls(path)
                rows = repo.ranking(50) if table == 'arena' else repo.ranking('score', 50)
                self.assertEqual(['larger', 'large', 'science', 'small'], [r[0] for r in rows])
                self.assertEqual(1000, rows[2][1])
                rank = repo.rank_of('large') if table == 'arena' else repo.rank_of('large', 'score')
                self.assertEqual((2, 4, 100000000000000000001), (rank.rank, rank.total, rank.value))

    def sect(self, affinity='INTEGER'):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(f'CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_scale {affinity},sect_materials INTEGER,combat_power INTEGER)')
            uow.execute(f'CREATE TABLE user_xiuxian(user_id TEXT,sect_id INTEGER,power {affinity})')
            uow.execute('CREATE TABLE sect_scheduled_material_grants(grant_key TEXT,sect_id INTEGER,materials INTEGER,combat_power INTEGER,PRIMARY KEY(grant_key,sect_id))')
            uow.execute("INSERT INTO sects VALUES(1,'owner',120,50,1)")
        return SectScheduledMaterialSqlRepository(self.game)

    def test_sect_member_power_sum_cannot_overflow_sqlite_integer(self):
        repository = self.sect()
        with DatabaseUnitOfWork(self.game) as uow:
            uow.executemany('INSERT INTO user_xiuxian VALUES(?,1,?)', [('a', 2**62), ('b', 2**62)])
        result = repository.grant('day', 1, 2)
        self.assertEqual(('granted', 2**63), (result['status'], result['combat_power']))
        before = self.snapshot()
        self.assertEqual('duplicate', repository.grant('day', 1, 99)['status'])
        self.assertEqual(before, self.snapshot())

    def test_sect_sum_reads_real_and_exact_scientific_text_before_adding(self):
        repository = self.sect('TEXT')
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE sects SET sect_scale='1e3'")
            uow.executemany('INSERT INTO user_xiuxian VALUES(?,1,?)', [('a', '100000000000000000001'), ('b', '1e3')])
        result = repository.grant('day', 1, 2)
        self.assertEqual((2000, 100000000000000001001), (result['materials'], result['combat_power']))

    def test_sect_receipt_failure_rolls_back_all_tables(self):
        repository = self.sect()
        with DatabaseUnitOfWork(self.game) as uow:
            uow.executemany('INSERT INTO user_xiuxian VALUES(?,1,?)', [('a', 2**62), ('b', 2**62)])
            uow.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON sect_scheduled_material_grants BEGIN SELECT RAISE(ABORT,'receipt rejected'); END")
        before = self.snapshot()
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt rejected'):
            repository.grant('day', 1, 2)
        self.assertEqual(before, self.snapshot())

    def test_player_rank_consumers_use_numeric_order_and_filtering(self):
        from tests.test_db_backend import db_backend
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XiuxianDateManage
        with db_backend.transaction(self.game) as conn:
            conn.execute('CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,level TEXT,exp,stone,power)')
            conn.executemany('INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s)', [
                ('small', 'small', '江湖好手', 900, 900, 900),
                ('science', 'science', '江湖好手', '1e3', '1e3', '1e3'),
                ('large', 'large', '江湖好手', '100000000000000000001', '100000000000000000001', '100000000000000000001'),
                ('larger', 'larger', '江湖好手', '100000000000000000002', '100000000000000000002', '100000000000000000002'),
                ('negative', 'negative', '江湖好手', '-1', '-1', '-1'),
            ])
        with db_backend.connection(self.game) as conn:
            def read(sql, params=(), *, one=False, dict_row=False, **kwargs):
                cursor = conn.execute(sql, params)
                rows = cursor.fetchall()
                results = [dict(zip([col[0] for col in cursor.description], row)) if dict_row else tuple(row) for row in rows]
                return (results[0] if results else None) if one else results
            handle = SimpleNamespace(_read_query=read, _conn_lock=RLock(), conn=conn)
            for method in ('stone_top', 'power_top', 'get_ls_rank', 'realm_top'):
                with self.subTest(method=method):
                    rows = getattr(XiuxianDateManage, method)(handle)
                    self.assertEqual(['larger', 'large', 'science', 'small'], [row[0] for row in rows[:4]])
                    if method == 'get_ls_rank':
                        self.assertEqual(4, len(rows))
            self.assertEqual((2,), XiuxianDateManage.get_exp_rank(handle, 'large'))
            self.assertEqual((2,), XiuxianDateManage.get_stone_rank(handle, 'large'))
            self.assertEqual('larger', XiuxianDateManage.get_top1_user(handle)['user_id'])
            self.assertEqual('larger', XiuxianDateManage.get_realm_top1_user(handle)['user_name'])

    def test_impart_rank_tie_uses_canonical_stone_order(self):
        from tests.test_db_backend import db_backend
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF
        with db_backend.transaction(self.game) as conn:
            conn.execute('CREATE TABLE xiuxian_impart(user_id TEXT,impart_lv INTEGER,stone_num TEXT)')
            conn.executemany('INSERT INTO xiuxian_impart VALUES(%s,10,%s)', [('small','900'), ('science','1e3'), ('large','100000000000000000001'), ('larger','100000000000000000002')])
        with db_backend.connection(self.game) as conn:
            rows = XIUXIAN_IMPART_BUFF.get_impart_rank(SimpleNamespace(conn=conn, _conn_lock=RLock()))
        self.assertEqual(['larger', 'large', 'science', 'small'], [r['user_id'] for r in rows])

    def test_season_rank_uses_numeric_order_and_exact_rank_parameters(self):
        from tests.test_db_backend import db_backend
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import season_rank_service as ranks
        with db_backend.transaction(self.game) as conn:
            conn.execute('CREATE TABLE season_rank(rank_key TEXT,mode TEXT,period_key TEXT,rank_type TEXT,user_id TEXT,sect_id INTEGER,score TEXT,extra TEXT,updated_at TEXT)')
            conn.execute('CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)')
            conn.execute('CREATE TABLE sects(sect_id INTEGER,sect_name TEXT)')
            conn.executemany("INSERT INTO season_rank VALUES('key','monthly','period','战力',?,0,?,'{}','now')", [
                ('large', '100000000000000000001'), ('larger', '100000000000000000002'), ('science', '1e3'), ('small', '900'),
            ])
        with db_backend.connection(self.game) as conn:
            def read(sql, params=(), *, one=False, dict_row=False):
                cur = conn.execute(sql, params)
                rows = [dict(zip([c[0] for c in cur.description], row)) for row in cur.fetchall()]
                return (rows[0] if rows else None) if one else rows
            with patch.object(ranks, '_sql_message_instance', SimpleNamespace(_read_query=read)), \
                    patch.object(ranks, 'ensure_season_rank_table', return_value=True), \
                    patch.object(ranks, '_build_rank_identity', return_value=('key', 'monthly', 'period')):
                self.assertEqual(['larger', 'large', 'science', 'small'], [r['user_id'] for r in ranks.get_top_season_rank('战力')])
                self.assertEqual(2, ranks.get_user_season_rank('large', '战力')['rank'])
                self.assertEqual(1000, ranks.get_user_season_rank('science', '战力')['score'])

    def test_arena_and_tower_initialization_preserve_scientific_state(self):
        for table, migrate, cls, field in (('arena', apply_arena_state, ArenaStateRepository, 'score'),
                                           ('tower', apply_tower_state, TowerStateRepository, 'score')):
            with self.subTest(table=table):
                path = Path(self.temp.name) / (table + '-state.db')
                with DatabaseUnitOfWork(path) as uow:
                    uow.execute(f'CREATE TABLE {table}(user_id TEXT PRIMARY KEY,score TEXT)')
                    migrate(uow)
                    uow.execute(f'INSERT INTO {table}(user_id,score) VALUES(?,?)', ('u', '1e3'))
                self.assertEqual(1000, cls(path).initialize('u', date(2026, 11, 11))[field])

    def test_arena_candidates_keep_exact_scientific_scores_and_exclude_invalid(self):
        from nonebot_plugin_xiuxian_2.features.arena.opponent_repository import ArenaOpponentRepository
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute('CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)')
            uow.executemany('INSERT INTO user_xiuxian VALUES(?,?)', [('target', 'target'), ('bad', 'bad')])
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute('CREATE TABLE arena(user_id TEXT,score TEXT)')
            uow.executemany('INSERT INTO arena VALUES(?,?)', [('target', '1.00000000000000000001e20'), ('bad', 'invalid-score')])
        rows = ArenaOpponentRepository(self.game, self.player).candidates('self')
        self.assertEqual([('target', 100000000000000000001)], [(r['user_id'], r['score']) for r in rows])


if __name__ == '__main__':
    unittest.main()
