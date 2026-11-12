from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
from nonebot_plugin_xiuxian_2.features.admin_asset.level_repository import AdminLevelChangeSqlRepository
from nonebot_plugin_xiuxian_2.features.admin_asset.migrations import apply_admin_realm_changes
from nonebot_plugin_xiuxian_2.features.base.economy_repository import PlayerEconomySqlRepository
from nonebot_plugin_xiuxian_2.features.buff.migrations import apply_normal_training_game
from nonebot_plugin_xiuxian_2.features.buff.training_start_repository import NormalTrainingStartSqlRepository
from nonebot_plugin_xiuxian_2.features.buff.training_complete_repository import NormalTrainingCompleteSqlRepository
from nonebot_plugin_xiuxian_2.features.bank.account_application import BankDepositApplication
from nonebot_plugin_xiuxian_2.features.bank.command_receipt_repository import BankCommandReceiptRepository
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.features.player_state.repository import PlayerStateRepository
from nonebot_plugin_xiuxian_2.features.package_reward.repository import PackageRewardRepository
from nonebot_plugin_xiuxian_2.features.stone_gift.application import StoneGiftApplication
from nonebot_plugin_xiuxian_2.features.stone_gift.migrations import apply_stone_gift, apply_stone_gift_limits
from nonebot_plugin_xiuxian_2.features.stone_gift.repository import StoneGiftRepository
from nonebot_plugin_xiuxian_2.features.training.event_repository import TrainingEventSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.readonly import ReadOnlyQuery
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
from tests.test_db_backend import db_backend


VALUES = (123, 2**63 - 1, 1e29, '1.00000000000000000000000000001e29', 0, None)
MONEY_VALUES = (123, 123.0, 2**53 + 1, 2**63 - 1, 2**63 + 1,
                10**45 + 123, '1.00000000000000000000000000001e29', b'123', 0, None)


class LargeNumericComparisonTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / 'game.db'
        with DatabaseUnitOfWork(self.database) as uow:
            # No affinity retains historical scientific TEXT beside REAL/INTEGER.
            uow.execute('CREATE TABLE user_xiuxian(user_id TEXT,level TEXT,exp,stone,hp,mp,atk,power,root_type TEXT,root_level INTEGER)')
            apply_admin_realm_changes(uow)
            apply_platform_schema(uow)
            apply_stone_gift(uow)
            apply_stone_gift_limits(uow)

    def insert(self, value, user_id='u', *, safe_wallet=False):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('DELETE FROM user_xiuxian WHERE user_id=?', (user_id,))
            stone = bind_sqlite_param(as_int_like(value), exact=True) if safe_wallet else value
            uow.execute('INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?,?,?,?)', (user_id, 'old', value, stone, value, value, value, value, 'root', 0))

    def test_economy_grants_storage_matrix(self):
        repo = PlayerEconomySqlRepository(self.database)
        for value in VALUES:
            with self.subTest(value=value):
                self.insert(value)
                expected = as_int_like(value)
                result = repo.grant_experience('u', 10**20, max_exp=10**32, expected_exp=expected)
                self.assertEqual('applied', result.status)
                self.assertEqual(expected + 10**20, result.value)
                self.assertEqual('state_changed', repo.grant_experience('u', 1, max_exp=10**32, expected_exp=expected).status)
        for value in MONEY_VALUES:
            with self.subTest(money=value):
                self.insert(value, safe_wallet=True)
                result = repo.grant_stone('u', 10**20)
                self.assertEqual(('applied', as_int_like(value) + 10**20), (result.status, result.value))
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    self.assertEqual(result.value, as_int_like(uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")['stone']))

    def test_admin_joint_snapshot_storage_matrix_and_replay(self):
        repo = AdminLevelChangeSqlRepository(self.database)
        for index, value in enumerate(VALUES):
            with self.subTest(value=value):
                self.insert(value)
                expected = ('old', *([as_int_like(value)] * 5), 'root', 0)
                args = (f'level-{index}', 'admin', 'u', expected, 'new', 10**25, 2.0, 1.0)
                self.assertEqual('applied', repo.change(*args).status)
                self.assertEqual('duplicate', repo.change(*args).status)

    def test_admin_exact_changes_rejected_with_reason(self):
        repo = AdminLevelChangeSqlRepository(self.database)
        for value in (2**53 + 1, '1.00000000000000000000000000001e29'):
            for offset, field in enumerate(('exp', 'hp', 'mp', 'atk', 'power'), 1):
                with self.subTest(value=value, field=field):
                    self.insert(value)
                    expected = ['old', *([as_int_like(value)] * 5), 'root', 0]
                    expected[offset] -= 1
                    result = repo.change('stale', 'admin', 'u', expected, 'new', 1000, 2, 1)
                    self.assertEqual(('state_changed', f'{field}_changed'), (result.status, result.reason))

    def test_snapshots_sum_canonical_rows_without_int64_saturation(self):
        with DatabaseUnitOfWork(self.database) as uow:
            for value in MONEY_VALUES:
                uow.execute('INSERT INTO user_xiuxian(user_id,stone) VALUES(?,?)', ('u', value))
            expected = {'rows': len(MONEY_VALUES), 'stone': sum(as_int_like(v) for v in MONEY_VALUES)}
            self.assertEqual(expected, PackageRewardRepository().snapshot(uow, 'u'))
            self.assertEqual({'u': expected, 'missing': {'rows': 0, 'stone': 0}}, StoneGiftRepository().snapshot(uow, ('u', 'missing', 'u')))

    def test_large_transfer_and_replay(self):
        self.insert(10**29 + 1, 'sender', safe_wallet=True)
        self.insert(0, 'recipient')
        app = StoneGiftApplication(self.database)
        args = dict(operation_id='gift', sender_id='sender', recipient_id='recipient', gross_amount=10**25)
        result = app.transfer(**args)
        self.assertTrue(result.ok, result)
        self.assertEqual('replayed', app.transfer(**args).status)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(1, uow.query_one('SELECT COUNT(*) AS n FROM stone_gift_operations')['n'])

    def test_admin_receipt_failure_rolls_back_all_fields(self):
        self.insert(1e29)
        with sqlite3.connect(self.database) as conn:
            before = conn.execute('SELECT * FROM user_xiuxian').fetchall()
            conn.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON admin_level_change_operations BEGIN SELECT RAISE(ABORT,'receipt'); END")
        expected = ('old', *([as_int_like(1e29)] * 5), 'root', 0)
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt'):
            AdminLevelChangeSqlRepository(self.database).change('rollback', 'admin', 'u', expected, 'new', 10**25, 2, 1)
        with sqlite3.connect(self.database) as conn:
            self.assertEqual(before, conn.execute('SELECT * FROM user_xiuxian').fetchall())
            self.assertEqual(0, conn.execute('SELECT COUNT(*) FROM admin_level_change_operations').fetchone()[0])

    def test_vitals_cas_preserves_exact_changes(self):
        for value in VALUES:
            with self.subTest(value=value):
                self.insert(value)
                expected = as_int_like(value)
                repo = PlayerStateRepository(self.database)
                self.assertEqual('applied', repo.update_vitals('u', 100, 100, expected_hp=expected, expected_mp=expected).status)
                self.assertEqual('state_changed', repo.update_vitals('u', 1, 1, expected_hp=expected+1).status)

    def test_training_start_complete_attached_and_idempotent(self):
        player = Path(self.temp.name) / 'player.db'
        with DatabaseUnitOfWork(player) as uow:
            uow.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"修炼次数" INTEGER,"修炼修为" INTEGER)')
            uow.execute('CREATE TABLE xiuxian_tasks(user_id TEXT PRIMARY KEY,weekly_period TEXT,weekly_progress TEXT)')
        with DatabaseUnitOfWork(self.database) as uow:
            apply_normal_training_game(uow)
            uow.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)')
        for index, value in enumerate(VALUES):
            with self.subTest(value=value):
                self.insert(value, safe_wallet=True)
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("INSERT OR REPLACE INTO user_cd VALUES('u',0,'0',NULL)")
                args = (f'train-{index}', 'u', 'cultivation', as_int_like(value), as_int_like(value), 10**20, 10**32, 2.0)
                self.assertEqual('started', NormalTrainingStartSqlRepository(self.database).start(*args).status)
                repo = NormalTrainingCompleteSqlRepository(self.database, player)
                self.assertEqual('applied', repo.complete(args[0], '2026-W46', 'u').status)
                self.assertEqual('duplicate', repo.complete(args[0], '2026-W46', 'u').status)

    def test_large_bank_balance_threshold_and_replay(self):
        with DatabaseUnitOfWork(self.database) as uow:
            apply_bank_accounts(uow)
        for index, value in enumerate((10**29 + 1, '1.00000000000000000000000000001e29', 2**53+1)):
            with self.subTest(value=value):
                self.insert(value)
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute('DELETE FROM bank_accounts')
                args = dict(operation_id=f'bank-{index}', user_id='u', amount=as_int_like(value), interest=0, limit=10**32, bank_level='1', settled_at='2026-11-11')
                app = BankDepositApplication(self.database)
                self.assertEqual('applied', app.deposit(**args)['status'])
                self.assertEqual('duplicate', app.deposit(**args)['status'])

    def test_comparator_connections_exact_text_and_null_semantics(self):
        self.insert('1.00000000000000000000000000001e29')
        expected = 10**29 + 1
        query = 'SELECT xiuxian_int_cmp(stone,?) AS comparison FROM user_xiuxian WHERE user_id=?'
        for boundary in ('uow', 'legacy', 'legacy_ro', 'readonly', 'attached_ro'):
            with self.subTest(boundary=boundary):
                if boundary == 'readonly':
                    result = ReadOnlyQuery(self.database).one(query, (expected, 'u'))['comparison']
                elif boundary.startswith('legacy'):
                    connection = db_backend.connect_readonly(self.database) if boundary == 'legacy_ro' else db_backend.connect(self.database)
                    try:
                        result = connection.execute(query, (expected, 'u')).fetchone()[0]
                    finally:
                        connection.close()
                else:
                    with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                        if boundary == 'attached_ro':
                            uow.attach_database(self.database, 'copy', read_only=True)
                        result = uow.query_one(query.replace('FROM user_xiuxian', 'FROM copy.user_xiuxian') if boundary == 'attached_ro' else query, (expected, 'u'))['comparison']
                self.assertEqual(0, result)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            for a, b in ((expected, expected-1), (2**63-1, 2**63-2), (2**53+1, 2**53), (int(1e29), int(1e29)-1)):
                self.assertEqual(1, uow.query_one('SELECT xiuxian_int_cmp(?,?) AS n', (a, b))['n'])
            self.assertEqual({'same': 0, 'different': -1, 'defaulted': 0}, uow.query_one('SELECT xiuxian_int_cmp(NULL,NULL) AS same,xiuxian_int_cmp(NULL,0) AS different,xiuxian_int_cmp(COALESCE(NULL,0),0) AS defaulted'))

    def test_bank_command_receipt_large_balance_and_exact_identity(self):
        with DatabaseUnitOfWork(self.database) as uow:
            apply_bank_accounts(uow)
        self.insert(10**30 + 123, safe_wallet=True)
        amount = 10**25 + 1
        result = BankDepositApplication(self.database).deposit(operation_id='command', user_id='u', amount=amount, interest=0, limit=10**32, bank_level='1', settled_at='today')
        self.assertEqual('applied', result['status'])
        repo = BankCommandReceiptRepository(self.database)
        args = dict(operation_id='command', user_id='u', action='deposit', amount=amount)
        receipt = repo.find(**args)
        self.assertEqual('duplicate', receipt['status'])
        self.assertEqual(amount, receipt['deposited'])
        self.assertEqual('operation_conflict', repo.find(**{**args, 'amount': amount+1})['status'])
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('UPDATE bank_account_operations SET deposited=?', (str(amount+1)+'e0',))
        # An INTEGER-affinity receipt rounds above int64; use a historical TEXT
        # receipt to prove the precise one-unit corruption is still rejected.
        with sqlite3.connect(self.database) as conn:
            row = conn.execute('SELECT * FROM bank_account_operations').fetchone()
            conn.execute('ALTER TABLE bank_account_operations RENAME TO old_receipts')
            conn.execute('CREATE TABLE bank_account_operations(operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at)')
            conn.execute('INSERT INTO bank_account_operations VALUES(?,?,?,?,?,?,?,?)', (*row[:3], str(amount+1)+'e0', *row[4:]))
        self.assertEqual('receipt_invalid', repo.find(**args)['status'])

    def test_training_attached_failure_rolls_back_game_and_player(self):
        self.insert(1e29, safe_wallet=True)
        player = Path(self.temp.name) / 'player.db'
        with DatabaseUnitOfWork(player) as uow:
            uow.execute('CREATE TABLE statistics(user_id TEXT PRIMARY KEY,"修炼次数" INTEGER,"修炼修为" INTEGER)')
            uow.execute('CREATE TABLE xiuxian_tasks(user_id TEXT PRIMARY KEY,weekly_period TEXT,weekly_progress TEXT)')
            uow.execute("CREATE TRIGGER reject_stat BEFORE INSERT ON statistics BEGIN SELECT RAISE(ABORT,'stat'); END")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_normal_training_game(uow)
            uow.execute('CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)')
            uow.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
        expected = as_int_like(1e29)
        self.assertEqual('started', NormalTrainingStartSqlRepository(self.database).start('rollback-train', 'u', 'cultivation', expected, expected, 10**20, 10**32, 2.0).status)
        def snapshot():
            with sqlite3.connect(self.database) as game, sqlite3.connect(player) as stats:
                return tuple(game.iterdump()), tuple(stats.iterdump())
        before = snapshot()
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'stat'):
            NormalTrainingCompleteSqlRepository(self.database, player).complete('rollback-train', '2026-W46', 'u')
        self.assertEqual(before, snapshot())

    def test_training_state_numeric_snapshot_is_canonical_and_strict(self):
        for value in MONEY_VALUES:
            with self.subTest(value=value):
                self.assertTrue(TrainingEventSqlRepository._state_matches({'points': value}, {'points': as_int_like(value)}, ('points',)))
                self.assertFalse(TrainingEventSqlRepository._state_matches({'points': value}, {'points': as_int_like(value)+1}, ('points',)))
        self.assertFalse(TrainingEventSqlRepository._state_matches({'last_time': '1e2'}, {'last_time': 100}, ('last_time',)))

    def test_invalid_numeric_operands_never_satisfy_sql_guards(self):
        invalid = ('bad', '', 'NaN', 'Infinity', '-Infinity', float('inf'), float('-inf'), b'\xff')
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            for value in invalid:
                for left, right in ((value, 0), (0, value), (value, None), (None, value)):
                    with self.subTest(left=left, right=right):
                        result = uow.query_one('SELECT xiuxian_int_cmp(?,?) AS n', (left, right))
                        self.assertIsNone(result['n'])
            self.assertEqual(0, uow.query_one('SELECT xiuxian_int_cmp(?,0) AS n', (b'0',))['n'])

    def test_invalid_numeric_balances_cannot_be_overwritten_as_zero(self):
        repo = PlayerEconomySqlRepository(self.database)
        for value in ('bad', '', 'NaN', 'Infinity', float('inf')):
            with self.subTest(value=value):
                self.insert(value)
                self.assertEqual('state_changed', repo.grant_experience('u', 1, max_exp=10**32, expected_exp=0).status)
                with self.assertRaises(DomainError) as caught:
                    repo.grant_stone('u', 1)
                self.assertEqual('stone_invalid_numeric_value', caught.exception.details['reason'])
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    self.assertEqual({'exp': value, 'stone': value}, uow.query_one('SELECT exp,stone FROM user_xiuxian WHERE user_id=?', ('u',)))
