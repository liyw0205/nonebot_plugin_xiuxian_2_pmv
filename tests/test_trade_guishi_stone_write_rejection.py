"""A transfer must not commit a wallet change with a rejected projection/receipt."""
import unittest

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests import test_trade_guishi_deposit_repository as deposit_fixture
from tests import test_trade_guishi_withdraw_repository as withdraw_fixture


class GuishiStoneWriteRejectionTests(unittest.TestCase):
    def check_rejection(self, action, target, fault):
        cls = (deposit_fixture.GuishiDepositSqlRepositoryTests if action == 'deposit'
               else withdraw_fixture.GuishiWithdrawSqlRepositoryTests)
        fixture = cls()
        fixture.setUp()
        try:
            database = fixture.trade_database if target in {'balance', 'new_balance'} else fixture.game_database
            table = ('guishi_info' if target in {'balance', 'new_balance'} else
                     'user_xiuxian' if target == 'wallet' else f'trade_guishi_{action}_operations')
            if target == 'balance' and action == 'deposit':
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute("INSERT INTO guishi_info VALUES('u',800,'{\"1001\":3}')")
            if target == 'balance':
                statement = ("UPDATE guishi_info SET items='corrupted' WHERE user_id=OLD.user_id; "
                             "SELECT RAISE(IGNORE);" if fault == 'ignore' else
                             "UPDATE guishi_info SET stored_stone='invalid' WHERE user_id=NEW.user_id;" if fault == 'invalid' else
                             "UPDATE guishi_info SET stored_stone=NEW.stored_stone+1 WHERE user_id=NEW.user_id;")
                timing = 'BEFORE' if fault == 'ignore' else 'AFTER'
                sql = f"CREATE TRIGGER reject_transfer {timing} UPDATE ON {table} BEGIN {statement} END"
            elif target == 'wallet':
                sql = ("CREATE TRIGGER reject_transfer BEFORE UPDATE ON user_xiuxian BEGIN "
                       "UPDATE user_xiuxian SET stone=1 WHERE user_id=OLD.user_id; SELECT RAISE(IGNORE); END")
            elif target == 'receipt' and fault == 'rewrite':
                sql = (f"CREATE TRIGGER reject_transfer AFTER INSERT ON {table} BEGIN "
                       f"UPDATE {table} SET amount=NEW.amount+1 WHERE operation_id=NEW.operation_id; END")
            else:
                sql = f"CREATE TRIGGER reject_transfer BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(sql)
            before = fixture.state()
            with DatabaseUnitOfWork(fixture.trade_database, read_only=True) as uow:
                account_before = uow.query_all('SELECT * FROM guishi_info')
            call = fixture.deposit if action == 'deposit' else fixture.withdraw
            result = call('rejected', 100)
            expected = ('stored_stone_invalid_numeric_value' if fault == 'invalid' else
                        'guishi_balance_write_rejected' if target in {'balance', 'new_balance'} else
                        'stone_write_rejected' if target == 'wallet' else 'guishi_receipt_write_rejected')
            self.assertEqual(fixture.state(), before)
            with DatabaseUnitOfWork(fixture.trade_database, read_only=True) as uow:
                self.assertEqual(uow.query_all('SELECT * FROM guishi_info'), account_before)
            self.assertEqual((result.status, result.reason, result.subject), ('state_changed', expected, 'u'))
            with DatabaseUnitOfWork(database) as uow:
                uow.execute('DROP TRIGGER reject_transfer')
            recovered = call('rejected', 100)
            self.assertEqual(recovered.status, 'completed')
            after = fixture.state()
            self.assertEqual(call('rejected', 100).status, 'duplicate')
            self.assertEqual(fixture.state(), after)
        finally:
            fixture.tearDown()

    def test_ignored_deposit_balance_does_not_lose_wallet_stones(self):
        self.check_rejection('deposit', 'balance', 'ignore')

    def test_ignored_withdraw_balance_does_not_mint_wallet_stones(self):
        self.check_rejection('withdraw', 'balance', 'ignore')

    def test_rewritten_deposit_balance_rolls_back_the_transfer(self):
        self.check_rejection('deposit', 'balance', 'rewrite')

    def test_rewritten_withdraw_balance_rolls_back_the_transfer(self):
        self.check_rejection('withdraw', 'balance', 'rewrite')

    def test_ignored_deposit_receipt_does_not_commit_an_unreplayable_transfer(self):
        self.check_rejection('deposit', 'receipt', 'ignore')

    def test_ignored_withdraw_receipt_does_not_commit_an_unreplayable_transfer(self):
        self.check_rejection('withdraw', 'receipt', 'ignore')

    def test_ignored_new_account_rolls_back_the_deposit(self):
        self.check_rejection('deposit', 'new_balance', 'ignore')

    def test_wallet_rejections_roll_back_trigger_side_effects(self):
        for action in ('deposit', 'withdraw'):
            with self.subTest(action=action):
                self.check_rejection(action, 'wallet', 'ignore')

    def test_rewritten_receipts_cannot_commit_inconsistent_transfers(self):
        for action in ('deposit', 'withdraw'):
            with self.subTest(action=action):
                self.check_rejection(action, 'receipt', 'rewrite')

    def test_invalid_balance_retains_numeric_reason_and_subject_after_rollback(self):
        for action in ('deposit', 'withdraw'):
            with self.subTest(action=action):
                self.check_rejection(action, 'balance', 'invalid')

    def test_web_rejection_preserves_reason_and_historical_rejection(self):
        from flask import Flask
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        from nonebot_plugin_xiuxian_2.features.trade.web import blueprint
        for action in ('deposit', 'withdraw'):
            with self.subTest(action=action):
                cls = (deposit_fixture.GuishiDepositSqlRepositoryTests if action == 'deposit'
                       else withdraw_fixture.GuishiWithdrawSqlRepositoryTests)
                fixture = cls()
                fixture.setUp()
                try:
                    application = (TradeApplication(fixture.game_database, fixture.trade_database)
                                   if action == 'deposit' else fixture.application)
                    app = Flask(__name__)
                    app.secret_key = 'test'
                    app.register_blueprint(blueprint(application, permission=lambda _: True))
                    client = app.test_client()
                    with client.session_transaction() as session:
                        session['_csrf_token'] = 'csrf'
                    with DatabaseUnitOfWork(fixture.game_database) as uow:
                        uow.execute(f"CREATE TRIGGER reject_transfer BEFORE INSERT ON trade_guishi_{action}_operations "
                                    "BEGIN SELECT RAISE(IGNORE); END")
                    before = fixture.state()

                    def request(key):
                        return client.post(f'/api/v1/trade/{action}',
                                           headers={'Idempotency-Key': key, 'X-CSRF-Token': 'csrf'},
                                           json={'user_id': 'u', 'amount': 100})

                    response = request('rejected')
                    self.assertEqual(response.status_code, 409)
                    data = response.get_json()['data']['data']
                    self.assertEqual((data['status'], data['reason'], data['subject']),
                                     ('state_changed', 'guishi_receipt_write_rejected', 'u'))
                    self.assertEqual(fixture.state(), before)
                    with DatabaseUnitOfWork(fixture.game_database) as uow:
                        uow.execute('DROP TRIGGER reject_transfer')
                    replay = request('rejected')
                    self.assertEqual(replay.status_code, 409)
                    self.assertEqual(replay.get_json()['data']['data'], data)
                    self.assertEqual(fixture.state(), before)
                    self.assertEqual(request('recovery').status_code, 200)
                    after = fixture.state()
                    self.assertEqual(request('recovery').status_code, 200)
                    self.assertEqual(fixture.state(), after)
                finally:
                    fixture.tearDown()


if __name__ == '__main__':
    unittest.main()
