from __future__ import annotations

import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
from tests import test_repositories as fixture


class CommonCurrencyPrecisionTests(unittest.TestCase):
    setUp = fixture.RepositoryTests.setUp
    tearDown = fixture.RepositoryTests.tearDown
    connection = fixture.RepositoryTests.connection
    read_query = fixture.RepositoryTests.read_query
    log_change = fixture.RepositoryTests.log_change

    def repo(self):
        return fixture.repositories.EconomyRepository(self.connection, int, self.log_change)

    def balances(self):
        return tuple(as_int_like(row[0]) for row in self.read_query(
            "SELECT stone FROM user_xiuxian ORDER BY user_id"))

    def set_balance(self, value):
        with self.connection() as conn:
            conn.execute("UPDATE user_xiuxian SET stone=%s", (bind_sqlite_param(value, exact=True),))
            conn.commit()

    def test_common_add_cost_and_transfer_preserve_adjacent_values(self):
        for value in (2**53 + 1, 2**63 - 1, 2**63 + 1, 10**45 + 123):
            with self.subTest(value=value):
                self.set_balance(value)
                repo = self.repo()
                repo.update_stones("1001", 1, 1)
                self.assertEqual((value + 1, value), self.balances())
                self.assertTrue(repo.try_update_stones("1001", 2, 2))
                self.assertEqual((value - 1, value), self.balances())
                self.assertTrue(repo.transfer_stones(str(value), "1001", "1002", 1))
                self.assertEqual((value - 2, value + 1), self.balances())
                self.assertFalse(repo.transfer_stones(str(value), "1001", "1002", 1))
                self.assertEqual((value - 2, value + 1), self.balances())

    def test_bad_history_refuses_before_any_mutation(self):
        for value, reason in ((float(2**63), "stone_precision_lost_real"),
                              (1.5, "stone_invalid_numeric_value"),
                              ("broken", "stone_invalid_numeric_value")):
            for action in (lambda r: r.update_stones("1001", 1, 1),
                           lambda r: r.try_update_stones("1001", 1, 2),
                           lambda r: r.transfer_stones("bad", "1002", "1001", 1)):
                with self.subTest(value=value, action=action):
                    self.set_balance(value)
                    before = self.read_query("SELECT * FROM user_xiuxian ORDER BY user_id")
                    with self.assertRaises(DomainError) as caught:
                        action(self.repo())
                    self.assertEqual(reason, caught.exception.details["reason"])
                    self.assertEqual(before, self.read_query("SELECT * FROM user_xiuxian ORDER BY user_id"))
                    self.assertEqual([], self.read_query("SELECT * FROM economy_operations"))

    def test_transfer_receipt_failure_rolls_back_both_accounts(self):
        self.set_balance(10**40 + 1)
        with self.connection() as conn:
            conn.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON economy_operations "
                         "BEGIN SELECT RAISE(ABORT,'receipt fault'); END")
            conn.commit()
        before = self.balances()
        with self.assertRaisesRegex(Exception, "receipt fault"):
            self.repo().transfer_stones("fault", "1001", "1002", 2)
        self.assertEqual(before, self.balances())

    def test_ledger_exact_delta_replay_and_payload_conflict(self):
        from threading import RLock
        from types import SimpleNamespace
        from nonebot_plugin_xiuxian_2.features.economy_ledger.migrations import apply_economy_log_schema
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import economy_log

        with DatabaseUnitOfWork(str(self.database), immediate=True) as uow:
            apply_economy_log_schema(uow)
        with self.connection() as conn:
            manager = SimpleNamespace(conn=conn, lock=RLock(), _commit_write=conn.commit)
            with patch.object(economy_log, "XiuxianDateManage", return_value=manager):
                value = 10**45 + 123
                args = dict(user_id="1001", source="test", action="reward", event_id="exact-ledger")
                first = economy_log.log_economy_change(**args, stone_delta=value)
                row = conn.execute("SELECT stone_delta FROM economy_log").fetchone()
                self.assertEqual(value, as_int_like(row[0]))
                self.assertEqual(first, economy_log.log_economy_change(**args, stone_delta=str(value)))
                with self.assertRaisesRegex(ValueError, "payload conflict"):
                    economy_log.log_economy_change(**args, stone_delta=value + 1)
                self.assertEqual(value, as_int_like(conn.execute("SELECT stone_delta FROM economy_log").fetchone()[0]))

    def test_native_reward_writer_persists_exact_balance_not_just_result(self):
        from nonebot_plugin_xiuxian_2.features.base.economy_repository import PlayerEconomySqlRepository
        repo = PlayerEconomySqlRepository(self.database)
        for value in (2**53 + 1, 2**63 - 1, 2**63 + 1, 10**45 + 123):
            with self.subTest(value=value):
                self.set_balance(value)
                result = repo.grant_stone("1001", 1)
                self.assertEqual(("applied", value + 1), (result.status, result.value))
                self.assertEqual((value + 1, value), self.balances())

    def test_native_reward_writer_refuses_unrecoverable_history_zero_write(self):
        from nonebot_plugin_xiuxian_2.features.base.economy_repository import PlayerEconomySqlRepository
        self.set_balance(float(2**63))
        before = self.read_query("SELECT * FROM user_xiuxian ORDER BY user_id")
        with self.assertRaises(DomainError) as caught:
            PlayerEconomySqlRepository(self.database).grant_stone("1001", 1)
        self.assertEqual("stone_precision_lost_real", caught.exception.details["reason"])
        self.assertEqual("1001", caught.exception.details["subject"])
        self.assertEqual(before, self.read_query("SELECT * FROM user_xiuxian ORDER BY user_id"))

    def test_native_contribution_reward_preserves_cas_and_adjacent_large_values(self):
        from nonebot_plugin_xiuxian_2.features.base.economy_repository import PlayerEconomySqlRepository
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        repo = PlayerEconomySqlRepository(self.database)
        value = 10**45 + 123
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute('ALTER TABLE user_xiuxian ADD COLUMN sect_contribution INTEGER')
            uow.execute('UPDATE user_xiuxian SET sect_contribution=?', (bind_sqlite_param(value, exact=True),))
        result = repo.grant_sect_contribution('1001', 1, expected_value=str(value) + 'e0')
        self.assertEqual(('applied', value + 1), (result.status, result.value))
        self.assertEqual('state_changed', repo.grant_sect_contribution('1001', 1, expected_value=value).status)
        row = self.read_query("SELECT sect_contribution FROM user_xiuxian WHERE user_id='1001'")[0]
        self.assertEqual(value + 1, as_int_like(row[0]))
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE user_xiuxian SET sect_contribution=? WHERE user_id='1001'", (float(2**63),))
        before = self.read_query('SELECT * FROM user_xiuxian ORDER BY user_id')
        with self.assertRaises(DomainError) as caught:
            repo.grant_sect_contribution('1001', 1)
        self.assertEqual('sect_contribution_precision_lost_real', caught.exception.details['reason'])
        self.assertEqual(before, self.read_query('SELECT * FROM user_xiuxian ORDER BY user_id'))


if __name__ == "__main__":
    unittest.main()
