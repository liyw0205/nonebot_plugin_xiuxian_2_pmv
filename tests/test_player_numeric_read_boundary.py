from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from tests.test_player_field_list_cache import module
from tests import test_partner_cultivation_transaction as partner_tests
from tests.test_db_backend import db_backend
from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner_storage


class PlayerNumericReadBoundaryTests(unittest.TestCase):
    setUp = partner_tests.Tests.setUp
    tearDown = partner_tests.Tests.tearDown
    call = partner_tests.Tests.call
    def test_exact_text_survives_all_manager_read_apis_and_cache(self):
        value = '1.00000000000000000001e20'
        with db_backend.transaction(self.p) as conn:
            conn.execute('DROP TABLE partner')
            conn.execute('CREATE TABLE partner(user_id TEXT PRIMARY KEY,partner_id TEXT,affection TEXT,ratio TEXT,payload TEXT,bind_time TEXT)')
            conn.execute('INSERT INTO partner VALUES(%s,%s,%s,%s,%s,%s)',
                         ('a', 'b', value, '1.25', '{"count":[1,2]}', '2026-11-11'))
            conn.execute("INSERT INTO partner(user_id,partner_id,affection) VALUES('b','a','4')")
        manager = object.__new__(module.PlayerDataManager)
        manager._conn_lock = threading.RLock()
        manager._ensured_tables = {'partner'}
        manager._ensured_fields = {('partner', 'affection')}
        manager._field_list_cache = {}
        manager._field_list_cache_charged_bytes = 0
        manager.conn = db_backend.connect(self.p)
        self.addCleanup(manager.conn.close)
        expected = as_int_like(value)
        self.assertEqual(expected, manager.get_fields('a', 'partner')['affection'])
        self.assertEqual(expected, manager.get_field_data('a', 'partner', 'affection'))
        self.assertEqual(expected, dict(manager.get_all_field_data('partner', 'affection'))['a'])
        self.assertEqual(expected, dict(manager.get_all_field_data('partner', 'affection'))['a'])
        self.assertEqual(expected, manager.get_all_records('partner')[0]['affection'])
        row = manager.get_fields('a', 'partner')
        self.assertEqual((1.25, {'count': [1, 2]}, '2026-11-11'), (row['ratio'], row['payload'], row['bind_time']))
        with patch.object(partner_storage, 'player_data_manager', manager):
            snapshot = partner_storage.load_partner('a')
        self.assertEqual(expected, snapshot['affection'])
        self.assertEqual('applied', self.call(aff=snapshot['affection']).status)
        self.assertEqual('duplicate', self.call(aff=snapshot['affection']).status)

    def test_integer_field_real_and_invalid_values_keep_their_contract(self):
        for value in (1e29, None, 'None', '', 'not-a-number', 'null', '{"x":1}', '[1,2]'):
            with self.subTest(value=value):
                decoded = module._decode_field('partner', 'affection', value)
                if isinstance(value, float):
                    self.assertEqual(int(value), decoded)
                elif value == 'null':
                    self.assertIsNone(decoded)
                elif value == '{"x":1}':
                    self.assertEqual({'x': 1}, decoded)
                elif value == '[1,2]':
                    self.assertEqual([1, 2], decoded)
                else:
                    self.assertEqual(value, decoded)
