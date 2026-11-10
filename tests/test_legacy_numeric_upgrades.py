from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.features.mixelixir.fire_control_upgrade_repository import MixelixirFireControlUpgradeSqlRepository
from nonebot_plugin_xiuxian_2.features.mixelixir.harvest_level_upgrade_repository import MixelixirHarvestLevelUpgradeSqlRepository
from nonebot_plugin_xiuxian_2.features.mixelixir.migrations import apply_mixelixir_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_mixelixir.transaction_service import MixelixirFireControlUpgradeService, MixelixirHarvestLevelUpgradeService


class LegacyNumericUpgradeTests(unittest.TestCase):
    def build(self, root, cls, experience):
        game, player = Path(root) / 'game.db', Path(root) / 'player.db'
        with DatabaseUnitOfWork(game) as uow:
            uow.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)')
            uow.execute("INSERT INTO user_xiuxian VALUES('u',5000)")
            apply_mixelixir_operations(uow)
        with DatabaseUnitOfWork(player) as uow:
            uow.execute('CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,"丹药控火" TEXT,"收取等级" TEXT,"炼丹经验" TEXT)')
            uow.execute('INSERT INTO mix_elixir_info VALUES(?,?,?,?)', ('u', '0e0', '0e0', experience))
        return cls(game, player), game, player

    @staticmethod
    def snapshot(group):
        result = []
        for path in group[1:]:
            with sqlite3.connect(path) as conn:
                result.append(list(conn.iterdump()))
        return result

    def test_upgrade_scientific_text_and_exact_large_experience(self):
        for cls in (MixelixirFireControlUpgradeSqlRepository, MixelixirHarvestLevelUpgradeSqlRepository,
                    MixelixirFireControlUpgradeService, MixelixirHarvestLevelUpgradeService):
            for value in ('9e2', '100000000000000000001'):
                with self.subTest(cls=cls.__name__, value=value), tempfile.TemporaryDirectory() as root:
                    group = self.build(root, cls, value)
                    result = group[0].upgrade('op', 'u', 0, as_int_like(value), 5000, 1, 100)
                    self.assertEqual(('applied', as_int_like(value) - 100), (result.status, result.experience))
                    before = self.snapshot(group)
                    self.assertEqual('duplicate', group[0].upgrade('op', 'u', 0, as_int_like(value), 5000, 1, 100).status)
                    self.assertEqual(before, self.snapshot(group))
                    with sqlite3.connect(group[2]) as conn:
                        self.assertEqual(as_int_like(value) - 100, as_int_like(conn.execute('SELECT "炼丹经验" FROM mix_elixir_info').fetchone()[0]))

    def test_real_snapshot_changes_and_receipt_rejection_preserve_both_databases(self):
        for cls in (MixelixirFireControlUpgradeSqlRepository, MixelixirHarvestLevelUpgradeSqlRepository,
                    MixelixirFireControlUpgradeService, MixelixirHarvestLevelUpgradeService):
            for failure in ('stale', 'receipt'):
                with self.subTest(cls=cls.__name__, failure=failure), tempfile.TemporaryDirectory() as root:
                    group = self.build(root, cls, '100000000000000000001')
                    if failure == 'stale':
                        with sqlite3.connect(group[2]) as conn:
                            conn.execute('UPDATE mix_elixir_info SET "炼丹经验"=?', ('100000000000000000002',))
                    else:
                        table = 'mixelixir_fire_control_upgrade_operations' if 'FireControl' in cls.__name__ else 'mixelixir_harvest_level_upgrade_operations'
                        with DatabaseUnitOfWork(group[1]) as uow:
                            uow.execute(f"CREATE TRIGGER refuse_receipt BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'receipt refused'); END")
                    before = self.snapshot(group)
                    if failure == 'receipt':
                        with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt refused'):
                            group[0].upgrade('op', 'u', 0, 100000000000000000001, 5000, 1, 100)
                    else:
                        self.assertEqual('state_changed', group[0].upgrade('op', 'u', 0, 100000000000000000001, 5000, 1, 100).status)
                    self.assertEqual(before, self.snapshot(group))
