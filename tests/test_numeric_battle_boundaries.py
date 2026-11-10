from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from tests.test_world_boss_battle_settlement import create_databases, settle
from nonebot_plugin_xiuxian_2.features.boss.battle_repository import WorldBossBattleSettlementSqlRepository
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_settlement, apply_boss_player_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import WorldBossBattleSettlementService


class NumericBossBattleTests(unittest.TestCase):
    def test_scientific_limits_settle_replay_reject_changes_and_roll_back(self):
        for cls in (WorldBossBattleSettlementSqlRepository, WorldBossBattleSettlementService):
            for failure in ('success', 'stale', 'receipt'):
                with self.subTest(service=cls.__name__, failure=failure), tempfile.TemporaryDirectory() as temp:
                    game, player, _ = create_databases(Path(temp))
                    with DatabaseUnitOfWork(game) as uow:
                        apply_boss_settlement(uow)
                    with DatabaseUnitOfWork(player) as uow:
                        apply_boss_player_schema(uow)
                        uow.execute('DROP TABLE boss')
                        uow.execute('CREATE TABLE boss(user_id TEXT PRIMARY KEY,boss_stone TEXT,boss_integral TEXT,boss_battle_count TEXT)')
                        uow.execute("INSERT INTO boss VALUES('10001','1e2','1e1','2e0')")
                        uow.execute('DROP TABLE boss_limit')
                        uow.execute('CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral TEXT)')
                        uow.execute("INSERT INTO boss_limit VALUES('10001','5e1')")
                    if failure == 'stale':
                        with sqlite3.connect(player) as conn:
                            conn.execute("UPDATE boss_limit SET integral='51'")
                    if failure == 'receipt':
                        with sqlite3.connect(game) as conn:
                            conn.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON world_boss_battle_operations BEGIN SELECT RAISE(ABORT,'battle receipt refused'); END")
                    def snapshot():
                        result = []
                        for path in (game, player):
                            with sqlite3.connect(path) as conn:
                                result.append(list(conn.iterdump()))
                        return result
                    service = cls(game, player)
                    before = snapshot()
                    if failure == 'receipt':
                        with self.assertRaisesRegex(sqlite3.IntegrityError, 'battle receipt refused'):
                            settle(service)
                    elif failure == 'stale':
                        self.assertEqual('state_changed', settle(service).status)
                    else:
                        self.assertEqual('applied', settle(service).status)
                        before = snapshot()
                        self.assertEqual('duplicate', settle(service).status)
                    self.assertEqual(before, snapshot())
