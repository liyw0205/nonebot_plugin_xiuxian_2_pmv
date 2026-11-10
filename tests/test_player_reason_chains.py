import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.impart_pk.application import ImpartPkApplication
from nonebot_plugin_xiuxian_2.features.impart_pk.migrations import apply_impart_pk_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
from nonebot_plugin_xiuxian_2.features.tower.repository import TowerPurchaseSqlRepository


class ImpartReasonChainTests(unittest.TestCase):
    def test_training_rejections_survive_application_and_reply(self):
        with tempfile.TemporaryDirectory() as directory:
            game, impart, player = [Path(directory) / name for name in ('game', 'impart', 'player')]
            for database, ddl, row in (
                (game, 'CREATE TABLE user_xiuxian(user_id TEXT,exp INTEGER,power INTEGER)', "INSERT INTO user_xiuxian VALUES('u',10,1)"),
                (impart, 'CREATE TABLE xiuxian_impart(user_id TEXT,exp_day INTEGER)', "INSERT INTO xiuxian_impart VALUES('u',20)"),
                (player, 'CREATE TABLE impart_pk_daily(user_id TEXT,exp_used INTEGER,exp_count INTEGER,exp_load INTEGER,exp_gain INTEGER)', "INSERT INTO impart_pk_daily VALUES('u',0,0,0,0)"),
            ):
                with sqlite3.connect(database) as conn:
                    conn.execute(ddl)
                    conn.execute(row)
            with DatabaseUnitOfWork(game) as uow:
                apply_impart_pk_operations(uow)
            application = ImpartPkApplication(game, impart, player)
            replies = []
            for expected_exp, expected_time, reason in ((11,20,'exp_changed'), (10,21,'impart_time_changed')):
                result = application.training_settle(
                    operation_id='test', user_id='u', expected_exp=expected_exp,
                    expected_exp_day=expected_time, expected_daily=dict(exp_used=0,exp_count=0,exp_load=0,exp_gain=0),
                    exp_cost=2, exp_gain=5, exp_load_gain=1, power=2,
                )
                self.assertEqual(result.status, 'state_changed')
                self.assertEqual(result.reason, reason)
                replies.append(result_failure('虚神界修炼', result))
            self.assertNotEqual(*replies)
            self.assertIn('修为', replies[0])
            self.assertIn('时间', replies[1])
            with sqlite3.connect(game) as conn:
                self.assertEqual(conn.execute('SELECT exp FROM user_xiuxian').fetchone()[0], 10)


class PartnerReplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_accepting_player_is_not_mistaken_for_inviter_at_limit(self):
        profiles = {'a':dict(user_name='甲',exp=100,level='练气'), 'b':dict(user_name='乙',exp=100,level='练气')}
        sender = AsyncMock()
        with patch.object(partner,'_sql_message',return_value=Mock(get_user_info_with_id=profiles.get)), \
             patch.object(partner.two_exp_cd,'find_user',side_effect=lambda uid: 0 if uid=='a' else 3), \
             patch.object(partner,'two_exp_limit',3), \
             patch.object(partner.xiuxian_impart,'get_user_impart_info_with_id',return_value={'impart_two_exp':0}), \
             patch.object(partner,'UserBuffDate',return_value=Mock(get_user_main_buff_data=Mock(return_value=None))), \
             patch.object(partner,'handle_send',sender):
            await partner.direct_two_exp(None,Mock(user_id='b'),'a','b',1,invite_id='inv')
        message = sender.call_args.args[2]
        self.assertIn('受邀人乙',message)
        self.assertIn('已用3/上限3',message)
        self.assertIn('双修令',message)

    async def test_process_read_failure_keeps_actual_reason(self):
        with patch.object(partner,'get_user_profile',side_effect=[None,dict(user_name='乙')]):
            one,two,message = await partner.process_two_exp('a','b')
        self.assertEqual((one,two),(0,0))
        self.assertIn('发起人',message)
        self.assertIn('资料不存在',message)
        self.assertNotIn('上限',message)


class TowerRejectionReplayTests(unittest.TestCase):
    def test_rejected_purchase_stays_rejected_and_preserves_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            game, player = Path(directory) / 'game', Path(directory) / 'player'
            with sqlite3.connect(game) as conn:
                conn.executescript('CREATE TABLE user_xiuxian(user_id TEXT);'
                    "INSERT INTO user_xiuxian VALUES('u');"
                    'CREATE TABLE tower_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT,quantity INTEGER,cost INTEGER,score INTEGER,purchased INTEGER,inventory INTEGER);')
            with sqlite3.connect(player) as conn:
                conn.executescript('CREATE TABLE tower(user_id TEXT,score INTEGER,weekly_purchases TEXT);'
                    "INSERT INTO tower VALUES('u',10,'{}');")
            repository = TowerPurchaseSqlRepository(game, player)
            args = ('request','u',1,'item','type',1,2,5,11,{},99)
            first = repository.purchase(*args, today='2026-11-10')
            replay = repository.purchase(*args, today='2026-11-10')
            self.assertEqual(first['reason'], 'tower_score_changed')
            self.assertEqual((replay['status'], replay['reason']), (first['status'], first['reason']))
            self.assertEqual(result_failure('通天塔兑换', first), result_failure('通天塔兑换', replay))
