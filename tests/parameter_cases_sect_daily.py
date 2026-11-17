"""Real player commands observe daily maintenance and recover after task failure."""
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

from tests.parameter_cases_auction import fire
from tests.parameter_cases_guishi_return import integrity
from tests.parameter_cases_market import FixedClock
from tests.parameter_cases_sect_body import sect_module, seed_sect, sect_rows


@contextmanager
def setup(probe):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_scheduler as scheduled
    module, database = sect_module(), probe.context.paths.game_db
    first = seed_sect(probe, members=False)
    sects = [first]
    for uid, key, name in (('900002', 'daily-second', '参数降级宗'), ('900003', 'daily-third', '参数失效宗')):
        probe.player(uid, stone=10**12, sect_id=None, sect_position=None)
        created = module.sect_application.create_sect(key, uid, name, 0, 0)
        assert created.applied, created
        sects.append(created.sect_id)
    for sid, level, materials in zip(sects, (1, 2, 1), (1000, 500, 100)):
        probe.seed(database, 'UPDATE sects SET elixir_room_level=?,sect_materials=? WHERE sect_id=?', (level, materials, sid))
    for uid in ('900001', '900002', '900003'):
        probe.player(uid, sect_task=module.config['每日宗门任务次上限'], sect_elixir_get=1, sect_contribution=10**12)
    clock = FixedClock(datetime(2026, 11, 17, 16, 1, tzinfo=timezone.utc))
    config = deepcopy(module.config)
    for key, value in (('1', 200), ('2', 600)):
        config['宗门丹房参数']['elixir_room_level'][key]['level_up_cost']['建设度'] = value
    with patch.object(module, 'runtime_clock', clock), patch.object(module, 'config', config):
        yield SimpleNamespace(module=module, scheduled=scheduled, database=database, clock=clock, sects=sects)


def snapshot(probe):
    game, trade, player = probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db
    return (probe.rows(game, 'SELECT * FROM user_xiuxian ORDER BY user_id'), sect_rows(probe),
            probe.rows(game, 'SELECT * FROM user_cd ORDER BY user_id'),
            probe.rows(game, 'SELECT * FROM sect_daily_reset_operations ORDER BY business_date'),
            probe.rows(game, 'SELECT * FROM sect_elixir_claim_operations ORDER BY operation_id'),
            probe.rows(game, 'SELECT * FROM back ORDER BY user_id,goods_id'),
            probe.rows(game, 'SELECT * FROM domain_outbox ORDER BY event_id'),
            probe.rows(game, 'SELECT * FROM operation_ledger ORDER BY operation_id'),
            probe.rows(trade, 'SELECT * FROM guishi_info ORDER BY user_id'),
            probe.rows(trade, 'SELECT * FROM guishi_item ORDER BY id'),
            probe.rows(trade, 'SELECT * FROM auction_player_upload ORDER BY user_id,item_id'),
            probe.rows(player, 'SELECT * FROM boss ORDER BY user_id'),
            probe.rows(player, 'SELECT * FROM statistics ORDER BY user_id'),
            probe.rows(game, 'SELECT * FROM sect_task_state ORDER BY user_id,period'),
            probe.rows(game, 'SELECT * FROM sect_task_claim_operations ORDER BY operation_id'))


async def read_sect(probe, state, actor, materials, level):
    before = snapshot(probe)
    reply = await probe.command('宗门信息', actor=actor)
    text = reply.replace('\r\n', '\n').replace('\r', '\n')
    name = state.module.config['宗门丹房参数']['elixir_room_level'][str(level)]['name'] if level else '暂无'
    assert f'宗门拥有资材\n> {materials}' in text and f'宗门丹房\n> {name}' in text, text
    assert snapshot(probe) == before
    return reply


async def message_contract(reply, token):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    event = SimpleNamespace(get_user_id=lambda: '900001')
    for onebot, markdown, send_fails in ((True, True, False), (False, False, False),
                                        (False, True, False), (False, True, True)):
        bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: 'OneBot V11' if onebot else 'QQ'))
        config = SimpleNamespace(markdown_status=markdown, markdown_id='', markdown_id2='', markdown_button_status=False,
                                 at_sender=False, message_optimization=False, img=False)
        plain, md = AsyncMock(), AsyncMock(side_effect=RuntimeError('isolated daily delivery failure') if send_fails else None)
        with patch.object(utils, 'XiuConfig', return_value=config), patch.object(utils, 'get_chat_scene', return_value='group'), \
                patch.object(utils, 'is_group_event', return_value=True), \
                patch.object(utils, 'get_real_id_async', new=AsyncMock(return_value='')), \
                patch.object(utils, '_send_event_message', plain), patch.object(utils, '_send_markdown_or_keyboard', md):
            await utils.handle_send(bot, event, reply, md_type='宗门', k1='宗门', v1='我的宗门')
        if onebot or not markdown or send_fails:
            plain.assert_awaited_once()
            assert plain.call_args.kwargs['message'] == reply
        else:
            md.assert_awaited_once()
            assert token in md.call_args.args[2]


def register(case):
    @case('sect-daily-maintenance-player-lifecycle', '宗门丹药领取', '已领/任务次数上限→实际重置→别名领取→重放/次日',
          '三个真实宗门分别扣资材/降级/失效，隔离三库与可控UTC时钟',
          '玩家读取真实维护结果，重置后领取一次，同日任务重放保留新状态，次日再次可领',
          group='sect-daily-maintenance')
    async def lifecycle(probe):
        with setup(probe) as state:
            reply = await probe.command('宗门丹药领取')
            assert '已经领取过' in reply, reply
            reply = await probe.command('宗门任务接取')
            assert '今日无法再获取宗门任务' in reply, reply
            before = snapshot(probe)
            assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
            after = snapshot(probe)
            assert after[2] == before[2] and after[4:] == before[4:]
            assert probe.rows(state.database, 'SELECT business_date FROM sect_daily_reset_operations') == [('2026-11-18',)]
            assert probe.rows(state.database, 'SELECT sect_task,sect_elixir_get FROM user_xiuxian ORDER BY user_id') == [(0, 0)]*3
            await read_sect(probe, state, '900001', 800, 1)
            await read_sect(probe, state, '900002', 500, 1)
            reply = await read_sect(probe, state, '900003', 100, 0)
            await message_contract(reply, '暂无')
            reply = await probe.command('领取宗门丹药')
            assert '成功领取' in reply, reply
            assert probe.managers[0].goods_num('900001', 1999) == 1
            assert probe.rows(state.database, "SELECT sect_elixir_get FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
            await message_contract(reply, '成功领取')
            reply = await probe.command('宗门任务接取')
            task = state.module.get_user_sect_task('900001')
            assert task and task['任务内容']['desc'] in reply, (task, reply)
            assert len(probe.rows(state.database, 'SELECT * FROM sect_task_claim_operations')) == 1
            for uid in ('900001', '900002', '900003'):
                probe.player(uid, sect_task=2)
            before = snapshot(probe)
            state.clock.moment = state.clock.moment.astimezone(ZoneInfo('Asia/Shanghai'))
            assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
            assert snapshot(probe) == before
            reply = await probe.command('宗门丹药领取')
            assert '已经领取过' in reply and probe.managers[0].goods_num('900001', 1999) == 1, reply
            reply = await probe.command('宗门丹药领取', actor='900003')
            assert '还未建设丹房' in reply, reply
            reply = await probe.command('宗门成员查看', '宗门成员查看 诊断道友 1')
            assert '诊断道友' in reply and '诊断对手' not in reply, reply
            state.clock.moment += timedelta(days=1)
            assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
            await read_sect(probe, state, '900001', 600, 1)
            reply = await probe.command('领取宗门丹药')
            assert '成功领取' in reply and probe.managers[0].goods_num('900001', 1999) == 2, reply
            assert len(probe.rows(state.database, 'SELECT * FROM sect_daily_reset_operations')) == 2
            assert len(probe.rows(state.database, 'SELECT * FROM sect_elixir_claim_operations')) == 2
            assert snapshot(probe)[8:] == before[8:]
            integrity(probe)
            probe.evidence.update(actual_scheduler=True, branches=['charged', 'downgraded', 'disabled'], alias_claim=True,
                                  task_unlocked_and_preserved=True, new_daily_usage_preserved=True,
                                  next_day_claim=True, adapter_contracts=8, integrity='ok')

    @case('sect-daily-maintenance-recovery', '宗门成员查看', '四步IGNORE/原ABORT→原状态读取→重建应用恢复→领取→重复任务',
          '真实迁移三库、四步trigger拒写和副作用、UTC业务时钟',
          '具体原因和主体到实际任务ERROR，全日回滚，故障解除恢复一次，玩家读取与领取一致',
          group='sect-daily-maintenance')
    async def recovery(probe):
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        with setup(probe) as state:
            for step, table, verb, condition in (
                ('user', 'user_xiuxian', 'UPDATE', "WHEN NEW.user_id='900002'"),
                ('material', 'sects', 'UPDATE', f'WHEN NEW.sect_id={state.sects[0]}'),
                ('level', 'sects', 'UPDATE', f'WHEN NEW.sect_id={state.sects[1]}'),
                ('receipt', 'sect_daily_reset_operations', 'INSERT', ''),
            ):
                probe.seed(state.database, f'CREATE TRIGGER refuse_daily BEFORE {verb} ON {table} {condition} '
                           "BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
                before = snapshot(probe)
                result = await fire(state.scheduled.daily_reset_sect_task)
                assert result.code == EVENT_JOB_ERROR and f'sect_daily_reset_{step}_write_rejected' in str(result.exception)
                subject = {'user': '900002', 'material': str(state.sects[0]), 'level': str(state.sects[1]), 'receipt': '2026-11-18'}[step]
                assert f'subject={subject}' in str(result.exception)
                assert snapshot(probe) == before
                await read_sect(probe, state, '900001', 1000, 1)
                await read_sect(probe, state, '900002', 500, 2)
                probe.seed(state.database, 'DROP TRIGGER refuse_daily')
            probe.seed(state.database, "CREATE TRIGGER refuse_daily BEFORE INSERT ON sect_daily_reset_operations "
                       "BEGIN SELECT RAISE(ABORT,'isolated daily command abort'); END")
            before = snapshot(probe)
            result = await fire(state.scheduled.daily_reset_sect_task)
            assert result.code == EVENT_JOB_ERROR and isinstance(result.exception, sqlite3.IntegrityError)
            assert 'isolated daily command abort' in str(result.exception) and snapshot(probe) == before
            probe.seed(state.database, 'DROP TRIGGER refuse_daily')
            with patch.object(state.module, 'sect_application', SectApplication(state.database)):
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                await read_sect(probe, state, '900001', 800, 1)
                await read_sect(probe, state, '900002', 500, 1)
                await read_sect(probe, state, '900003', 100, 0)
                reply = await probe.command('宗门丹药领取')
                assert '成功领取' in reply, reply
                probe.player(sect_task=3)
                before = snapshot(probe)
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                reply = await probe.command('宗门成员查看', '宗门成员查看 诊断道友 1')
                assert '诊断道友' in reply and snapshot(probe) == before, reply
                assert probe.managers[0].goods_num('900001', 1999) == 1
            integrity(probe)
            probe.evidence.update(rejected_steps=4, whole_day_rollback=True, original_abort=True,
                                  restarted_recovery=True, real_player_reads=True, replay_zero_write=True, integrity='ok')
