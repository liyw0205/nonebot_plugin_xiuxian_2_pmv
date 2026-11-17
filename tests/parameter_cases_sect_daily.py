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


async def task_command(probe, command, *, number):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.lay_out import _shutdown_cooldown_runtimes
    # Expire only the isolated probe timers; command guards and stamina still run.
    _shutdown_cooldown_runtimes()
    return await probe.command(command, number=number)


def register(case):
    @case('stamina-recovery-task-handoff', '宗门任务完成', '恢复拒写/篡改/ABORT→体力不足→后续恢复→完成任务',
          '隔离三库、已注册恢复回调、真实宗门命令与玩家投影',
          '恢复任务实际补充可用体力，任务消费只扣一次且结算状态一致',
          group='stamina-recovery')
    async def stamina_recovery_handoff(probe):
        from nonebot_plugin_xiuxian_2.features.base.stamina_application import PlayerStaminaApplication
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import lay_out, utils
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils as member
        with setup(probe) as state:
            module = state.module
            app = SectApplication(state.database, clock=state.clock)
            async def tick():
                with patch.object(lay_out, 'XiuConfig', return_value=SimpleNamespace(max_stamina=10, stamina_recovery_points=3)):
                    return await fire(lay_out.limit_all_stamina_)
            def assets():
                return (*snapshot(probe),
                        probe.rows(state.database, 'SELECT * FROM player_stamina_operations ORDER BY operation_id'),
                        probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations ORDER BY operation_id'))
            with patch.object(module, 'sect_application', app), patch.object(member, 'sect_application', app), \
                    patch.object(utils, '_player_stamina_application', PlayerStaminaApplication(state.database)):
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                probe.player(sect_task=0, user_stamina=2, stone=10**12, hp=100000, exp=100000)
                with patch.object(app.random, 'choice', return_value=next(key for key, value in module.config['宗门任务'].items() if value['type'] == 2)):
                    await task_command(probe, '宗门任务接取', number=984001)
                for action in ('ignore', 'rewrite', 'abort'):
                    timing = 'AFTER' if action == 'rewrite' else 'BEFORE'
                    body = ('SELECT RAISE(IGNORE);' if action == 'ignore' else
                            "SELECT RAISE(ABORT,'isolated recovery command fault');" if action == 'abort' else
                            "UPDATE user_xiuxian SET user_stamina=0 WHERE user_id='900001';")
                    probe.seed(state.database, f"CREATE TRIGGER refuse_recovery {timing} UPDATE OF user_stamina ON user_xiuxian "
                               "WHEN NEW.user_id='900001' BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; " + body + ' END')
                    before = assets()
                    failed = await tick()
                    assert failed.code == EVENT_JOB_ERROR and assets() == before
                    if action == 'abort':
                        assert isinstance(failed.exception, sqlite3.IntegrityError) and 'isolated recovery command fault' in str(failed.exception)
                    else:
                        assert failed.exception.details['reason'] == 'stamina_recovery_write_rejected'
                        assert failed.exception.details['subject'] == '900001'
                    reply = await task_command(probe, '宗门任务完成', number=984002)
                    assert '体力不足' in reply and assets() == before and app.get_active_task('900001'), reply
                    await message_contract(reply, '体力不足')
                    probe.seed(state.database, 'DROP TRIGGER refuse_recovery')
                with patch.object(utils, '_player_stamina_application', PlayerStaminaApplication(state.database)):
                    assert (await tick()).code == EVENT_JOB_EXECUTED
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(5,)]
                before = assets()
                reply = await task_command(probe, '宗门任务完成', number=984002)
                assert '任务完成' in reply and '消耗灵石' in reply, reply
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(2,)]
                assert len(probe.rows(state.database, 'SELECT * FROM player_stamina_operations')) == 1
                assert len(probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations')) == 1
                assert probe.rows(probe.context.paths.player_db, 'SELECT "宗门任务" FROM statistics WHERE user_id=\'900001\'') == [(1,)]
                assert assets()[2:12] == before[2:12]
                await message_contract(reply, '任务完成')
                assert (await tick()).code == EVENT_JOB_EXECUTED
                saved = assets()
                reply = await task_command(probe, '宗门任务完成', number=984002)
                assert '已完成' in reply and assets() == saved and probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(5,)]
                await message_contract(reply, '已完成')
                probe.seed(state.database, 'UPDATE user_xiuxian SET user_stamina=10')
                for uid in ('900001', '900002', '900003'):
                    probe.player(uid, user_stamina=0)
                probe.player(sect_task=0, hp=100000, stone=10**12)
                await task_command(probe, '宗门任务接取', number=984003)
                probe.seed(state.database, "CREATE TRIGGER refuse_recovery BEFORE UPDATE OF user_stamina ON user_xiuxian "
                           "WHEN NEW.user_id='900002' BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
                side_effect_balance = probe.rows(state.database, "SELECT stone FROM user_xiuxian WHERE user_id='900003'")
                with patch.dict('os.environ', {'XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE': '1'}):
                    failed = await tick()
                    assert failed.code == EVENT_JOB_ERROR and failed.exception.details['subject'] == '900002'
                    assert failed.exception.details['updated'] == 1
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id IN ('900001','900002','900003') ORDER BY user_id") == [(3,), (0,), (0,)]
                assert probe.rows(state.database, "SELECT stone FROM user_xiuxian WHERE user_id='900003'") == side_effect_balance
                reply = await task_command(probe, '宗门任务完成', number=984004)
                assert '任务完成' in reply and probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(0,)], reply
                probe.seed(state.database, 'DROP TRIGGER refuse_recovery')
                assert (await tick()).code == EVENT_JOB_EXECUTED
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id IN ('900001','900002','900003') ORDER BY user_id") == [(3,), (3,), (3,)]
                assert probe.rows(probe.context.paths.player_db, 'SELECT "宗门任务" FROM statistics WHERE user_id=\'900001\'') == [(2,)]
                saved = assets()
                reply = await task_command(probe, '宗门任务完成', number=984004)
                assert '已完成' in reply and assets() == saved
                probe.evidence.update(scheduled_recovery=True, stamina_before=2, recovered=3,
                                      task_consumed=3, refusal_then_same_message=True,
                                      old_message_preserves_new_stamina=True, earlier_batches_preserved=True,
                                      later_tick_is_new_recovery=True, adapter_contracts=20)
            integrity(probe)

    @case('sect-task-stamina-write-recovery', '宗门任务完成', '体力扣减/回执拒写→同消息恢复→历史重放',
          '实际宗门指令/DI与隔离三库，体力两步IGNORE/改写/末步篡改',
          '具体原因、体力含副作用全笔回滚且不开始任务结算，恢复只扣一次',
          group='sect-task-stamina')
    async def task_stamina(probe):
        from nonebot_plugin_xiuxian_2.features.base.stamina_application import PlayerStaminaApplication
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils as member
        with setup(probe) as state:
            module = state.module
            app = SectApplication(state.database, clock=state.clock)
            def assets():
                return (*snapshot(probe),
                        probe.rows(state.database, 'SELECT * FROM player_stamina_operations ORDER BY operation_id'),
                        probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations ORDER BY operation_id'))
            with patch.object(module, 'sect_application', app), patch.object(member, 'sect_application', app):
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                for index, (step, action) in enumerate((step, action) for step in ('user', 'receipt') for action in ('ignore', 'rewrite', 'late')):
                    number = 983000+index
                    probe.player(sect_task=0, user_stamina=10, hp=100000, exp=100000, stone=10**12)
                    reply = await task_command(probe, '宗门任务接取', number=number+100)
                    task = app.get_active_task('900001')
                    assert task and task['任务内容']['desc'] in reply, reply
                    table, verb, rewrite = (
                        ('user_xiuxian', 'UPDATE OF user_stamina', "UPDATE user_xiuxian SET user_stamina=0 WHERE user_id='900001';") if step == 'user' else
                        ('player_stamina_operations', 'INSERT', 'UPDATE player_stamina_operations SET stamina_after=99;')
                    )
                    if action == 'late':
                        table, verb = 'player_stamina_operations', 'INSERT'
                    timing = 'BEFORE' if action == 'ignore' else 'AFTER'
                    condition = "WHEN NEW.user_id='900001'" if table == 'user_xiuxian' else ''
                    body = 'SELECT RAISE(IGNORE);' if action == 'ignore' else rewrite
                    probe.seed(state.database, f'CREATE TRIGGER refuse_stamina {timing} {verb} ON {table} {condition} '
                               "BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; " + body + ' END')
                    before = assets()
                    reply = await task_command(probe, '宗门任务完成', number=number)
                    token = '体力扣减保存失败' if step == 'user' else '体力消耗回执保存失败'
                    assert token in reply and '已撤销' in reply and assets() == before, (step, action, reply)
                    await message_contract(reply, token)
                    probe.seed(state.database, 'DROP TRIGGER refuse_stamina')
                    with patch.object(utils, '_player_stamina_application', PlayerStaminaApplication(state.database)):
                        reply = await task_command(probe, '宗门任务完成', number=number)
                        assert '任务完成' in reply, reply
                        assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(7,)]
                        saved = assets()
                        reply = await task_command(probe, '宗门任务完成', number=number)
                        assert '已完成' in reply and assets() == saved, reply
                probe.player(sect_task=0, user_stamina=2, stone=10**12, hp=100000)
                await task_command(probe, '宗门任务接取', number=9831000)
                before = assets()
                reply = await task_command(probe, '宗门任务完成', number=9831001)
                assert '体力不足' in reply and assets() == before, reply
                from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import lay_out
                with patch.object(lay_out, 'XiuConfig', return_value=SimpleNamespace(max_stamina=10, stamina_recovery_points=3)):
                    assert (await fire(lay_out.limit_all_stamina_)).code == EVENT_JOB_EXECUTED
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(5,)]
                from tests.command_probe import run_case
                from tests.parameter_exception_watch import watch_handler_catches
                entry = next(row for row in probe.entries if row['command'] == '宗门任务完成')
                probe.seed(state.database, "CREATE TRIGGER refuse_stamina BEFORE INSERT ON player_stamina_operations BEGIN SELECT RAISE(ABORT,'isolated task stamina abort'); END")
                before = assets()
                from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.lay_out import _shutdown_cooldown_runtimes
                _shutdown_cooldown_runtimes()
                with patch.object(module, 'report_exception', return_value='任务失败（定位号：stamina-test）') as reported, watch_handler_catches(entry) as caught:
                    result = await run_case(entry, 'parameter', 9831001, input_text='宗门任务完成', actor='900001')
                assert result['status'] == 'pass' and len(caught) == 1 and assets() == before, (result, caught)
                assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
                assert 'sqlite3.IntegrityError: isolated task stamina abort' in caught[0]['traceback']
                assert '定位号：stamina-test' in ''.join(result['replies'])
                probe.calls.append({'command': '宗门任务完成', 'expected_injected_fault': caught, **result})
                probe.seed(state.database, 'DROP TRIGGER refuse_stamina')
                reply = await task_command(probe, '宗门任务完成', number=9831001)
                assert '任务完成' in reply and probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(2,)], reply
                assert len(probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations')) == 7
                assert probe.rows(probe.context.paths.player_db, 'SELECT "宗门任务" FROM statistics WHERE user_id=\'900001\'') == [(7,)]
                probe.player(sect_task=0, user_stamina=0, hp=100000, stone=10**12)
                with patch.object(module, 'ADMIN_IDS', {'900001'}), patch.object(lay_out, 'ADMIN_IDS', {'900001'}):
                    await task_command(probe, '宗门任务接取', number=9831002)
                    receipts = probe.rows(state.database, 'SELECT * FROM player_stamina_operations ORDER BY operation_id')
                    reply = await task_command(probe, '宗门任务完成', number=9831003)
                    assert '任务完成' in reply and probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(0,)], reply
                    assert probe.rows(state.database, 'SELECT * FROM player_stamina_operations ORDER BY operation_id') == receipts
            integrity(probe)
            probe.evidence.update(stamina_rejected_writes=6, rollback_before_task_settlement=True,
                                  restarted_same_message=True, actual_recovery_task=True,
                                  original_sqlite_error=True, admin_exemption_preserved=True,
                                  adapter_contracts=24, integrity='ok')

    @case('sect-task-daily-handoff', '宗门任务接取', '日重置→接取/刷新→灵石与气血完成→旧消息/次日',
          '实际每日任务、迁移三库与可控UTC业务时钟，真实随机任务配置',
          '任务状态/资产/次数/回执一致，历史消息不覆盖当前任务，次日旧消息零业务写',
          group='sect-task-daily')
    async def task_lifecycle(probe):
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils as member
        with setup(probe) as state:
            module = state.module
            app = SectApplication(state.database, clock=state.clock)
            with patch.object(module, 'sect_application', app), patch.object(member, 'sect_application', app):
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                keys = {kind: next(key for key, value in module.config['宗门任务'].items() if value['type'] == kind) for kind in (1, 2)}
                probe.player(exp=100000, hp=100000, stone=10**12)
                with patch.object(app.random, 'choice', return_value=keys[2]):
                    reply = await task_command(probe, '宗门任务接取', number=982001)
                task = app.get_active_task('900001')
                assert task['任务名称'] == keys[2] and task['period'] == '2026-11-18' and task['任务内容']['desc'] in reply, (task, reply)
                with patch.object(app.random, 'choice', return_value=keys[1]):
                    reply = await task_command(probe, '宗门任务刷新', number=982002)
                assert '已刷新' in reply and app.get_active_task('900001')['任务名称'] == keys[1], reply
                saved = snapshot(probe)
                reply = await task_command(probe, '宗门任务刷新', number=982002)
                assert '已完成' in reply and snapshot(probe) == saved, reply
                reply = await task_command(probe, '我的宗门任务', number=982001)
                assert '已完成' in reply and app.get_active_task('900001')['任务名称'] == keys[1] and snapshot(probe) == saved
                before = probe.rows(state.database, "SELECT hp,stone,exp,sect_task,sect_contribution FROM user_xiuxian WHERE user_id='900001'")[0]
                reply = await task_command(probe, '宗门任务完成', number=982003)
                assert '任务完成' in reply and '气血减少' in reply, reply
                first = probe.rows(state.database, 'SELECT cost_type,cost,exp_reward,sect_reward FROM sect_task_settlement_operations')[0]
                after = probe.rows(state.database, "SELECT hp,stone,exp,sect_task,sect_contribution FROM user_xiuxian WHERE user_id='900001'")[0]
                assert first[0] == 'hp' and tuple(map(int, after)) == (int(before[0])-first[1], int(before[1]), int(before[2])+first[2], 1, int(before[4])+first[3])
                assert app.get_active_task('900001') is None
                await message_contract(reply, '任务完成')
                with patch.object(app.random, 'choice', return_value=keys[2]):
                    await task_command(probe, '宗门任务接取', number=982004)
                saved = snapshot(probe)
                reply = await task_command(probe, '宗门任务完成', number=982003)
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(97,)], 'replay deducted stamina again'
                assert '已完成' in reply and snapshot(probe) == saved and app.get_active_task('900001')['任务名称'] == keys[2], reply
                reply = await task_command(probe, '宗门任务完成', number=982005)
                assert '任务完成' in reply and '消耗灵石' in reply, reply
                assert probe.rows(state.database, "SELECT sect_task FROM user_xiuxian WHERE user_id='900001'") == [(2,)]
                assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(94,)]
                assert probe.rows(probe.context.paths.player_db, 'SELECT "宗门任务" FROM statistics WHERE user_id=\'900001\'') == [(2,)]
                saved = snapshot(probe)
                settled = probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations ORDER BY operation_id')
                state.clock.moment += timedelta(days=1)
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                for command, number in (('宗门任务接取', 982001), ('宗门任务刷新', 982002), ('宗门任务完成', 982005)):
                    before = snapshot(probe)
                    reply = await task_command(probe, command, number=number)
                    assert '已完成' in reply and snapshot(probe) == before, reply
                probe.player(sect_id=None)
                before = snapshot(probe)
                reply = await task_command(probe, '宗门任务完成', number=982003)
                assert '已完成' in reply and snapshot(probe) == before, reply
                probe.player(sect_id=state.sects[0])
                assert probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations ORDER BY operation_id') == settled
                assert probe.rows(state.database, "SELECT sect_task FROM user_xiuxian WHERE user_id='900001'") == [(0,)]
                await message_contract(reply, '已完成')
                with patch.object(app.random, 'choice', return_value=keys[2]):
                    await task_command(probe, '宗门任务接取', number=982006)
                assert app.get_active_task('900001')['period'] == '2026-11-19'
                assert snapshot(probe)[8:13] == saved[8:13]
                assert len(probe.rows(state.database, 'SELECT * FROM sect_task_claim_operations')) == 4
                probe.player(stone=0)
                before = snapshot(probe)
                reply = await task_command(probe, '宗门任务完成', number=982007)
                assert '灵石带少' in reply and snapshot(probe)[1:] == before[1:] and app.get_active_task('900001'), reply
                with patch.object(app.random, 'choice', return_value=keys[1]):
                    await task_command(probe, '宗门任务刷新', number=982008)
                probe.player(hp=1)
                reply = await task_command(probe, '宗门任务完成', number=982009)
                assert '状态欠佳' in reply and app.get_active_task('900001'), reply
                assert probe.rows(state.database, "SELECT sect_task FROM user_xiuxian WHERE user_id='900001'") == [(0,)]
                probe.player(sect_task=module.config['每日宗门任务次上限'])
                before = snapshot(probe)
                reply = await task_command(probe, '宗门任务接取', number=982010)
                assert '今日无法再获取' in reply and snapshot(probe) == before, reply
                reply = await task_command(probe, '宗门任务刷新', number=982011)
                assert '次数' in reply and snapshot(probe) == before, reply
            integrity(probe)
            probe.evidence.update(actual_daily_reset=True, claim_refresh_complete=True, both_costs=True,
                                  current_task_preserved_on_replay=True, next_day_zero_write=True, adapter_contracts=8, integrity='ok')

    @case('sect-task-write-recovery', '宗门任务接取', '接取/刷新两步与完成四步拒写→重建应用同消息恢复→ABORT',
          '实际handler/DI、SQLite触发器副作用、可控业务时钟与隔离三库',
          '每种具体原因回复，失败整笔回滚；故障解除同消息只成功一次，统计与资产一致',
          group='sect-task-daily')
    async def task_recovery(probe):
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect import sect_member_utils as member
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        with setup(probe) as state:
            module = state.module
            app = SectApplication(state.database, clock=state.clock)
            with patch.object(module, 'sect_application', app), patch.object(member, 'sect_application', app):
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                probe.player(exp=100000, hp=100000, stone=10**12)
                for mode, command, steps, number in (
                    ('claim', '宗门任务接取', [('state', 'sect_task_state', 'INSERT', '接取任务状态'), ('receipt', 'sect_task_claim_operations', 'INSERT', '接取任务回执')], 982020),
                    ('refresh', '宗门任务刷新', [('state', 'sect_task_state', 'UPDATE', '刷新任务状态'), ('receipt', 'sect_task_claim_operations', 'INSERT', '刷新任务回执')], 982021),
                    ('settlement', '宗门任务完成', [('user', 'user_xiuxian', 'UPDATE', '任务消耗'), ('sect', 'sects', 'UPDATE', '任务宗门奖励'), ('state', 'sect_task_state', 'UPDATE', '任务完成状态'), ('receipt', 'sect_task_settlement_operations', 'INSERT', '任务结算回执')], 982022),
                ):
                    stamina_before = probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'")[0][0]
                    for step, table, verb, token in steps:
                        condition = "WHEN NEW.user_id='900001'" if table == 'user_xiuxian' else ''
                        if table == 'user_xiuxian':
                            verb = 'UPDATE OF sect_task'
                        probe.seed(state.database, f'CREATE TRIGGER refuse_task BEFORE {verb} ON {table} {condition} '
                                   "BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
                        before = snapshot(probe)
                        reply = await task_command(probe, command, number=number)
                        after = snapshot(probe)
                        if mode == 'settlement' and step == 'user':
                            columns = [row[1] for row in probe.rows(state.database, 'PRAGMA table_info(user_xiuxian)')]
                            stamina_index, user_index = columns.index('user_stamina'), columns.index('user_id')
                            expected_users = [tuple(value-3 if i == stamina_index and row[user_index] == '900001' else value for i, value in enumerate(row)) for row in before[0]]
                            before = (expected_users, *before[1:])
                        assert token in reply and '已撤销' in reply and after == before, (mode, step, reply)
                        await message_contract(reply, token)
                        probe.seed(state.database, 'DROP TRIGGER refuse_task')
                    restarted = SectApplication(state.database, clock=state.clock)
                    with patch.object(module, 'sect_application', restarted), patch.object(member, 'sect_application', restarted):
                        reply = await task_command(probe, command, number=number)
                        if mode == 'claim':
                            assert '已撤销' not in reply and restarted.get_active_task('900001'), reply
                        else:
                            assert ('已刷新' if mode == 'refresh' else '任务完成') in reply, reply
                        before = snapshot(probe)
                        reply = await task_command(probe, command, number=number)
                        assert '已完成' in reply and snapshot(probe) == before, reply
                    assert probe.rows(state.database, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(stamina_before-(3 if mode == 'settlement' else 0),)]
                assert probe.rows(probe.context.paths.player_db, 'SELECT "宗门任务" FROM statistics WHERE user_id=\'900001\'') == [(1,)]
                before = snapshot(probe)
                probe.seed(state.database, "CREATE TRIGGER refuse_task BEFORE INSERT ON sect_task_claim_operations BEGIN SELECT RAISE(ABORT,'isolated task command fault'); END")
                entry = next(row for row in probe.entries if row['command'] == '宗门任务接取')
                with patch.object(module, 'report_exception', return_value='任务失败（定位号：task-test）') as reported, watch_handler_catches(entry) as caught:
                    result = await run_case(entry, 'parameter', 982023, input_text='宗门任务接取', actor='900001')
                assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
                assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
                assert 'sqlite3.IntegrityError: isolated task command fault' in caught[0]['traceback']
                assert '定位号：task-test' in ''.join(result['replies']) and snapshot(probe) == before
                probe.calls.append({'command': '宗门任务接取', 'expected_injected_fault': caught, **result})
                probe.seed(state.database, 'DROP TRIGGER refuse_task')
                await probe.command('宗门任务接取', number=982023)
                assert len(probe.rows(state.database, 'SELECT * FROM sect_task_settlement_operations')) == 1
                assert probe.rows(state.database, "SELECT sect_task FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
            integrity(probe)
            probe.evidence.update(rejected_steps=8, rollback=True, restarted_same_message=True,
                                  original_sqlite_error=True, adapter_contracts=32, integrity='ok')

    @case('sect-elixir-claim-recovery', '宗门丹药领取', '日重置→三步拒写/原ABORT→同消息恢复→别名回放→次日旧消息',
          '隔离三库、真实每日任务、背包/次数/回执trigger与副作用',
          '具体原因回复，失败全笔回滚，重建应用后恢复一次，次日旧消息不发药也不占新次数',
          group='sect-elixir-claim')
    async def claim_recovery(probe):
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        with setup(probe) as state:
            assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
            def assets():
                rows = snapshot(probe)
                return rows[:2] + rows[3:7] + rows[8:]
            for step, table, verb, condition, text in (
                ('item', 'back', 'INSERT', '', '丹药入库保存失败'),
                ('user', 'user_xiuxian', 'UPDATE', "WHEN NEW.user_id='900001' AND NEW.sect_elixir_get=1", '已领标志保存失败'),
                ('receipt', 'sect_elixir_claim_operations', 'INSERT', '', '领取回执保存失败'),
            ):
                probe.seed(state.database, f'CREATE TRIGGER refuse_claim BEFORE {verb} ON {table} {condition} '
                           "BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
                before = assets()
                reply = await probe.command('宗门丹药领取', number=981001)
                assert text in reply and '已撤销' in reply and '成功领取' not in reply, reply
                assert assets() == before
                ledger = probe.rows(state.database, "SELECT status,result_json FROM operation_ledger WHERE action='sect.claim_elixir'")
                assert len(ledger) == 1 and ledger[0][0] == 'failed' and f'sect_elixir_{step}_write_rejected' in ledger[0][1]
                await message_contract(reply, text)
                probe.seed(state.database, 'DROP TRIGGER refuse_claim')
            probe.seed(state.database, "CREATE TRIGGER refuse_claim BEFORE INSERT ON sect_elixir_claim_operations "
                       "BEGIN SELECT RAISE(ABORT,'isolated elixir command fault'); END")
            entry = next(row for row in probe.entries if row['command'] == '宗门丹药领取')
            before = assets()
            with patch.object(state.module, 'report_exception', return_value='领取失败（定位号：elixir-test）') as reported, \
                    watch_handler_catches(entry) as caught:
                result = await run_case(entry, 'parameter', 981001, input_text='宗门丹药领取', actor='900001')
            assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
            assert 'sqlite3.IntegrityError: isolated elixir command fault' in caught[0]['traceback']
            assert isinstance(reported.call_args.args[1], sqlite3.IntegrityError)
            assert '定位号：elixir-test' in ''.join(result['replies']) and assets() == before
            probe.calls.append({'command': '宗门丹药领取', 'expected_injected_fault': caught, **result})
            probe.seed(state.database, 'DROP TRIGGER refuse_claim')
            with patch.object(state.module, 'sect_application', SectApplication(state.database)):
                reply = await probe.command('领取宗门丹药', number=981001)
                assert '成功领取' in reply and probe.managers[0].goods_num('900001', 1999) == 1, reply
                saved = snapshot(probe)
                reply = await probe.command('宗门丹药领取', number=981001)
                assert '已完成' in reply and snapshot(probe) == saved, reply
                state.clock.moment += timedelta(days=1)
                assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
                saved = snapshot(probe)
                reply = await probe.command('领取宗门丹药', number=981001)
                assert '已完成' in reply and snapshot(probe) == saved, reply
                reply = await probe.command('领取宗门丹药', number=981002)
                assert '成功领取' in reply and probe.managers[0].goods_num('900001', 1999) == 2, reply
                assert probe.rows(state.database, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=1999") == [(2, 2)]
                assert len(probe.rows(state.database, 'SELECT * FROM sect_elixir_claim_operations')) == 2
            integrity(probe)
            probe.evidence.update(actual_daily_reset=True, rejected_steps=3, rollback=True, restarted_same_message=True,
                                  alias_replay=True, next_day_old_message_zero_write=True, adapter_contracts=12, integrity='ok')

    @case('sect-elixir-multiple-rewards', '领取宗门丹药', '高阶丹房随机多药→回执拒写→重新生成→同消息恢复',
          '真实配置二级丹房，固定两次不同随机结果，隔离迁移三库',
          '服务端重新生成结果不当玩家改参，整批绑定数量一致；已领/无房/职位/贡献/资材/满包具体拒绝',
          group='sect-elixir-claim')
    async def multiple_rewards(probe):
        with setup(probe) as state:
            sid = state.sects[0]
            probe.seed(state.database, 'UPDATE sects SET elixir_room_level=2,sect_materials=2000 WHERE sect_id=?', (sid,))
            state.module.config['宗门丹房参数']['elixir_room_level']['2']['give_level']['give_num'] = 3
            assert (await fire(state.scheduled.daily_reset_sect_task)).code == EVENT_JOB_EXECUTED
            probe.seed(state.database, "CREATE TRIGGER refuse_claim BEFORE INSERT ON sect_elixir_claim_operations BEGIN SELECT RAISE(IGNORE); END")
            before = snapshot(probe)
            with patch.object(state.module.items, 'get_random_id_list_by_rank_and_item_type', return_value=[2001, 2002]), \
                    patch.object(state.module.runtime_random, 'choice', return_value=2001):
                reply = await probe.command('领取宗门丹药', number=981010)
            assert '回执保存失败' in reply and snapshot(probe)[5] == before[5], reply
            probe.seed(state.database, 'DROP TRIGGER refuse_claim')
            with patch.object(state.module.items, 'get_random_id_list_by_rank_and_item_type', return_value=[2001, 2002]), \
                    patch.object(state.module.runtime_random, 'choice', return_value=2002):
                reply = await probe.command('宗门丹药领取', number=981010)
            assert '成功领取' in reply, reply
            assert probe.rows(state.database, "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id") == [(1999, 1, 1), (2002, 2, 2)]
            saved = snapshot(probe)
            reply = await probe.command('领取宗门丹药', number=981010)
            assert '已完成' in reply and snapshot(probe) == saved
            await message_contract(reply, '已完成')
            original_check = state.module.check_user
            def stale_player(event):
                valid, player, message = original_check(event)
                return valid, {**player, 'sect_elixir_get': 0}, message
            with patch.object(state.module.sect_application, 'elixir_claim_receipt', return_value=None), \
                    patch.object(state.module, 'check_user', side_effect=stale_player), \
                    patch.object(state.module.items, 'get_random_id_list_by_rank_and_item_type', return_value=[2001, 2002]), \
                    patch.object(state.module.runtime_random, 'choice', return_value=2001):
                reply = await probe.command('领取宗门丹药', number=981010)
            assert '已完成' in reply and state.module.items.get_data_by_item_id(2002)['name'] in reply, reply
            assert state.module.items.get_data_by_item_id(2001)['name'] not in reply, reply
            assert snapshot(probe)[3:] == saved[3:] and snapshot(probe)[:2] == saved[:2]
            reply = await probe.command('领取宗门丹药', number=981011)
            assert '已经领取过' in reply
            reply = await probe.command('领取宗门丹药', actor='900003')
            assert '还未建设丹房' in reply
            for values, token in (({'sect_position': 15}, '不满足领取要求'),
                                  ({'sect_position': 0, 'sect_contribution': 0}, '贡献度不满足')):
                probe.player('900002', **values)
                reply = await probe.command('宗门丹药领取', actor='900002')
                assert token in reply, reply
            probe.player('900002', sect_contribution=10**12)
            probe.seed(state.database, 'UPDATE sects SET sect_materials=0 WHERE sect_id=?', (state.sects[1],))
            reply = await probe.command('领取宗门丹药', actor='900002')
            assert '资材无法维护' in reply, reply
            probe.seed(state.database, 'UPDATE sects SET sect_materials=500 WHERE sect_id=?', (state.sects[1],))
            with patch.object(state.module, 'XiuConfig', return_value=SimpleNamespace(max_goods_num=0)):
                reply = await probe.command('领取宗门丹药', actor='900002')
            assert '背包已满' in reply, reply
            assert snapshot(probe)[4:7] == saved[4:7] and snapshot(probe)[8:] == saved[8:]
            integrity(probe)
            probe.evidence.update(multiple_items=True, bound_counts=True, regenerated_rewards_recovered=True,
                                  specific_eligibility=True, alias_replay=True, adapter_contracts=4, integrity='ok')

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
