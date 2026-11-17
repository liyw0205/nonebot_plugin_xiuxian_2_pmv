"""Scheduled refresh through live player commands and durable battle settlement."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_auction import fire
from tests.parameter_cases_market import FixedClock
from tests.parameter_cases_guishi_return import integrity


@contextmanager
def setup(probe):
    from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.old_boss_info import OLD_BOSS_INFO
    clock = FixedClock(datetime(2026, 11, 17, 10, tzinfo=timezone.utc))
    config = dict(module.get_boss_config(), Boss生成时间参数={'hours': '1', 'minutes': '0'})
    app = BossApplication(probe.context.paths.game_db, probe.context.paths.player_db,
                          activity_database=probe.context.paths.game_db, full_refresh_config_loader=lambda: config, clock=clock)
    store = OLD_BOSS_INFO(probe.context.paths.data / 'probe-boss-info.json')
    store.read_boss_info()
    with patch.object(module, 'boss_application', app), patch.object(module, 'config', config), \
            patch.object(module, 'get_boss_config', return_value=config), patch.object(module, 'runtime_clock', clock), \
            patch.object(module, '_world_boss_full_refresh_service_instance', None), \
            patch.object(module, 'old_boss_info', store), patch.object(module, 'group_boss', {}), \
            patch.object(module, 'battle_flag', {}), patch.object(module, 'groups', []):
        yield SimpleNamespace(module=module, app=app, clock=clock, config=config, store=store)


def assets(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return (probe.rows(game, 'SELECT user_id,stone,exp,hp,mp,user_stamina FROM user_xiuxian ORDER BY user_id'),
            probe.rows(game, 'SELECT * FROM user_cd ORDER BY user_id'),
            probe.rows(game, 'SELECT * FROM back ORDER BY user_id,goods_id'),
            probe.rows(game, 'SELECT * FROM world_boss_battle_operations ORDER BY operation_id'),
            probe.rows(game, 'SELECT * FROM domain_outbox ORDER BY event_id'),
            probe.rows(player, 'SELECT * FROM world_boss_state'),
            probe.rows(player, 'SELECT * FROM world_boss_full_refresh_operations ORDER BY operation_id'),
            probe.rows(player, 'SELECT * FROM boss ORDER BY user_id'),
            probe.rows(player, 'SELECT * FROM boss_limit ORDER BY user_id'),
            probe.rows(player, 'SELECT * FROM xiuxian_tasks ORDER BY user_id'),
            probe.rows(player, 'SELECT * FROM statistics ORDER BY user_id'))


def register(case):
    @case('world-boss-refresh-player-lifecycle', '世界BOSS讨伐', '刷新→查询/分页/编号讨伐→重放→下一时间槽',
          '真实启动迁移库及随包生成配置；战斗伤害固定，发送mock',
          '任务刷新与玩家读取同一持久化状态，讨伐扣血/体力/奖励/统计一次，重复刷新不复活BOSS',
          group='world-boss-refresh')
    async def player_lifecycle(probe):
        from apscheduler.events import EVENT_JOB_EXECUTED
        with setup(probe) as state:
            module, app = state.module, state.app
            probe.player(level='练气境中期', exp=1000, hp=1000, mp=1000, user_stamina=100)
            probe.player('900003', level='永恒境中期', exp=10**12)
            assert (await fire(module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            bosses, revision = app.snapshot()
            assert bosses and revision == 1
            index = next(i for i, row in enumerate(bosses, 1) if row['jj'] == '练气境')
            target = bosses[index - 1]
            before = assets(probe)
            query = await probe.command('查询世界BOSS', '查询世界BOSS  ')
            assert target['name'] in query, query
            with patch.object(module, 'get_msg_pic', new=AsyncMock(return_value='base64://aW1hZ2U=')) as render:
                await probe.command('世界BOSS查询', f'世界BOSS查询 {index}')
                assert target['name'] in render.call_args.args[0]
            listing = await probe.command('世界BOSS列表', '世界BOSS列表 1')
            assert '第1' in listing and '编号1' in listing, listing
            pages = (len(bosses) + 4) // 5
            for text, expected in ((f'世界boss列表 {pages}', f'第{pages}'), ('世界BOSS列表 0', '页码错误'),
                                   (f'世界BOSS列表 {pages+1}', '页码错误')):
                reply = await probe.command('世界BOSS列表', text)
                assert expected in reply, reply
            assert assets(probe) == before
            async def fight(uid, boss, **kwargs):
                assert boss == app.snapshot()[0][index-1]
                nodes = [{'type': 'node', 'data': {'name': 'battle', 'uin': uid, 'content': 'isolated battle'}}]
                return nodes, 'Boss赢了', deepcopy(boss), [
                    {'player': {'team_id': 0, 'user_id': uid, 'total_dmg': 100,
                                'hp': 900, 'mp': 900}}]
            with patch.object(module, 'Boss_fight', side_effect=fight) as battle, \
                    patch.object(module, 'boss_drops', return_value=(None, None)):
                reply = await probe.command('世界BOSS讨伐', f'讨伐世界BOSS {index}', number=981000)
                assert '实际削减世界BOSS血量：100' in reply, reply
                battle.assert_awaited_once()
                damaged, after_revision = app.snapshot()
                assert damaged[index-1]['气血'] == target['气血'] - 100
                assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(100-int(state.config['讨伐世界Boss体力消耗']),)]
                assert probe.rows(probe.context.paths.player_db, 'SELECT "讨伐世界BOSS" FROM statistics WHERE user_id=\'900001\'') == [(1,)]
                assert probe.rows(probe.context.paths.game_db, 'SELECT COUNT(*) FROM world_boss_battle_operations') == [(1,)]
                saved = assets(probe)
                probe.clear_cooldowns()
                reply = await probe.command('世界BOSS讨伐', f'世界BOSS讨伐 {index}', number=981000)
                assert '已经处理' in reply and assets(probe) == saved, reply
                assert (await fire(module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
                assert assets(probe) == saved
                assert module.group_boss['global'] == state.store.data['global'] == damaged
                battle.assert_awaited_once()
                for number, raw_time in ((981001, None), (981002, '2026-11-17 17:59:00'), (981003, '')):
                    if raw_time is not None:
                        probe.seed(probe.context.paths.game_db, "UPDATE user_cd SET last_check_info_time=? WHERE user_id='900001'", (raw_time,))
                    state.clock.moment += timedelta(minutes=1)
                    probe.clear_cooldowns()
                    reply = await probe.command('世界BOSS讨伐', f'讨伐世界BOSS {index}', number=number)
                    assert '实际削减世界BOSS血量：100' in reply, reply
                assert app.snapshot()[0][index-1]['气血'] == target['气血'] - 400
                assert probe.rows(probe.context.paths.player_db, 'SELECT "讨伐世界BOSS" FROM statistics WHERE user_id=\'900001\'') == [(4,)]
            state.clock.moment += timedelta(hours=1)
            assert (await fire(module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            refreshed, new_revision = app.snapshot()
            assert new_revision == after_revision + 1 and all(row['气血'] == row['总血量'] for row in refreshed)
            assert probe.rows(probe.context.paths.player_db, 'SELECT COUNT(*) FROM world_boss_full_refresh_operations') == [(2,)]
            integrity(probe)
            probe.evidence.update(boss_count=len(bosses), battle_index=index, damage_per_attack=100, attacks=4,
                                  cooldown_forms=['null', 'aware', 'legacy_local', 'empty'],
                                  replay_zero_writes=True, next_slot_refreshed=True, integrity='ok')

    @case('world-boss-refresh-write-recovery', '世界BOSS全部生成', '管理员刷新/两步拒写/恢复/别名重放/多余参数',
          '真实SQLite状态/回执IGNORE及副作用；重建应用同消息恢复',
          '具体原因和主体回复、拒绝全回滚、成功和重复缓存一致', group='world-boss-refresh')
    async def write_recovery(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
        from tests.command_probe import run_case
        with setup(probe) as state:
            module, database = state.module, probe.context.paths.player_db
            entry = next(row for row in probe.entries if row['command'] == '世界BOSS全部生成')
            before = assets(probe)
            denied = await run_case(entry, 'parameter', 981009, input_text='世界BOSS全部生成', actor='900001')
            assert denied['status'] == 'blocked' and denied['reason'] == 'permission/rule rejected', denied
            assert not denied['replies'] and not denied['errors'] and assets(probe) == before
            probe.calls.append({'command': '世界BOSS全部生成', 'expected_permission_rejection': True, **denied})
            for target, table, event in (('state', 'world_boss_state', 'UPDATE'),
                                         ('receipt', 'world_boss_full_refresh_operations', 'INSERT')):
                probe.seed(database, f'CREATE TRIGGER refuse_refresh BEFORE {event} ON {table} BEGIN '
                           "INSERT INTO boss(user_id,boss_integral) VALUES('side-effect',7); SELECT RAISE(IGNORE); END")
                before = assets(probe)
                outcome = await fire(module.generate_all_bosses_task)
                assert outcome.code == EVENT_JOB_ERROR
                reply = await probe.command('世界BOSS全部生成', '世界BOSS全部生成', actor='900003', number=981010)
                assert f'world_boss_refresh_{target}_write_rejected' in reply and '主体：' in reply, reply
                assert '已生成' not in reply and assets(probe) == before
                assert module.group_boss == {} and state.store.data['global'] == []
                probe.seed(database, 'DROP TRIGGER refuse_refresh')
            restarted = BossApplication(probe.context.paths.game_db, database, full_refresh_config_loader=lambda: state.config)
            with patch.object(module, 'boss_application', restarted):
                reply = await probe.command('世界BOSS全部生成', '生成全部世界BOSS', actor='900003', number=981010)
                assert '已生成全部' in reply, reply
                saved = assets(probe)
                await probe.command('世界BOSS全部生成', '世界BOSS全部生成', actor='900003', number=981010)
                assert assets(probe) == saved
                reply = await probe.command('世界BOSS全部生成', '世界BOSS全部生成 extra', actor='900003')
                assert '无需参数' in reply and assets(probe) == saved
                assert module.group_boss['global'] == state.store.data['global'] == restarted.snapshot()[0]
                assert (await fire(module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            integrity(probe)
            probe.evidence.update(reasons=['state_write_rejected', 'receipt_write_rejected'],
                                  permission_zero_write=True, retry_same_message=True, replay_zero_writes=True, integrity='ok')

    @case('world-boss-player-parameter-rejection', '世界BOSS查询', '非法/负数/多余参数/无编号',
          '已实际刷新BOSS及有效玩家，排除无BOSS遮挡',
          '明确拒绝参数，玩家资产/场次/战斗回执/统计不变',
          group='world-boss-refresh', outcome='rejection')
    async def parameters(probe):
        from apscheduler.events import EVENT_JOB_EXECUTED
        with setup(probe) as state:
            assert (await fire(state.module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            probe.player(level='练气境中期', exp=1000, hp=1000)
            # The legacy eligibility check initializes this projection for a new player.
            probe.managers[0].get_user_cd('900001')
            state.module.boss_application.daily_limit_snapshot('900001')
            state.module.boss_integral_application.get_integral('900001')
            before = assets(probe)
            for command in ('世界BOSS查询', '世界BOSS列表', '世界BOSS讨伐'):
                for value in ('-1', '1 extra', 'abc', '0', '9' * 5000):
                    probe.clear_cooldowns()
                    reply = await probe.command(command, f'{command} {value}')
                    assert '编号' in reply or '页码错误' in reply, reply
                    assert '请输入' in reply or '错误' in reply, reply
                    after = assets(probe)
                    assert after == before, [(i, previous, current) for i, (previous, current) in enumerate(zip(before, after)) if previous != current]
            probe.clear_cooldowns()
            reply = await probe.command('世界BOSS讨伐')
            assert '请输入正确' in reply and assets(probe) == before
            probe.evidence.update(invalid_parameter_forms=16, assets_unchanged=True, integrity='ok')
            integrity(probe)
