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


def manual_assets(probe):
    return assets(probe) + (
        probe.rows(probe.context.paths.player_db,
                   'SELECT * FROM world_boss_manual_spawn_operations ORDER BY operation_id'),
        probe.rows(probe.context.paths.trade_db, 'SELECT * FROM auction_player_upload ORDER BY user_id,item_id'),
    )


def register(case):
    @case('world-boss-manual-session-lifecycle', '世界BOSS指定生成', '定时刷新→指定/随机替换→玩家查询→下一槽→旧消息回放',
          '真实迁移库/生成配置/业务时钟；管理员和玩家真实matcher',
          '只替换选定境界、其他资产不变，旧回放不恢复历史场次，缓存与当前状态一致',
          group='world-boss-manual-spawn')
    async def manual_lifecycle(probe):
        from apscheduler.events import EVENT_JOB_EXECUTED
        with setup(probe) as state:
            module, app = state.module, state.app
            probe.player(level='练气境中期', exp=1000)
            probe.player('900003', level='永恒境中期', exp=10**12)
            assert (await fire(module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            original, revision = app.snapshot()
            before = manual_assets(probe)
            realm, name = '练气境', state.config['Boss名字'][0]
            command = f'指定生成世界boss  {realm}  {name}  '
            reply = await probe.command('世界BOSS指定生成', command, actor='900003', number=983000)
            assert f'已生成{realm}Boss:{name}' in reply, reply
            changed, new_revision = app.snapshot()
            assert new_revision == revision + 1
            assert [row for row in original if row['jj'] != realm] == [row for row in changed if row['jj'] != realm]
            target = next(row for row in changed if row['jj'] == realm)
            assert target['name'] == name and len(changed) == len(original)
            after = manual_assets(probe)
            assert after[:5] == before[:5] and after[6:11] == before[6:11] and after[12:] == before[12:]
            with patch.object(module, 'get_msg_pic', new=AsyncMock(return_value='base64://aW1hZ2U=')) as render:
                await probe.command('查询世界BOSS', f'查询世界BOSS {len(changed)}')
                assert name in render.call_args.args[0]
            reply = await probe.command('世界BOSS列表', f'世界BOSS列表 {(len(changed)+4)//5}')
            assert name in reply, reply
            assert manual_assets(probe) == after
            reply = await probe.command('世界BOSS指定生成', f'世界BOSS指定生成 {realm}', actor='900003', number=983001)
            assert f'已生成{realm}Boss:' in reply, reply
            with patch.object(module, 'createboss', return_value=realm):
                reply = await probe.command('世界BOSS生成', '生成世界boss  ', actor='900003', number=983002)
            assert f'已生成{realm}Boss:' in reply, reply
            state.clock.moment += timedelta(hours=1)
            assert (await fire(module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            current = app.snapshot()[0]
            saved = manual_assets(probe)
            with patch.object(module, 'createboss_jj') as generate:
                for entry, text, number in [('世界BOSS指定生成', command, 983000),
                                            ('世界BOSS指定生成', f'世界BOSS指定生成 {realm}', 983001),
                                            ('世界BOSS生成', '世界BOSS生成', 983002)]:
                    reply = await probe.command(entry, text, actor='900003', number=number)
                    assert '该请求此前已生成' in reply and manual_assets(probe) == saved, reply
                    assert module.group_boss['global'] == state.store.data['global'] == current
                generate.assert_not_called()
            assert probe.rows(probe.context.paths.player_db, 'SELECT COUNT(*) FROM world_boss_manual_spawn_operations') == [(3,)]
            integrity(probe)
            probe.evidence.update(single_realm_replaced=True, scheduled_handoff=True, historical_replay_zero_writes=True,
                                  unrelated_assets_unchanged=True, integrity='ok')

    @case('world-boss-manual-parameters', '世界BOSS指定生成', '权限/缺参/多参/非法境界名称/同消息参数冲突',
          '真实管理员/玩家dispatch，当前合法配置；非法参数不调用生成器',
          '拒绝保留原因且零写，随机无参数、指定1至2参数，消息内容改变不可回放',
          group='world-boss-manual-spawn')
    async def manual_parameters(probe):
        from tests.command_probe import run_case
        with setup(probe) as state:
            before = manual_assets(probe)
            for command in ('世界BOSS生成', '世界BOSS指定生成'):
                entry = next(row for row in probe.entries if row['command'] == command)
                result = await run_case(entry, 'parameter', 983010, input_text=command, actor='900001')
                assert result['status'] == 'blocked' and result['reason'] == 'permission/rule rejected', result
                assert not result['replies'] and not result['errors'] and manual_assets(probe) == before
                probe.calls.append({'command': command, 'expected_permission_rejection': True, **result})
            with patch.object(state.module, 'createboss_jj') as generate:
                for command, text, expected in [
                    ('世界BOSS生成', '世界BOSS生成 extra', '无需参数'),
                    ('世界BOSS指定生成', '世界BOSS指定生成', '正确的指令'),
                    ('世界BOSS指定生成', '世界BOSS指定生成 练气境 name extra', '正确的指令'),
                    ('世界BOSS指定生成', '世界BOSS指定生成 invalid', 'world_boss_realm_invalid'),
                    ('世界BOSS指定生成', '世界BOSS指定生成 练气境 invalid', 'world_boss_name_invalid'),
                ]:
                    reply = await probe.command(command, text, actor='900003')
                    assert expected in reply and manual_assets(probe) == before, reply
                generate.assert_not_called()
            name = state.config['Boss名字'][0]
            reply = await probe.command('世界BOSS指定生成', '世界BOSS指定生成 练气境', actor='900003', number=983011)
            assert '已生成练气境Boss:' in reply, reply
            saved = manual_assets(probe)
            for text in (f'世界BOSS指定生成 练气境 {name}', '世界BOSS指定生成 筑基境'):
                reply = await probe.command('世界BOSS指定生成', text, actor='900003', number=983011)
                assert 'operation_conflict' in reply and '983011' in reply and manual_assets(probe) == saved, reply
            integrity(probe)
            probe.evidence.update(permission_zero_write=True, invalid_parameter_zero_write=True,
                                  changed_request_conflict=True, integrity='ok')

    @case('world-boss-manual-write-recovery', '世界BOSS生成', '两步拒写/原ABORT→重建应用同消息恢复/重放',
          '实际任务先生成，SQLite trigger IGNORE/副作用与ABORT，真实两种管理员handler',
          '失败整笔回滚并保留具体原因或安全定位，解除故障恢复一次，其他资产不变',
          group='world-boss-manual-spawn')
    async def manual_recovery(probe):
        from apscheduler.events import EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
        from tests.command_probe import run_case
        with setup(probe) as state:
            assert (await fire(state.module.generate_all_bosses_task)).code == EVENT_JOB_EXECUTED
            player = probe.context.paths.player_db
            before = manual_assets(probe)
            for command, text, number in [('世界BOSS生成', '世界BOSS生成', 983020),
                                          ('世界BOSS指定生成', '世界BOSS指定生成 练气境', 983021)]:
                entry = next(row for row in probe.entries if row['command'] == command)
                for target, table, event in [('state', 'world_boss_state', 'UPDATE'),
                                              ('receipt', 'world_boss_manual_spawn_operations', 'INSERT')]:
                    probe.seed(player, f'CREATE TRIGGER refuse_manual BEFORE {event} ON {table} BEGIN '
                               "INSERT INTO boss(user_id,boss_integral) VALUES('side-effect',7); SELECT RAISE(IGNORE); END")
                    reply = await probe.command(command, text, actor='900003', number=number)
                    assert f'world_boss_spawn_{target}_write_rejected' in reply and '主体：' in reply, reply
                    assert '已回滚' in reply and '已生成' not in reply and manual_assets(probe) == before, reply
                    probe.seed(player, 'DROP TRIGGER refuse_manual')
                probe.seed(player, 'CREATE TRIGGER refuse_manual BEFORE INSERT ON world_boss_manual_spawn_operations '
                           "BEGIN SELECT RAISE(ABORT,'isolated manual abort'); END")
                result = await run_case(entry, 'parameter', number, input_text=text, actor='900003')
                assert result['status'] == 'exception' and result['replies'] and not result['blocked'], result
                assert result['errors'] and all('isolated manual abort' in error for error in result['errors']), result
                assert '定位号' in ''.join(result['replies']) and manual_assets(probe) == before, result
                probe.calls.append({'command': command, 'expected_failure': 'isolated manual abort', **result})
                probe.seed(player, 'DROP TRIGGER refuse_manual')
                restarted = BossApplication(probe.context.paths.game_db, player, clock=state.clock)
                with patch.object(state.module, 'boss_application', restarted):
                    reply = await probe.command(command, text, actor='900003', number=number)
                    assert '已生成' in reply, reply
                    saved = manual_assets(probe)
                    assert saved[:5] == before[:5] and saved[6:11] == before[6:11] and saved[12:] == before[12:]
                    reply = await probe.command(command, text, actor='900003', number=number)
                    assert '此前已生成' in reply and manual_assets(probe) == saved, reply
                    assert state.module.group_boss['global'] == state.store.data['global'] == restarted.spawn_snapshot(config_loader=lambda: state.config)[0]
                before = saved
            assert probe.rows(player, 'SELECT COUNT(*) FROM world_boss_manual_spawn_operations') == [(2,)]
            integrity(probe)
            probe.evidence.update(rejected_steps=['state', 'receipt'], rollback_including_trigger=True,
                                  restart_recovery=True, duplicate_zero_write=True, integrity='ok')

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

    @case('world-boss-daily-reset-lifecycle', '重置世界BOSS', '上海零点重置→读取额度→同日重放→次日',
          '真实启动迁移三库与可控UTC时钟；管理员和普通玩家真实matcher',
          '日期跟随调度时区，三额度归零，永久积分与三库其他资产不变，同日不清零新额度',
          group='world-boss-daily-reset')
    async def daily_reset_lifecycle(probe):
        from apscheduler.events import EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_scheduler as scheduled
        from tests.command_probe import run_case
        with setup(probe) as state:
            player = probe.context.paths.player_db
            probe.player(level='练气境中期', exp=1000, hp=1000)
            state.app.daily_limit_snapshot('900001')
            state.module.boss_integral_application.get_integral('900001')
            probe.seed(player, "INSERT OR REPLACE INTO boss VALUES('900001',12,30,2)")
            state.clock.moment = datetime(2026, 11, 17, 15, 59, 59, 999999, tzinfo=timezone.utc)
            entry = next(row for row in probe.entries if row['command'] == '重置世界BOSS')
            before = assets(probe)
            denied = await run_case(entry, 'parameter', 982000, input_text='重置世界BOSS', actor='900001')
            assert denied['status'] == 'blocked' and denied['reason'] == 'permission/rule rejected', denied
            assert not denied['replies'] and not denied['errors'] and assets(probe) == before
            probe.calls.append({'command': '重置世界BOSS', 'expected_permission_rejection': True, **denied})
            reply = await probe.command('世界BOSS信息')
            assert '今日已获积分：12/' in reply and '今日讨伐次数：2/' in reply, reply
            assert (await fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED
            assert probe.rows(player, 'SELECT business_date FROM world_boss_daily_limit_reset_operations') == [('2026-11-17',)]
            probe.seed(player, "UPDATE boss SET boss_integral=7,boss_stone=9,boss_battle_count=1 WHERE user_id='900001'")
            state.clock.moment += timedelta(microseconds=1)
            assert (await fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED
            assert probe.rows(player, 'SELECT business_date FROM world_boss_daily_limit_reset_operations ORDER BY business_date') == [('2026-11-17',), ('2026-11-18',)]
            probe.clear_cooldowns()
            reply = await probe.command('世界BOSS信息')
            assert '今日已获积分：0/' in reply and '今日讨伐次数：0/' in reply, reply
            assert state.app.daily_limit_snapshot('900001').stone == 0
            after = assets(probe)
            assert after[:7] == before[:7] and after[8:] == before[8:]
            probe.seed(player, "UPDATE boss SET boss_integral=3,boss_stone=5,boss_battle_count=1 WHERE user_id='900001'")
            saved = assets(probe)
            reply = await probe.command('重置世界BOSS', '重置世界BOSS  ', actor='900003', number=982001)
            assert '已重置' in reply and assets(probe) == saved, reply
            reply = await probe.command('重置世界BOSS', '重置世界BOSS extra', actor='900003')
            assert '无需参数' in reply and assets(probe) == saved, reply
            state.clock.moment += timedelta(days=1)
            reply = await probe.command('重置世界BOSS', actor='900003', number=982002)
            assert '重置完成' in reply and state.app.daily_limit_snapshot('900001').stone == 0, reply
            integrity(probe)
            probe.evidence.update(permission_zero_write=True, local_midnight=True, new_daily_usage_preserved=True,
                                  permanent_integral_and_assets_unchanged=True, integrity='ok')

    @case('world-boss-daily-reset-recovery', '重置世界BOSS', '五步拒写/原ABORT→重建应用同消息恢复→任务重放',
          '隔离SQLite IGNORE/副作用；保留已完成分块和新产生额度',
          '具体原因及主体回复，失败分块回滚，恢复不重复清零已完成玩家', group='world-boss-daily-reset')
    async def daily_reset_recovery(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        from nonebot_plugin_xiuxian_2.features.boss.application import BossApplication
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_scheduler as scheduled
        from tests.command_probe import run_case
        with setup(probe) as state:
            player = probe.context.paths.player_db
            probe.seed(player, "INSERT OR REPLACE INTO boss VALUES('900001',12,30,2)")
            probe.seed(player, "INSERT OR REPLACE INTO boss VALUES('900002',8,20,1)")
            state.clock.moment = datetime(2026, 11, 17, 16, 1, 40, tzinfo=timezone.utc)
            day = '2026-11-18'
            tables = ('world_boss_daily_limit_reset_operations', 'world_boss_daily_limit_reset_targets')
            entry = next(row for row in probe.entries if row['command'] == '重置世界BOSS')
            async def rejected_command(reason):
                result = await run_case(entry, 'parameter', 982010, input_text='重置世界BOSS', actor='900003')
                assert result['status'] == 'exception' and result['replies'] and not result['blocked'], result
                assert result['errors'] and all(reason in error for error in result['errors']), result
                probe.calls.append({'command': '重置世界BOSS', 'expected_failure': reason, **result})
                return '\n'.join(result['replies'])
            for step, table, event in [('operation', tables[0], 'INSERT'), ('freeze', tables[1], 'INSERT')]:
                before = assets(probe)
                probe.seed(player, f'CREATE TRIGGER refuse_reset BEFORE {event} ON {table} BEGIN '
                           "UPDATE boss SET boss_stone=99; SELECT RAISE(IGNORE); END")
                reply = await rejected_command(f'world_boss_reset_{step}_write_rejected')
                assert f'world_boss_reset_{step}_write_rejected' in reply and f'主体：{day}' in reply, reply
                assert '重置完成' not in reply and assets(probe) == before
                assert probe.rows(player, f'SELECT COUNT(*) FROM {tables[0]}') == [(0,)]
                probe.seed(player, 'DROP TRIGGER refuse_reset')
            first = state.app.reset_daily_limit(day, chunk_size=1)
            assert first.completed == 1
            probe.seed(player, "UPDATE boss SET boss_stone=5 WHERE user_id='900001'")
            def snapshot():
                return assets(probe), tuple(probe.rows(player, f'SELECT * FROM {table} ORDER BY rowid') for table in tables)
            before = snapshot()
            for step, table in [('limit', 'boss'), ('target', tables[1]), ('progress', tables[0])]:
                probe.seed(player, f'CREATE TRIGGER refuse_reset BEFORE UPDATE ON {table} BEGIN '
                           "INSERT INTO boss VALUES('side-effect',7,9,2); SELECT RAISE(IGNORE); END")
                outcome = await fire(scheduled.daily_reset_boss_limits)
                assert outcome.code == EVENT_JOB_ERROR and f'world_boss_reset_{step}_write_rejected' in str(outcome.exception)
                reply = await rejected_command(f'world_boss_reset_{step}_write_rejected')
                assert f'world_boss_reset_{step}_write_rejected' in reply and '主体：' in reply, reply
                assert snapshot() == before
                probe.seed(player, 'DROP TRIGGER refuse_reset')
            probe.seed(player, f"CREATE TRIGGER refuse_reset BEFORE UPDATE ON {tables[1]} "
                       "BEGIN SELECT RAISE(ABORT,'isolated reset abort'); END")
            reply = await rejected_command('isolated reset abort')
            assert '定位号' in reply and snapshot() == before, reply
            probe.seed(player, 'DROP TRIGGER refuse_reset')
            restarted = BossApplication(probe.context.paths.game_db, player, clock=state.clock)
            with patch.object(state.module, 'boss_application', restarted):
                reply = await probe.command('重置世界BOSS', actor='900003', number=982010)
                assert '重置完成' in reply, reply
                assert restarted.daily_limit_snapshot('900001').stone == 5
                assert restarted.daily_limit_snapshot('900002').stone == 0
                saved = snapshot()
                reply = await probe.command('重置世界BOSS', actor='900003', number=982010)
                assert '已重置' in reply and snapshot() == saved, reply
                assert (await fire(scheduled.daily_reset_boss_limits)).code == EVENT_JOB_EXECUTED
                assert snapshot() == saved
            integrity(probe)
            probe.evidence.update(reasons=['operation', 'freeze', 'limit', 'target', 'progress'], failed_chunk_zero_writes=True,
                                  committed_chunk_preserved=True, retry_same_message=True, integrity='ok')

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
