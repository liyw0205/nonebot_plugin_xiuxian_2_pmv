"""Scheduled sect grants, recovery and player reads in migrated isolated databases."""
from contextlib import contextmanager
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
    module = sect_module()
    sid = seed_sect(probe)
    database = probe.context.paths.game_db
    probe.player(power=300)
    probe.player('900002', power=100)
    probe.seed(database, 'UPDATE sects SET sect_scale=120,sect_materials=50,combat_power=0 WHERE sect_id=?', (sid,))
    clock = FixedClock(datetime(2026, 11, 17, 15, 59, 59, 999999, tzinfo=timezone.utc))
    config = dict(module.config, **{'发放宗门资材': dict(module.config['发放宗门资材'], **{'倍率': 2})})
    with patch.object(module, 'runtime_clock', clock), patch.object(module, 'config', config):
        yield SimpleNamespace(module=module, clock=clock, sid=sid, database=database)


def receipts(probe):
    return probe.rows(probe.context.paths.game_db,
        'SELECT grant_key,sect_id,materials,combat_power FROM sect_scheduled_material_grants ORDER BY grant_key,sect_id')


def unrelated_assets(probe):
    game, trade, player = probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db
    return (probe.rows(game, 'SELECT * FROM user_xiuxian ORDER BY user_id'),
            probe.rows(game, 'SELECT * FROM user_cd ORDER BY user_id'),
            probe.rows(game, 'SELECT * FROM back ORDER BY user_id,goods_id'),
            probe.rows(game, 'SELECT * FROM domain_outbox ORDER BY event_id'),
            probe.rows(game, 'SELECT * FROM operation_ledger ORDER BY operation_id'),
            probe.rows(trade, 'SELECT * FROM guishi_info ORDER BY user_id'),
            probe.rows(trade, 'SELECT * FROM guishi_item ORDER BY id'),
            probe.rows(trade, 'SELECT * FROM auction_player_upload ORDER BY user_id,item_id'),
            probe.rows(player, 'SELECT * FROM boss ORDER BY user_id'),
            probe.rows(player, 'SELECT * FROM statistics ORDER BY user_id'))


async def read_materials(probe, state, materials, power=400, *, alias=False):
    before = (sect_rows(probe), receipts(probe), unrelated_assets(probe))
    reply = await probe.command('宗门信息' if alias else '我的宗门')
    rendered = reply.replace('\r\n', '\n').replace('\r', '\n')
    assert f'宗门拥有资材\n> {materials}' in rendered and f'宗门战力\n> {power}' in rendered, reply
    assert '参数测试宗' in reply
    assert (sect_rows(probe), receipts(probe), unrelated_assets(probe)) == before
    return reply


async def transport_contract(reply):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    event = SimpleNamespace(get_user_id=lambda: '900001')
    for onebot, markdown, send_fails in ((True, True, False), (False, False, False),
                                        (False, True, False), (False, True, True)):
        bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: 'OneBot V11' if onebot else 'QQ'))
        config = SimpleNamespace(markdown_status=markdown, markdown_id='', markdown_id2='', markdown_button_status=False,
                                 at_sender=False, message_optimization=False, img=False)
        plain = AsyncMock()
        md = AsyncMock(side_effect=RuntimeError('isolated material delivery failure') if send_fails else None)
        with patch.object(utils, 'XiuConfig', return_value=config), patch.object(utils, 'get_chat_scene', return_value='group'), \
                patch.object(utils, 'is_group_event', return_value=True), \
                patch.object(utils, 'get_real_id_async', new=AsyncMock(return_value='')), \
                patch.object(utils, '_send_event_message', plain), patch.object(utils, '_send_markdown_or_keyboard', md):
            await utils.handle_send(bot, event, reply, md_type='宗门', k1='加入', v1='宗门加入')
        if onebot or not markdown or send_fails:
            plain.assert_awaited_once()
            assert plain.call_args.kwargs['message'] == reply
        else:
            md.assert_awaited_once()
            assert '宗门拥有资材' in md.call_args.args[2] and '宗门战力' in md.call_args.args[2]
            assert '> 530' in md.call_args.args[2] and '> 400' in md.call_args.args[2]
        if onebot:
            md.assert_not_awaited()


def register(case):
    @case('sect-material-business-day-and-player-reads', '宗门成员查看', '业务跨日/同刻跨时区/默认与别名/成员名字页码',
          '真实迁移宗门与成员、可控UTC业务时钟、实际APScheduler',
          '资材按本地日期各发一次，战力真实汇总，玩家读取持久化结果，三库其他资产不变',
          group='sect-material-schedule')
    async def lifecycle(probe):
        with setup(probe) as state:
            prior = unrelated_assets(probe)
            assert (await fire(state.module.materialsupdate_)).code == EVENT_JOB_EXECUTED
            assert receipts(probe) == [('sect-materials:2026-11-17', state.sid, 240, 400)]
            await read_materials(probe, state, 290)
            state.clock.moment += timedelta(microseconds=1)
            assert (await fire(state.module.materialsupdate_)).code == EVENT_JOB_EXECUTED
            assert [row[0] for row in receipts(probe)] == ['sect-materials:2026-11-17', 'sect-materials:2026-11-18']
            reply = await read_materials(probe, state, 530, alias=True)
            await transport_contract(reply)
            probe.seed(state.database, 'UPDATE sects SET sect_materials=531 WHERE sect_id=?', (state.sid,))
            before = (sect_rows(probe), receipts(probe))
            state.clock.moment = state.clock.moment.astimezone(ZoneInfo('Asia/Shanghai'))
            assert (await fire(state.module.materialsupdate_)).code == EVENT_JOB_EXECUTED
            assert (sect_rows(probe), receipts(probe)) == before
            reply = await probe.command('宗门成员查看', '宗门成员查看 诊断对手 1')
            assert '诊断对手' in reply and '诊断道友' not in reply, reply
            reply = await probe.command('宗门成员查看', '宗门成员查看 不存在的弟子 1')
            assert '没有包含「不存在的弟子」的内容' in reply, reply
            assert (sect_rows(probe), receipts(probe)) == before
            state.clock.moment += timedelta(days=1)
            assert (await fire(state.module.materialsupdate_)).code == EVENT_JOB_EXECUTED
            await read_materials(probe, state, 771)
            assert receipts(probe)[-1] == ('sect-materials:2026-11-19', state.sid, 240, 400)
            assert unrelated_assets(probe) == prior
            integrity(probe)
            probe.evidence.update(local_midnight=True, equivalent_instants=True, new_materials_preserved=True,
                                  real_member_parameters=True, adapter_contracts=4, other_assets_unchanged=True, integrity='ok')

    @case('sect-material-partial-failure-and-restart', '宗门成员查看', '两步拒写/原ABORT/已提交保留/重建应用恢复/读取',
          '两个真实宗门，第二宗门IGNORE带副作用或ABORT，使用实际任务与玩家handler',
          '具体原因到任务ERROR，失败宗门全回滚，保留首宗门新资材，恢复只补剩余一次',
          group='sect-material-schedule')
    async def recovery(probe):
        from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
        with setup(probe) as state:
            module, database = state.module, state.database
            probe.player('900003', stone=10**12, power=200, sect_id=None, sect_position=None)
            second = module.sect_application.create_sect('second-fixture', '900003', '参数第二宗', 0, 0)
            assert second.applied, second
            probe.seed(database, 'UPDATE sects SET sect_scale=100,sect_materials=20,combat_power=0 WHERE sect_id=?', (second.sect_id,))
            state.clock.moment += timedelta(microseconds=1)
            prior = unrelated_assets(probe)
            for target, table, verb in (('state', 'sects', 'UPDATE'), ('receipt', 'sect_scheduled_material_grants', 'INSERT')):
                probe.seed(database, f'CREATE TRIGGER refuse_materials BEFORE {verb} ON {table} WHEN NEW.sect_id={second.sect_id} '
                           "BEGIN UPDATE user_xiuxian SET stone=0 WHERE user_id='900001'; SELECT RAISE(IGNORE); END")
                result = await fire(module.materialsupdate_)
                assert result.code == EVENT_JOB_ERROR
                assert f'sect_material_{target}_write_rejected' in str(result.exception)
                assert f'sect_id={second.sect_id}' in str(result.exception)
                assert receipts(probe) == [('sect-materials:2026-11-18', state.sid, 240, 400)]
                assert module.sect_application.get_sect_info(second.sect_id)['sect_materials'] == 20
                assert unrelated_assets(probe) == prior
                await read_materials(probe, state, 290)
                probe.seed(database, 'DROP TRIGGER refuse_materials')
            probe.seed(database, 'CREATE TRIGGER refuse_materials BEFORE INSERT ON sect_scheduled_material_grants '
                       f"WHEN NEW.sect_id={second.sect_id} BEGIN SELECT RAISE(ABORT,'isolated sect grant abort'); END")
            before = (sect_rows(probe), receipts(probe), unrelated_assets(probe))
            result = await fire(module.materialsupdate_)
            assert result.code == EVENT_JOB_ERROR and 'isolated sect grant abort' in str(result.exception)
            assert (sect_rows(probe), receipts(probe), unrelated_assets(probe)) == before
            probe.seed(database, 'DROP TRIGGER refuse_materials')
            probe.seed(database, 'UPDATE sects SET sect_materials=291 WHERE sect_id=?', (state.sid,))
            restarted = SectApplication(database)
            with patch.object(module, 'sect_application', restarted):
                assert (await fire(module.materialsupdate_)).code == EVENT_JOB_EXECUTED
                await read_materials(probe, state, 291, alias=True)
                reply = await probe.command('我的宗门', actor='900003')
                rendered = reply.replace('\r\n', '\n').replace('\r', '\n')
                assert '参数第二宗' in rendered and '宗门拥有资材\n> 220' in rendered and '宗门战力\n> 200' in rendered, reply
                assert receipts(probe) == [('sect-materials:2026-11-18', state.sid, 240, 400),
                                           ('sect-materials:2026-11-18', second.sect_id, 200, 200)]
                before = (sect_rows(probe), receipts(probe), unrelated_assets(probe))
                assert (await fire(module.materialsupdate_)).code == EVENT_JOB_EXECUTED
                await probe.command('宗门成员查看', '宗门成员查看 诊断对手 1')
                assert (sect_rows(probe), receipts(probe), unrelated_assets(probe)) == before
            assert unrelated_assets(probe) == prior
            integrity(probe)
            probe.evidence.update(rejected_steps=2, original_abort=True, committed_sect_preserved=True,
                                  restart_recovery=True, duplicate_zero_write=True, real_player_reads=True, integrity='ok')
