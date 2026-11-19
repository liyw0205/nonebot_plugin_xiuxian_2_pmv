"""Real command recovery across cutoff, failed cleanup, and subsequent convergence."""
import asyncio
from datetime import datetime, timezone
import sqlite3
from unittest.mock import patch

from tests.parameter_cases_guishi import setup, state
from tests.parameter_cases_market import herb


def register(case):
    @case("guishi-pending-cutoff-cleanup-resume", "鬼市摆摊", "07:59创建后中断/08:00拒绝撮合/清理失败/恢复/重放",
          "真实求购与摆摊，创建后故障，截止后重试，清理回执trigger故障",
          "过期不新成交，失败清理整单回滚；恢复退物退款，pending收敛且无重复结算",
          group="market-guishi-cutoff-recovery")
    async def cutoff(probe):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches

        module, clock, application = setup(probe)
        item_id, name = herb(probe, "900002")
        price = module.MIN_PRICE * 10
        probe.player(stone=100_000_000)
        scheduler = AsyncIOScheduler(timezone=application.business_timezone)

        async def scheduled(callback):
            outcome = asyncio.get_running_loop().create_future()
            listener = lambda event: outcome.set_result(event) if not outcome.done() else None
            scheduler.add_listener(listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
            try:
                scheduler.add_job(callback, "date")
                return await asyncio.wait_for(outcome, timeout=3)
            finally:
                scheduler.remove_listener(listener)

        try:
            with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
                await probe.command("鬼市存灵石", "鬼市存灵石 20000000")
                await probe.command("鬼市求购", f"鬼市求购 {name} {price} 2")
                clock.moment = datetime(2026, 11, 21, 23, 59, tzinfo=timezone.utc)
                entry = next(row for row in probe.entries if row['command'] == '鬼市摆摊')
                with patch.object(application.guishi_listing_fill_service, "match", side_effect=RuntimeError("cutoff fixture fault")), \
                        patch.object(module, "report_exception", return_value="定位号：cutoff-test"), \
                        watch_handler_catches(entry) as caught:
                    result = await run_case(entry, "parameter", 967020,
                                            input_text=f"鬼市摆摊 {name} {price} 2", actor="900002")
                assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
                assert 'RuntimeError: cutoff fixture fault' in caught[0]['traceback']
                assert '未完成' in ''.join(result['replies'])
                probe.calls.append({"command": "鬼市摆摊", "expected_injected_fault": caught, **result})
                assert probe.managers[0].goods_num("900002", item_id) == 8
                clock.moment = datetime(2026, 11, 22, 0, tzinfo=timezone.utc)
                reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002", number=967020)
                assert 'baitan_expired' in reply and '未完成' in reply and '求购成交' not in reply, reply
                assert application.guishi_get_account("900002")[0] == 0
                database = probe.context.paths.trade_db
                probe.seed(database, "CREATE TRIGGER fail_expiry BEFORE INSERT ON guishi_order_cancel_operations "
                           "BEGIN SELECT RAISE(ABORT,'cutoff cancellation fault'); END")
                before = state(probe)
                scheduler.start()
                outcome = await scheduled(module.clear_expired_baitan_orders_job)
                assert outcome.code == EVENT_JOB_ERROR and isinstance(outcome.exception, sqlite3.IntegrityError)
                assert str(outcome.exception) == 'cutoff cancellation fault' and outcome.traceback
                assert before == state(probe), 'failed cancellation did not roll back'
                probe.seed(database, "DROP TRIGGER fail_expiry")
                assert (await scheduled(module.clear_expired_baitan_orders_job)).code == EVENT_JOB_EXECUTED
                assert application.guishi_get_account("900001")[0] == 20_000_000
                assert probe.managers[0].goods_num("900002", item_id) == 10
                assert not application.guishi_get_orders()
                assert (await scheduled(module.auto_guishi_transactions_job)).code == EVENT_JOB_EXECUTED
                assert not application.guishi_pending_immediate_matches()
                before = state(probe)
                await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002", number=967020)
                assert (await scheduled(module.auto_guishi_transactions_job)).code == EVENT_JOB_EXECUTED
                assert (await scheduled(module.clear_expired_baitan_orders_job)).code == EVENT_JOB_EXECUTED
                assert before == state(probe)
                assert probe.rows(database, "SELECT COUNT(*) FROM guishi_match_operations") == [(0,)]
                assert probe.rows(database, "SELECT COUNT(*) FROM guishi_order_cancel_operations") == [(1,)]
                assert probe.rows(database, "SELECT refunded_quantity FROM guishi_expired_order_operations") == [(2,)]
                assert probe.rows(probe.context.paths.player_db,
                                  'SELECT COALESCE("鬼市购买次数",0),COALESCE("鬼市售出次数",0) '
                                  "FROM statistics WHERE user_id IN ('900001','900002') ORDER BY user_id") == [(0, 0), (0, 0)]
                for path in (probe.context.paths.game_db, database, probe.context.paths.player_db):
                    with DatabaseUnitOfWork(path, read_only=True) as uow:
                        assert list(uow.query_one("PRAGMA integrity_check").values()) == ['ok']
                probe.evidence.update(expiry_reason='baitan_expired', failed_cleanup_rollback=True,
                                      actual_scheduler_recovered=True, returned_stock=10, restored_balance=20_000_000,
                                      pending_complete=True, duplicate_zero_write=True)
        finally:
            if scheduler.running:
                scheduler.shutdown(wait=False)
