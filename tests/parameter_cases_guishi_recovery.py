"""Creation-to-matching recovery with real receipts and migrated databases."""
from unittest.mock import patch

from tests.parameter_cases_guishi import setup, state
from tests.parameter_cases_market import herb


def register(case):
    @case("guishi-buy-creation-interruption-resume", "鬼市求购", "创建后异常/重建应用/同消息恢复/完成后重放",
          "先建真实摆摊，买单创建后撮合存储抛错", "买单及冻结已提交，异常回复有定位；重试只结算一次且实际统计正确",
          group="market-guishi-recovery")
    async def buy_resume(probe):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from tests.command_probe import run_case
        from tests.parameter_exception_watch import watch_handler_catches
        module, clock, application = setup(probe)
        item_id, name = herb(probe, "900002")
        price = module.MIN_PRICE * 10
        probe.player(stone=100_000_000)
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002")
            await probe.command("鬼市存灵石", "鬼市存灵石 20000000")
            before_balance = application.guishi_get_account("900001")[0]
            with patch.object(application.guishi_listing_fill_service, "match", side_effect=RuntimeError("isolated match fault")), \
                    patch.object(module, "report_exception", return_value="处理提示（定位号：recovery-test）") as reported:
                entry = next(row for row in probe.entries if row['command'] == '鬼市求购')
                with watch_handler_catches(entry) as caught:
                    result = await run_case(entry, "parameter", 967010,
                                            input_text=f"鬼市求购 {name} {price} 2", actor="900001")
                assert result['status'] == 'pass' and len(caught) == 1, (result, caught)
                assert 'RuntimeError: isolated match fault' in caught[0]['traceback'], caught
                probe.calls.append({"command": "鬼市求购", "expected_injected_fault": caught, **result})
                reply = "\n".join(result['replies'])
            assert "成功发布求购订单" in reply and "未完成" in reply and "定位号" in reply, reply
            assert reported.call_count == 1 and "isolated match fault" not in reply
            assert application.guishi_get_account("900001")[0] == before_balance - price * 2
            pending = application.guishi_pending_immediate_matches()
            assert len(pending) == 1 and pending[0]['order_type'] == 'qiugou', pending
            assert probe.managers[0].goods_num("900002", item_id) == 8
            restarted = TradeApplication(probe.context.paths.game_db, probe.context.paths.trade_db,
                                         clock=clock, player_database=probe.context.paths.player_db)
            with patch.object(module, "trade_application", restarted):
                reply = await probe.command("鬼市求购", f"鬼市求购 {name} {price} 2", number=967010)
                assert "求购成交" in reply and "未完成" not in reply, reply
                assert restarted.guishi_get_account("900001")[1] == {str(item_id): 2}
                assert restarted.guishi_get_account("900002")[0] == price * 2
                assert not restarted.guishi_get_orders() and not restarted.guishi_pending_immediate_matches()
                before = state(probe)
                await probe.command("鬼市求购", f"鬼市求购 {name} {price} 2", number=967010)
                assert before == state(probe)
            assert probe.rows(probe.context.paths.player_db,
                              'SELECT "鬼市购买次数","鬼市购买数量" FROM statistics WHERE user_id=\'900001\'') == [(1, 2)]
            for database in (probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db):
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    assert list(uow.query_one("PRAGMA integrity_check").values()) == ["ok"]
        probe.evidence.update(creation_committed=True, exception_reply=True, restarted_resume=True,
                              frozen=price * 2, received=2, duplicate_zero_write=True)

    @case("guishi-stall-partial-match-resume", "鬼市摆摊", "首对成交/次对冲突/重建应用/同消息恢复/重放",
          "两名买方真实求购，第二对永久回执冲突", "创建与首对已提交，回复具体冲突；清除隔离故障后只成交剩余一对且统计不重复",
          group="market-guishi-recovery")
    async def stall_resume(probe):
        from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
        module, clock, application = setup(probe)
        application = TradeApplication(probe.context.paths.game_db, probe.context.paths.trade_db,
                                       clock=clock, player_database=probe.context.paths.player_db)
        item_id, name = herb(probe, "900002")
        price = module.MIN_PRICE * 10
        probe.player(stone=100_000_000)
        probe.player("900003", stone=100_000_000)
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            for actor in ("900001", "900003"):
                await probe.command("鬼市存灵石", "鬼市存灵石 20000000", actor=actor)
                await probe.command("鬼市求购", f"鬼市求购 {name} {price} 1", actor=actor)
            buys = application.guishi_get_orders(order_type="qiugou")
            ordered = sorted(buys, key=lambda row: row["id"])
            seller = application.guishi_baitan_repository
            with patch.object(seller.order_ids, "new_id", return_value="987654321"):
                probe.seed(probe.context.paths.trade_db,
                           "INSERT INTO guishi_match_operations(operation_id,payload,result) VALUES(?,?,?)",
                           (f"guishi-fill:987654321:{ordered[1]['id']}", '["bad", "pair"]', "{}"))
                reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002", number=967001)
            assert "成功摆摊" in reply and "operation_conflict" in reply and ordered[1]["id"] in reply, reply
            assert "未完成" in reply and "x1" in reply, reply
            assert application.guishi_get_account("900002")[0] == price
            assert probe.managers[0].goods_num("900002", item_id) == 8
            pending = probe.rows(probe.context.paths.trade_db,
                                 "SELECT status,reason FROM guishi_immediate_match_requests WHERE order_id='987654321'")
            assert pending == [("pending", "operation_conflict")], pending
            assert probe.rows(probe.context.paths.player_db,
                              'SELECT "鬼市售出次数","鬼市售出数量" FROM statistics WHERE user_id=\'900002\'') == [(1, 1)]
            probe.seed(probe.context.paths.trade_db, "DELETE FROM guishi_match_operations WHERE operation_id=?",
                       (f"guishi-fill:987654321:{ordered[1]['id']}",))
            restarted = TradeApplication(probe.context.paths.game_db, probe.context.paths.trade_db,
                                         clock=clock, player_database=probe.context.paths.player_db)
            with patch.object(module, "trade_application", restarted):
                reply = await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002", number=967001)
                assert "求购成交" in reply and "未完成" not in reply, reply
                assert restarted.guishi_get_account("900002")[0] == price * 2
                assert not restarted.guishi_get_orders()
                before = state(probe)
                await probe.command("鬼市摆摊", f"鬼市摆摊 {name} {price} 2", actor="900002", number=967001)
                assert before == state(probe)
            assert probe.rows(probe.context.paths.player_db,
                              'SELECT "鬼市售出次数","鬼市售出数量" FROM statistics WHERE user_id=\'900002\'') == [(2, 2)]
        probe.evidence.update(first_pair_committed=True, conflict_preserved=True, restarted_resume=True,
                              seller_income=price * 2, seller_stock=8, match_statistics=2)
