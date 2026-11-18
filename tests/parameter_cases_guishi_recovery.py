"""Creation-to-matching recovery with real receipts and migrated databases."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_guishi import setup, state
from tests.parameter_cases_market import herb


def register(case):
    @case(
        "guishi-create-request-write-recovery",
        "鬼市求购",
        "求购/摆摊真实参数与即时撮合请求故障恢复",
        "隔离 SQLite 已迁移，账户/背包/物品充足，真实 matcher 注入请求 INSERT IGNORE/改写",
        "具体拒写原因与三库零写；解除故障后同消息创建一次，成功重放零写",
        group="market-guishi-create-recovery",
    )
    async def create_request_recovery(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        module, clock, application = setup(probe)
        item_id, name = herb(probe)
        price = module.MIN_PRICE * 10
        probe.player(stone=100_000_000)
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            await probe.command("鬼市存灵石", "鬼市存灵石 20000000", number=967030)

            def snapshot():
                return (
                    probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item_id,)),
                    probe.rows(probe.context.paths.trade_db, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'"),
                    probe.rows(probe.context.paths.trade_db, "SELECT COUNT(*) FROM guishi_item"),
                    probe.rows(probe.context.paths.trade_db, "SELECT COUNT(*) FROM guishi_order_create_operations"),
                    probe.rows(probe.context.paths.trade_db, "SELECT COUNT(*) FROM guishi_immediate_match_requests"),
                )

            async def adapter_contract(reply):
                event = SimpleNamespace(get_user_id=lambda: "900001")
                for onebot, markdown, send_fails in (
                    (True, True, False),
                    (False, False, False),
                    (False, True, False),
                    (False, True, True),
                ):
                    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
                    config = SimpleNamespace(
                        markdown_status=markdown, markdown_id="", markdown_id2="",
                        markdown_button_status=False, at_sender=False,
                        message_optimization=False, img=False,
                    )
                    plain = AsyncMock()
                    markdown_send = AsyncMock(
                        side_effect=RuntimeError("isolated guishi delivery failure") if send_fails else None
                    )
                    with patch.object(utils, "XiuConfig", return_value=config), \
                            patch.object(utils, "get_chat_scene", return_value="group"), \
                            patch.object(utils, "is_group_event", return_value=True), \
                            patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")), \
                            patch.object(utils, "_send_event_message", plain), \
                            patch.object(utils, "_send_markdown_or_keyboard", markdown_send):
                        await utils.handle_send(
                            bot, event, reply, md_type="交易", k1="求购", v1="鬼市求购",
                        )
                    if onebot or not markdown or send_fails:
                        plain.assert_awaited_once()
                        assert plain.call_args.kwargs["message"] == reply
                    else:
                        markdown_send.assert_awaited_once()
                        assert "guishi_immediate_match_write_rejected" in str(markdown_send.call_args.args[2])

            async def run_create(command, text, number):
                with DatabaseUnitOfWork(probe.context.paths.trade_db) as uow:
                    uow.execute(
                        "CREATE TRIGGER ignore_guishi_create_request BEFORE INSERT ON guishi_immediate_match_requests "
                        "BEGIN SELECT RAISE(IGNORE); END"
                    )
                before = snapshot()
                failed = await probe.command(command, text, number=number)
                assert "guishi_immediate_match_write_rejected" in failed and "撤销" in failed, failed
                assert snapshot() == before
                await adapter_contract(failed)
                with DatabaseUnitOfWork(probe.context.paths.trade_db) as uow:
                    uow.execute("DROP TRIGGER ignore_guishi_create_request")
                recovered = await probe.command(command, text, number=number)
                assert ("成功发布求购订单" in recovered or "成功摆摊" in recovered), recovered
                after = snapshot()
                replay = await probe.command(command, text, number=number)
                assert ("成功发布求购订单" in replay or "成功摆摊" in replay), replay
                assert snapshot() == after
                return failed, recovered, replay

            failed_buy, recovered_buy, replay_buy = await run_create(
                "鬼市求购", f"鬼市求购 {name} {price} 2", 967031,
            )
            failed_stall, recovered_stall, replay_stall = await run_create(
                "鬼市摆摊", f"鬼市摆摊 {name} {price} 2", 967032,
            )
            assert application.guishi_get_orders(user_id="900001", order_type="qiugou")
            assert application.guishi_get_orders(user_id="900001", order_type="baitan")
            for database in (probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db):
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    assert list(uow.query_one("PRAGMA integrity_check").values()) == ["ok"]
        probe.evidence.update(
            request_ignore_rollback=True,
            buy_reason="guishi_immediate_match_write_rejected",
            stall_reason="guishi_immediate_match_write_rejected",
            recovered_once=True,
            replay_zero_write=True,
            adapters="onebot/plain/qq-markdown/fallback",
        )

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
