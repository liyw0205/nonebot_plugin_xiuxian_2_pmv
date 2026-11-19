"""Bounded real-dispatch coverage for Guishi amounts, orders and rollback."""
from unittest.mock import patch

from tests.parameter_cases_guishi import setup, state, statistics
from tests.parameter_cases_market import herb


def transfer_state(probe):
    return state(probe) + tuple(probe.rows(probe.context.paths.game_db, sql) for sql in (
        "SELECT user_id,stone FROM user_xiuxian ORDER BY user_id",
        "SELECT * FROM trade_guishi_deposit_operations ORDER BY operation_id",
        "SELECT * FROM trade_guishi_withdraw_operations ORDER BY operation_id",
    ))


def balances(probe):
    return (
        probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0],
        probe.rows(probe.context.paths.trade_db, "SELECT stored_stone FROM guishi_info WHERE user_id='900001'")[0][0],
    )


def register(case):
    for action, command in (("deposit", "鬼市存灵石"), ("withdraw", "鬼市取灵石")):
        @case(f"guishi-{action}-input-boundaries", command, "非法数值/超长数字/全角数字/空白",
              "周六固定时钟与真实钱包、账户", "非法输入明确拒绝零写，可解析数字兼容",
              group="market-guishi-boundaries")
        async def inputs(probe, command=command):
            module, clock, application = setup(probe)
            probe.player(stone=1000)
            probe.seed(probe.context.paths.trade_db, "INSERT INTO guishi_info VALUES('900001',800,'{}')")
            with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
                before = transfer_state(probe)
                for value in ("", "-1", "1.5", "1 2", "0", "\u00b2", "9" * 4301):
                    reply = await probe.command(command, f"{command} {value}")
                    assert "正确的灵石数量" in reply or "必须大于0" in reply, reply
                    assert transfer_state(probe) == before, value[:20]
                reply = await probe.command(command, f"{command}   \uff11\uff10\uff10   ")
                assert "成功" in reply, reply
                assert balances(probe) == ((900, 900) if command == "鬼市存灵石" else (1080, 700))
            probe.evidence.update(invalid_zero_write=7, unicode_decimal_compatible=True)

    @case("guishi-deposit-caps-replay", "鬼市存灵石", "钱包不足/单次与累计上限/重放/冲突",
          "实际余额与上限相邻", "精确扣钱包，拒绝和重复不写资产回执统计",
          group="market-guishi-boundaries")
    async def deposit(probe):
        module, clock, application = setup(probe)
        cap = 1_000_000_000
        probe.player(stone=cap - 1)
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            before = transfer_state(probe)
            for amount, expected in ((cap, "灵石不足"), (cap + 1, "单次存入不可超过")):
                reply = await probe.command("鬼市存灵石", f"鬼市存灵石 {amount}")
                assert expected in reply and before == transfer_state(probe), reply
            reply = await probe.command("鬼市存灵石", f"鬼市存灵石 {cap - 1}", number=968010)
            assert "成功存入" in reply and balances(probe) == (0, cap - 1), reply
            probe.player(stone=3)
            before = transfer_state(probe)
            reply = await probe.command("鬼市存灵石", "鬼市存灵石 2", number=968011)
            assert "本次最多还可存1" in reply and before == transfer_state(probe), reply
            await probe.command("鬼市存灵石", "鬼市存灵石 1", number=968012)
            assert balances(probe) == (2, cap)
            before = transfer_state(probe)
            for number, amount in ((968010, cap - 1), (968012, 1)):
                reply = await probe.command("鬼市存灵石", f"鬼市存灵石 {amount}", number=number)
                assert "成功存入" in reply and before == transfer_state(probe), reply
            reply = await probe.command("鬼市存灵石", "鬼市存灵石 2", number=968012)
            assert "该操作号已用于不同内容" in reply and before == transfer_state(probe), reply
            reply = await probe.command("鬼市存灵石", "鬼市存灵石 1", number=968013)
            assert "本次最多还可存0" in reply and before == transfer_state(probe), reply
            assert statistics(probe, "鬼市存入灵石") == [(cap,)]
        probe.evidence.update(wallet_exact=True, stored_cap=cap, replay_zero_write=True, conflict_zero_write=True)

    @case("guishi-withdraw-fee-caps-replay", "鬼市取灵石", "单次上限/余额不足/小额费率取整/满额取款/重放",
          "周六真实账户与钱包", "手续费及实际到账精确，回放不重复入账和统计",
          group="market-guishi-boundaries")
    async def withdraw(probe):
        module, clock, application = setup(probe)
        cap = 1_000_000_000
        probe.player(stone=0)
        probe.seed(probe.context.paths.trade_db, "INSERT INTO guishi_info VALUES('900001',10,'{}')")
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            before = transfer_state(probe)
            for amount, expected in ((11, "余额不足"), (cap + 1, "单次取出不可超过")):
                reply = await probe.command("鬼市取灵石", f"鬼市取灵石 {amount}")
                assert expected in reply and before == transfer_state(probe), reply
            for number, amount, wallet, stored, fee in ((968020, 4, 4, 6, 0), (968021, 5, 8, 1, 1), (968022, 1, 9, 0, 0)):
                reply = await probe.command("鬼市取灵石", f"鬼市取灵石 {amount}", number=number)
                assert f"扣除{fee}灵石" in reply and balances(probe) == (wallet, stored), reply
            probe.seed(probe.context.paths.trade_db, "UPDATE guishi_info SET stored_stone=?", (cap,))
            reply = await probe.command("鬼市取灵石", f"鬼市取灵石 {cap}", number=968023)
            assert "成功取出" in reply and balances(probe) == (800_000_009, 0), reply
            before = transfer_state(probe)
            for number, amount in ((968020, 4), (968023, cap)):
                reply = await probe.command("鬼市取灵石", f"鬼市取灵石 {amount}", number=number)
                assert "成功取出" in reply and before == transfer_state(probe), reply
            reply = await probe.command("鬼市取灵石", "鬼市取灵石 2", number=968023)
            assert "该操作号已用于不同内容" in reply and before == transfer_state(probe), reply
            assert statistics(probe, "鬼市取出灵石", "鬼市取灵石手续费") == [(800_000_009, 200_000_001)]
        probe.evidence.update(floor_fees=[0, 1, 0], cap_fee=200_000_000, replay_zero_write=True)

    @case("guishi-transfer-write-failure-retry", "鬼市存灵石", "账户/回执被忽略/解除故障/同消息恢复/重放",
          "两种转账各注入两种真实SQLite故障", "具体回复，三库零写；恢复一次提交，重复零写",
          group="market-guishi-boundaries")
    async def failures(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        module, clock, application = setup(probe)
        probe.player(stone=1000)
        probe.seed(probe.context.paths.trade_db, "INSERT INTO guishi_info VALUES('900001',800,'{}')")
        with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
            for index, (action, command, target) in enumerate((
                ("deposit", "鬼市存灵石", "balance"), ("withdraw", "鬼市取灵石", "balance"),
                ("deposit", "鬼市存灵石", "receipt"), ("withdraw", "鬼市取灵石", "receipt"),
            )):
                database = probe.context.paths.trade_db if target == "balance" else probe.context.paths.game_db
                table = "guishi_info" if target == "balance" else f"trade_guishi_{action}_operations"
                event = "UPDATE" if target == "balance" else "INSERT"
                probe.seed(database, f"CREATE TRIGGER reject_transfer BEFORE {event} ON {table} "
                           "BEGIN SELECT RAISE(IGNORE); END")
                before = transfer_state(probe)
                reply = await probe.command(command, f"{command} 100", number=968030 + index)
                expected = "鬼市余额保存失败" if target == "balance" else "鬼市存取回执保存失败"
                assert expected in reply and "本次存取已撤销" in reply and "本人账户" in reply, reply
                assert before == transfer_state(probe), reply
                probe.seed(database, "DROP TRIGGER reject_transfer")
                reply = await probe.command(command, f"{command} 100", number=968030 + index)
                assert "成功" in reply, reply
                after = transfer_state(probe)
                await probe.command(command, f"{command} 100", number=968030 + index)
                assert after == transfer_state(probe)
            assert balances(probe) == (960, 800)
            assert statistics(probe, "鬼市存入灵石", "鬼市取出灵石", "鬼市取灵石手续费") == [(200, 160, 40)]
            for database in (probe.context.paths.game_db, probe.context.paths.trade_db, probe.context.paths.player_db):
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    assert uow.query_all("PRAGMA integrity_check") == [{"integrity_check": "ok"}]
        probe.evidence.update(rollback_cases=4, recovered_once=4, integrity="ok", precise_subject=True)

    for kind, command in (("qiugou", "鬼市求购"), ("baitan", "鬼市摆摊")):
        @case(f"guishi-{kind}-price-quantity-limit", command, "最低价格/默认数量/零负与超量截断/10单上限",
              "真实存款与足额库存，无对手单", "原有经济规则保持，拒绝零写，重放不重复资产统计",
              group="market-guishi-boundaries")
        async def orders(probe, kind=kind, command=command):
            module, clock, application = setup(probe)
            item_id, name = herb(probe)
            price = module.MIN_PRICE * 10
            probe.player(stone=1_000_000_000)
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=100 WHERE user_id='900001' AND goods_id=?", (item_id,))
            with patch.object(module, "runtime_clock", clock), patch.object(module, "trade_application", application):
                await probe.command("鬼市存灵石", "鬼市存灵石 1000000000")
                before = transfer_state(probe)
                for suffix, expected in ((f"{price - 1}", "价格过低"), ("1.5", "有效的价格"),
                                         (f"{price} nope", "有效的价格"), (f"{price} 1 extra", "指令格式")):
                    reply = await probe.command(command, f"{command} {name} {suffix}")
                    assert expected in reply and before == transfer_state(probe), reply
                quantities = []
                for index, (raw, quantity) in enumerate((("", 1), ("0", 1), ("-3", 1), ("11", 10), ("10", 10))):
                    reply = await probe.command(command, f"{command} {name} {price} {raw}", number=968050 + index)
                    assert f"数量：{quantity}" in reply and "成功" in reply, reply
                    quantities.append(quantity)
                for index in range(5):
                    await probe.command(command, f"{command} {name} {price + 1} 1", number=968060 + index)
                    quantities.append(1)
                orders = application.guishi_get_orders(user_id="900001", order_type=kind)
                assert len(orders) == 10 and sum(row["quantity"] for row in orders) == sum(quantities), orders
                before = transfer_state(probe)
                reply = await probe.command(command, f"{command} {name} {price} 1", number=968070)
                assert "已达上限(10)" in reply and before == transfer_state(probe), reply
                reply = await probe.command(command, f"{command} {name} {price} 11", number=968053)
                assert "数量：10" in reply and before == transfer_state(probe), reply
                total = sum(quantities)
                if kind == "qiugou":
                    assert application.guishi_get_account("900001")[0] == 1_000_000_000 - total * price - 5
                    assert statistics(probe, "鬼市求购次数", "鬼市求购数量") == [(10, total)]
                    probe.player(uid="900002", stone=price - 1)
                    await probe.command("鬼市存灵石", f"鬼市存灵石 {price - 1}", actor="900002")
                    before = transfer_state(probe)
                    reply = await probe.command(command, f"{command} {name} {price}", actor="900002")
                    assert "余额不足" in reply and before == transfer_state(probe), reply
                else:
                    assert probe.managers[0].goods_num("900001", item_id) == 100 - total
                    assert statistics(probe, "鬼市摆摊次数", "鬼市摆摊数量") == [(10, total)]
            probe.evidence.update(default_quantity=1, clamped_quantities=quantities, max_orders=10,
                                  minimum_price=price, rejection_and_replay_zero_write=True)
