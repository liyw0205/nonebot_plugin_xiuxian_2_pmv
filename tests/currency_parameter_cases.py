"""Finite money paths through real startup, dispatch, parsing and repositories."""
from __future__ import annotations


def register(case):
    group = "currency-precision"

    @case("currency-xianshi-bilateral", "仙肆购买", "真实挂单ID 数量1", "合法药材挂单，双方精确超int64余额", "双边精确扣增、发货、operation和重放", group=group)
    async def xianshi(probe):
        from tests.parameter_cases_market import listing
        from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param, as_int_like
        item_id, name, offer = await listing(probe)
        value = 10**30 + 123
        probe.player(stone=bind_sqlite_param(value, exact=True))
        probe.player("900002", stone=bind_sqlite_param(value + 2, exact=True))
        db = probe.context.paths.game_db
        text = await probe.command("仙肆购买", f"仙肆购买 {offer['id']} 1", number=991001)
        sql = "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id"
        after = [(uid, as_int_like(stone)) for uid, stone in probe.rows(db, sql)]
        assert after == [("900001", value - offer["price"]), ("900002", value + 2 + offer["price"])], (text, after)
        assert probe.managers[0].goods_num("900001", item_id) == 1, text
        assert probe.rows(db, "SELECT quantity,total_cost FROM xianshi_operations") == [(1, offer["price"])], text
        await probe.command("仙肆购买", f"仙肆购买 {offer['id']} 1", number=991001)
        assert after == [(uid, as_int_like(stone)) for uid, stone in probe.rows(db, sql)]
        probe.evidence.update(item_id=item_id, listing_id=offer["id"], before=value, after=after, operation=True, replay_unchanged=True)

    @case("currency-gift-bilateral", "送灵石", "合法道号 金额100", "双方int64临界余额，合法赠送资格", "双边精确余额、每日限额、永久回执与重放", group=group)
    async def gift(probe):
        from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
        value = 2**63 - 1
        probe.player(stone=value)
        probe.player("900002", stone=value)
        text = await probe.command("送灵石", "送灵石 诊断对手 100", number=991002)
        db = probe.context.paths.game_db
        sql = "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id"
        after = [(uid, as_int_like(stone)) for uid, stone in probe.rows(db, sql)]
        assert after == [("900001", value - 100), ("900002", value + 90)], (text, after)
        assert probe.rows(db, "SELECT gross_amount,net_amount,fee_amount FROM stone_gift_operations") == [(100, 90, 10)]
        await probe.command("送灵石", "送灵石 诊断对手 100", number=991002)
        assert after == [(uid, as_int_like(stone)) for uid, stone in probe.rows(db, sql)]
        probe.evidence.update(before=value, after=after, operation=True, replay_unchanged=True)

    @case("currency-bank-deposit-withdraw", "灵庄", "存灵石1000 -> 取灵石400", "合法余额10**30+123，无银行账户", "钱包/存款精确重读、成功回执、存款重放不扣两次", group=group)
    async def bank(probe):
        from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
        value = 10**30 + 123
        probe.player(stone=bind_sqlite_param(value, exact=True))
        db = probe.context.paths.game_db
        sql = "SELECT stone FROM user_xiuxian WHERE user_id='900001'"
        text = await probe.command("灵庄", "灵庄 存灵石 1000", number=991003)
        assert as_int_like(probe.rows(db, sql)[0][0]) == value - 1000, text
        assert probe.rows(db, "SELECT saved_stone FROM bank_accounts") == [(1000,)], text
        await probe.command("灵庄", "灵庄 存灵石 1000", number=991003)
        assert as_int_like(probe.rows(db, sql)[0][0]) == value - 1000
        text = await probe.command("灵庄", "灵庄 取灵石 400", number=991004)
        assert as_int_like(probe.rows(db, sql)[0][0]) == value - 600, text
        assert probe.rows(db, "SELECT saved_stone FROM bank_accounts") == [(600,)], text
        assert len(probe.rows(db, "SELECT operation_id FROM bank_account_operations")) == 2
        probe.evidence.update(before=value, wallet=value - 600, savings=600, operation_count=2, deposit_replayed=True)

    @case("currency-activity-shop", "活动购买", "随包真实商品名 数量2", "活动启用，精确超int64积分及钱包", "精确扣积分与灵石奖励、库存/回执和重放", group=group)
    async def activity(probe):
        from tests.parameter_cases_activity import active_config, points
        from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
        value = 10**30 + 123
        with active_config() as (cfg, service):
            definition, item = points(probe, cfg)
            db = probe.context.paths.game_db
            probe.seed(db, "UPDATE activity_point_balance SET points=? WHERE activity_key=?", (bind_sqlite_param(value, exact=True), definition["key"]))
            probe.player(stone=bind_sqlite_param(value, exact=True))
            text = await probe.command("活动购买", f"活动购买 {item['name']} 2", number=991005)
            sql = "SELECT points FROM activity_point_balance WHERE activity_key=? AND user_id='900001'"
            after = as_int_like(probe.rows(db, sql, (definition["key"],))[0][0])
            reward = service.parse_reward(item["reward"])[0]["quantity"] * 2
            assert after == value - item["cost"] * 2, (text, after)
            assert as_int_like(probe.rows(db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]) == value + reward
            await probe.command("活动购买", f"活动购买 {item['name']} 2", number=991005)
            assert as_int_like(probe.rows(db, sql, (definition["key"],))[0][0]) == after
        probe.evidence.update(activity=definition["key"], item=item["item_key"], before=value, points=after, reward=reward, replay_unchanged=True)

    @case("currency-activity-exchange", "活动兑换", "随包真实词组名", "真实字牌满足词组需求，钱包10**30+123",
          "扣字牌、精确灵石奖励、兑换operation与重放", group=group)
    async def exchange(probe):
        from collections import Counter
        from tests.parameter_cases_activity import active_config
        from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
        value = 10**30 + 123
        probe.player(stone=bind_sqlite_param(value, exact=True))
        db = probe.context.paths.game_db
        with active_config() as (cfg, service):
            definition = next(row for row in cfg["gameplay_activities"] if row["type"] == "collect_words")
            phrase = definition["phrases"][0]
            needs = Counter(phrase["phrase"])
            for char, count in needs.items():
                probe.seed(db, "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) VALUES(?,'900001',?,?)",
                           (definition["key"], char, count))
            reward = sum(row["quantity"] for row in service.parse_reward(phrase["reward"]) if row["type"] == "stone")
            assert reward > 0
            text = await probe.command("活动兑换", "活动兑换 " + phrase["name"], number=991006)
            assert "成功" in text, text
            assert as_int_like(probe.rows(db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]) == value + reward
            assert all(row[0] == 0 for row in probe.rows(db, "SELECT count FROM activity_collect_inventory WHERE user_id='900001'"))
            receipts = probe.rows(db, "SELECT * FROM activity_collect_exchange_operations")
            assert len(receipts) == 1
            await probe.command("活动兑换", "活动兑换 " + phrase["name"], number=991006)
            assert receipts == probe.rows(db, "SELECT * FROM activity_collect_exchange_operations")
            assert as_int_like(probe.rows(db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]) == value + reward
        probe.evidence.update(phrase=phrase["phrase"], consumed=dict(needs), exact_reward=reward,
                              wallet=value + reward, operation=True, replay_unchanged=True)

    @case("currency-activity-task-reward", "活动任务领取", "随包真实日任务名", "合法完成进度，钱包10**30+123",
          "指定奖励精确入账、claimed/永久operation、回复与重放", group=group)
    async def task_reward(probe):
        from tests.parameter_cases_activity import active_config
        from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
        value = 10**30 + 123
        probe.player(stone=bind_sqlite_param(value, exact=True))
        db = probe.context.paths.game_db
        with active_config() as (cfg, service):
            task = cfg["daily_tasks"][0]
            key, scope = service._activity_config_key(cfg), service._task_scope_key("daily")
            probe.seed(db, "INSERT INTO activity_task_progress(activity_key,user_id,scope_type,scope_key,task_key,progress,target) VALUES(?,'900001','daily',?,?,?,?)",
                       (key, scope, task["key"], task["target"], task["target"]))
            reward = sum(row["quantity"] for row in service.parse_reward(task["reward"]) if row["type"] == "stone")
            assert reward > 0
            text = await probe.command("活动任务领取", "活动任务领取 " + task["name"], number=991007)
            assert "领取成功" in text, text
            assert as_int_like(probe.rows(db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]) == value + reward
            assert probe.rows(db, "SELECT claimed FROM activity_task_progress WHERE user_id='900001'") == [(1,)]
            receipts = probe.rows(db, "SELECT result_status,status FROM activity_task_reward_claim_operations")
            assert receipts == [("applied", "applied")], receipts
            await probe.command("活动任务领取", "活动任务领取 " + task["name"], number=991007)
            assert receipts == probe.rows(db, "SELECT result_status,status FROM activity_task_reward_claim_operations")
            assert as_int_like(probe.rows(db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]) == value + reward
        probe.evidence.update(task=task["key"], claimed=True, exact_reward=reward,
                              wallet=value + reward, operation=True, replay_unchanged=True)

    for kind in ("xianshi-buyer", "xianshi-seller", "bank-wallet", "activity-points", "gift-recipient"):
        def build(kind):
            async def rejected(probe):
                import sqlite3
                from contextlib import ExitStack
                with ExitStack() as stack:
                    db = probe.context.paths.game_db
                    command, raw = None, None
                    if kind.startswith("xianshi"):
                        from tests.parameter_cases_market import listing
                        _, _, offer = await listing(probe)
                        probe.player("900001" if kind.endswith("buyer") else "900002", stone=float(2**63))
                        command, raw = "仙肆购买", f"仙肆购买 {offer['id']} 1"
                    elif kind == "bank-wallet":
                        probe.player(stone=float(2**63))
                        command, raw = "灵庄", "灵庄 存灵石 1000"
                    elif kind == "gift-recipient":
                        probe.player(stone=1000)
                        probe.player("900002", stone=float(2**63))
                        command, raw = "送灵石", "送灵石 诊断对手 100"
                    else:
                        from tests.parameter_cases_activity import active_config, points
                        cfg, _ = stack.enter_context(active_config())
                        definition, item = points(probe, cfg)
                        probe.seed(db, "UPDATE activity_point_balance SET points=?", (float(2**63),))
                        command, raw = "活动购买", f"活动购买 {item['name']} 1"
                    with sqlite3.connect(db) as conn:
                        before = list(conn.iterdump())
                    text = await probe.command(command, raw, number=991010)
                    assert "REAL" in text and "精确整数" in text, text
                    with sqlite3.connect(db) as conn:
                        after = list(conn.iterdump())
                        import difflib
                        assert before == after, (text, list(difflib.unified_diff(before, after)))
                    probe.evidence.update(precision_rejected=True, database_unchanged=True, receipt_unchanged=True, reason_reply=text)
            return rejected
        case("currency-refuse-" + kind, {"bank-wallet": "灵庄", "activity-points": "活动购买", "gift-recipient": "送灵石"}.get(kind, "仙肆购买"),
             "真实目标/商品 正整数数量", "仅指定货币置为不可恢复REAL，其他前置满足",
             "具体货币精度拒绝、业务和operation/ledger零写", group=group, outcome="rejection")(build(kind))

    @case("currency-admin-large", "神秘力量", "超int64精确金额 实际目标道号", "虚构管理员权限，目标超大余额", "精确入账、账本/operation、重放和payload冲突", group=group)
    async def admin(probe):
        from nonebot_plugin_xiuxian_2.core.numeric import as_int_like, bind_sqlite_param
        value = 10**30 + 123
        probe.player("900002", stone=bind_sqlite_param(value, exact=True))
        text = await probe.command("神秘力量", f"神秘力量 {value} 诊断对手", actor="900003", number=991011)
        db = probe.context.paths.game_db
        sql = "SELECT stone FROM user_xiuxian WHERE user_id='900002'"
        assert as_int_like(probe.rows(db, sql)[0][0]) == value * 2, text
        assert as_int_like(probe.rows(db, "SELECT stone_delta FROM economy_log WHERE trace_id IN (SELECT operation_id FROM admin_stone_adjustment_operations)")[0][0]) == value
        await probe.command("神秘力量", f"神秘力量 {value} 诊断对手", actor="900003", number=991011)
        assert as_int_like(probe.rows(db, sql)[0][0]) == value * 2
        text = await probe.command("神秘力量", f"神秘力量 {value + 1} 诊断对手", actor="900003", number=991011)
        assert "不同内容的请求" in text, text
        assert as_int_like(probe.rows(db, sql)[0][0]) == value * 2
        probe.evidence.update(after=value * 2, exact_ledger=True, replay_unchanged=True, conflict_unchanged=True)


if __name__ == "__main__":
    from tests import parameter_probe

    register(parameter_probe.case)
    raise SystemExit(parameter_probe.main())
