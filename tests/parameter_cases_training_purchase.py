"""Live command contracts for training shop purchases."""
from __future__ import annotations

import json

from tests.parameter_cases_training_shop import (
    integrity_check,
    projection,
    seed_training,
)


def _today(module):
    return module.runtime_clock.now().date().isoformat()


def _seed(probe, module, *, points=5000, purchased=0):
    today = _today(module)
    weekly = {"_last_reset": today}
    if purchased:
        weekly["1999"] = purchased
    game = probe.context.paths.game_db
    # Keep repeated parameter cases independent: the fixture snapshot does
    # not reset mutations made inside one case.
    probe.seed(
        game,
        "DELETE FROM back WHERE user_id=? AND goods_id=?",
        ("900001", 1999),
    )
    probe.seed(
        game,
        "DELETE FROM training_purchase_operations WHERE operation_id LIKE ?",
        ("training-purchase:%:900001",),
    )
    probe.seed(
        game,
        "DELETE FROM operation_audit WHERE operation_id LIKE ?",
        ("training-purchase:%:900001",),
    )
    probe.seed(
        game,
        "DELETE FROM operation_ledger WHERE operation_id LIKE ? AND action=?",
        ("training-purchase:%:900001", "training.purchase"),
    )
    seed_training(
        probe,
        "900001",
        points=points,
        weekly_purchases=json.dumps(weekly, separators=(",", ":")),
    )
    return today, weekly


def _purchase_projection(probe):
    game = probe.context.paths.game_db
    player = probe.context.paths.player_db
    return {
        "training": probe.rows(
            player,
            "SELECT points,weekly_purchases FROM training WHERE user_id='900001'",
        ),
        "inventory": probe.rows(
            game,
            "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=1999",
        ),
        "receipts": probe.rows(
            game,
            "SELECT operation_id,payload,quantity,cost,points,purchased,inventory "
            "FROM training_purchase_operations ORDER BY operation_id",
        ),
        "ledger": probe.rows(
            game,
            "SELECT operation_id,action,status,result_json FROM operation_ledger "
            "WHERE operation_id LIKE 'training-purchase:%:900001' ORDER BY operation_id",
        ),
        "audit": probe.rows(
            game,
            "SELECT operation_id,action,status,category FROM operation_audit "
            "WHERE operation_id LIKE 'training-purchase:%:900001' ORDER BY audit_id",
        ),
    }


def _install_trigger(probe, sql, database="game_db"):
    game = getattr(probe.context.paths, database)
    probe.seed(game, sql)
    probe.evidence.setdefault("installed_triggers", []).append(
        probe.rows(game, "SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name")
    )


def _item(module):
    item = module.training_data.config["商店商品"]["1999"]
    definition = module._items().get_data_by_item_id("1999")
    assert definition and definition["name"] == "渡厄丹"
    return item, definition


async def _run_rejection_recovery(probe, module, trigger, number, expected_text):
    _seed(probe, module)
    trigger_database = "player_db" if "weekly" in trigger else "game_db"
    _install_trigger(probe, trigger, trigger_database)
    before = _purchase_projection(probe)
    before_all = projection(probe)
    failed_reply = await probe.command(
        "历练兑换", "历练兑换 1999", number=number,
        allow_exception=True,
    )
    failed_state = _purchase_projection(probe)
    probe.evidence.update(
        before=before,
        after_failure=failed_state,
        reply=failed_reply,
        full_projection_changed=before_all != projection(probe),
    )
    assert expected_text in failed_reply, failed_reply
    assert "成功兑换" not in failed_reply, failed_reply
    assert failed_state["training"] == before["training"], (before, failed_state)
    assert failed_state["inventory"] == before["inventory"], (before, failed_state)
    assert failed_state["receipts"] == before["receipts"], (before, failed_state)
    assert failed_state["ledger"] and failed_state["ledger"][0][2] in {"started", "needs_reconcile"}, failed_state

    _install_trigger(
        probe,
        f"DROP TRIGGER {trigger.split()[2]}",
        trigger_database,
    )
    recovered_reply = await probe.command(
        "历练兑换", "历练兑换 1999", number=number
    )
    recovered = _purchase_projection(probe)
    assert "成功兑换渡厄丹×1" in recovered_reply, recovered_reply
    assert recovered["training"][0][0] == 4000, recovered
    assert json.loads(recovered["training"][0][1])["1999"] == 1, recovered
    assert recovered["inventory"] == [(1, 1)], recovered
    assert len(recovered["receipts"]) == 1, recovered
    replay_reply = await probe.command(
        "历练兑换", "历练兑换 1999", number=number
    )
    assert any(word in replay_reply for word in ("已处理", "重复", "无需再次")), replay_reply
    assert _purchase_projection(probe) == recovered
    integrity_check(probe)


def register(case):
    @case(
        "training-purchase-receipt-ignore-recovery",
        "历练兑换",
        "合法商品默认数量",
        "隔离历练状态、实际商品定义，永久回执 INSERT RAISE(IGNORE)",
        "失败原因可见、业务状态整笔回滚、解除故障同消息恢复并幂等",
        group="training-purchase-parameters",
    )
    async def receipt_ignore(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        await _run_rejection_recovery(
            probe,
            module,
            "CREATE TRIGGER reject_training_purchase_receipt "
            "BEFORE INSERT ON training_purchase_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
            979_101,
            "回执",
        )

    @case(
        "training-purchase-inventory-ignore-recovery",
        "历练兑换",
        "合法商品数量",
        "隔离背包对 1999 商品 INSERT RAISE(IGNORE)",
            "物品入库拒写可见、积分/限购/背包/永久回执零副作用，恢复重试成功",
        group="training-purchase-parameters",
    )
    async def inventory_ignore(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        await _run_rejection_recovery(
            probe,
            module,
            "CREATE TRIGGER reject_training_purchase_inventory "
            "BEFORE INSERT ON back WHEN NEW.user_id='900001' AND NEW.goods_id=1999 "
            "BEGIN SELECT RAISE(IGNORE); END",
            979_102,
            "物品入库失败",
        )

    @case(
        "training-purchase-weekly-rewrite-recovery",
        "历练兑换",
        "合法商品默认数量",
        "隔离 training AFTER UPDATE 将 1999 周限购改写为 7",
        "周限购投影拒绝、整笔回滚、解除触发器后原消息恢复",
        group="training-purchase-parameters",
    )
    async def weekly_rewrite(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        today, _weekly = _seed(probe, module)
        weekly = json.dumps(
            {"_last_reset": today, "1999": 7}, separators=(",", ":")
        )
        trigger = (
            "CREATE TRIGGER rewrite_training_purchase_weekly "
            "AFTER UPDATE OF weekly_purchases ON training "
            "BEGIN UPDATE training SET weekly_purchases='" + weekly + "' "
            "WHERE user_id=NEW.user_id; END"
        )
        await _run_rejection_recovery(
            probe,
            module,
            trigger,
            979_103,
            "限购",
        )

    @case(
        "training-purchase-inventory-rewrite-recovery",
        "历练兑换",
        "合法商品默认数量",
        "隔离背包 AFTER INSERT 将库存改写为 6",
            "背包最终投影拒绝、整笔回滚且可同消息恢复",
        group="training-purchase-parameters",
    )
    async def inventory_rewrite(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        _seed(probe, module)
        trigger = (
            "CREATE TRIGGER rewrite_training_purchase_inventory "
            "AFTER INSERT ON back WHEN NEW.user_id='900001' AND NEW.goods_id=1999 "
            "BEGIN UPDATE back SET goods_num=goods_num+5 "
            "WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END"
        )
        await _run_rejection_recovery(
            probe,
            module,
            trigger,
            979_104,
            "背包结果与请求不一致",
        )

    @case(
        "training-purchase-arguments-and-default",
        "历练兑换",
        "缺省数量、非法数量与多余参数",
        "隔离积分充足、周限购未购，逐条真实 matcher 投递",
        "缺省数量买 1；0/负数/非数字/额外 token 均拒绝且零业务写",
        group="training-purchase-parameters",
    )
    async def arguments(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        _seed(probe, module)
        item, definition = _item(module)
        assert item["cost"] > 0 and item["weekly_limit"] > 1
        before = _purchase_projection(probe)
        success = await probe.command(
            "历练兑换", "历练兑换 1999", number=979_110
        )
        after = _purchase_projection(probe)
        assert "成功兑换渡厄丹×1" in success, success
        assert after["training"][0][0] == 5000 - item["cost"], after
        assert after["inventory"] == [(1, 1)], after
        assert len(after["receipts"]) == 1, after

        for offset, suffix in enumerate(("0", "-1", "abc", "1 extra"), 1):
            _seed(probe, module)
            invalid_before = _purchase_projection(probe)
            reply = await probe.command(
                "历练兑换",
                f"历练兑换 1999 {suffix}",
                number=979_110 + offset,
            )
            invalid_after = _purchase_projection(probe)
            probe.evidence[f"invalid_{suffix}"] = {
                "reply": reply,
                "before": invalid_before,
                "after": invalid_after,
            }
            assert "成功兑换" not in reply, (suffix, reply)
            assert invalid_after == invalid_before, (suffix, invalid_before, invalid_after)
        probe.evidence.update(
            item_name=definition["name"],
            default_quantity=1,
            default_before=before,
            default_after=after,
        )
        integrity_check(probe)

    @case(
        "training-purchase-same-message-replay",
        "历练兑换",
        "同消息重放与同号改参",
        "合法购买成功后以相同 message_id 原样重放，再使用同号改数量",
        "原请求识别为重复且全状态不变；同号改参返回冲突",
        group="training-purchase-parameters",
    )
    async def replay(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        _seed(probe, module)
        first = await probe.command(
            "历练兑换", "历练兑换 1999", number=979_120
        )
        after_first = _purchase_projection(probe)
        repeated = await probe.command(
            "历练兑换", "历练兑换 1999", number=979_120
        )
        after_repeat = _purchase_projection(probe)
        probe.evidence.update(
            first=first,
            repeated=repeated,
            after_first=after_first,
            after_repeat=after_repeat,
        )
        assert "成功兑换渡厄丹×1" in first, first
        assert any(word in repeated for word in ("已处理", "重复", "无需再次")), repeated
        assert after_repeat == after_first
        conflict = await probe.command(
            "历练兑换", "历练兑换 1999 2", number=979_120,
            allow_exception=True,
        )
        assert "该操作号已用于不同内容的请求" in conflict, conflict
        assert _purchase_projection(probe) == after_first
        integrity_check(probe)

    @case(
        "training-purchase-receipt-abort-rollback",
        "历练兑换",
        "合法商品购买，永久回执 ABORT",
        "隔离永久回执 INSERT RAISE(ABORT)",
        "handler 显示安全失败，积分/限购/库存/业务回执均完整回滚",
        group="training-purchase-parameters",
        outcome="expected_exception",
    )
    async def receipt_abort(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        _seed(probe, module)
        _install_trigger(
            probe,
            "CREATE TRIGGER abort_training_purchase_receipt "
            "BEFORE INSERT ON training_purchase_operations "
            "BEGIN SELECT RAISE(ABORT, 'isolated training receipt abort'); END",
        )
        before = _purchase_projection(probe)
        reply = await probe.command(
            "历练兑换",
            "历练兑换 1999",
            number=979_130,
            allow_exception=True,
        )
        after = _purchase_projection(probe)
        assert "成功兑换" not in reply, reply
        assert after["training"] == before["training"], (before, after)
        assert after["inventory"] == before["inventory"], (before, after)
        assert after["receipts"] == before["receipts"], (before, after)
        assert after["ledger"] and after["ledger"][0][2] == "failed", after
        probe.evidence.update(reply=reply, before=before, after=after)
        integrity_check(probe)
