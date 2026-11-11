"""Explicit asset parameter branches; registration is supplied by the runner."""
from unittest.mock import patch


def balances(probe):
    return probe.rows(probe.context.paths.game_db,
                      "SELECT user_id,stone,exp FROM user_xiuxian ORDER BY user_id")


def backpack(probe):
    return probe.rows(probe.context.paths.game_db, "SELECT * FROM back ORDER BY user_id,goods_id")


def register(case):
    for label, target in (("uid", "900002"), ("mention", "[CQ:at,qq=900002]")):
        def build(target):
            async def run(probe):
                before = balances(probe)
                text = await probe.command("送灵石", f"送灵石 {target} 1000")
                after = balances(probe)
                assert before[0][1] - after[0][1] == 1000, (text, before, after)
                assert after[1][1] - before[1][1] == 900, (text, before, after)
                receipt = probe.rows(probe.context.paths.game_db, "SELECT sender_id,recipient_id,gross_amount,net_amount FROM stone_gift_operations")
                assert receipt == [("900001", "900002", 1000, 900)], receipt
                probe.evidence.update(before=before, after=after, receipt=receipt)
            return run
        case("gift-target-" + label, "送灵石", label + " 数量", "真实注册收发双方和足够灵石",
             "按目标解析完成真实资产转移并写赠送回执", group="asset-parameters")(build(target))

    for label, args, expected in (
        ("self", "900001 100", "自己"), ("zero", "900002 0", "大于0"),
        ("negative", "900002 -2", "正确"), ("decimal", "900002 1.5", "正确"),
        ("extra", "900002 100 extra", "正确"), ("missing", "900002", "正确"),
        ("unknown", "不存在的诊断对象 100", "未踏入"),
        ("huge", "900002 " + "9" * 350, "额度"),
    ):
        def build(args, expected):
            async def run(probe):
                before = balances(probe)
                text = await probe.command("送灵石", "送灵石 " + args)
                assert expected in text, text
                assert before == balances(probe), (before, balances(probe))
                assert not probe.rows(probe.context.paths.game_db, "SELECT * FROM stone_gift_operations")
                probe.evidence.update(assets_unchanged=True, expected_refusal=expected)
            return run
        case("gift-reject-" + label, "送灵石", label, "双方注册且有灵石；特定输入触发指定拒绝",
             "准确参数或业务拒绝，双方资产与赠送回执不变", group="asset-parameters", outcome="rejection")(build(args, expected))

    @case("bag-special-valid-count", "道具使用", "实际物品名 正整数数量", "背包有5个真实灵石福袋",
          "消耗2个福袋并增加对应灵石，重复事件不再次消费", group="bag-parameters")
    async def special_success(probe):
        name = probe.grant_item(20020)
        before = balances(probe)
        text = await probe.command("道具使用", f"道具使用 {name} 2", number=920001)
        assert probe.managers[0].goods_num("900001", 20020) == 3, text
        after = balances(probe)
        assert after[0][1] > before[0][1], (text, before, after)
        await probe.command("道具使用", f"道具使用 {name} 2", number=920001)
        assert balances(probe) == after and probe.managers[0].goods_num("900001", 20020) == 3
        probe.evidence.update(item_id=20020, before=before, after=after, remaining=3, replay_unchanged=True)

    for command in ("使用", "道具使用"):
        for label, suffix in (("bad-count", "abc"), ("zero", "0"), ("negative", "-2"),
                              ("extra", "2 extra"), ("over-limit", "101"), ("huge", "9" * 350)):
            def build(command, suffix):
                async def run(probe):
                    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back as module
                    item_id = 20020 if command == "道具使用" else int(next(iter(module.items.get_data_by_item_type("礼包"))))
                    name = probe.grant_item(item_id)
                    actual_suffix = "1001" if command == "使用" and suffix == "101" else suffix
                    before, stock = balances(probe), backpack(probe)
                    with patch.object(module.PackageRewardResolver, "resolve_once", side_effect=AssertionError("invalid quantity reached reward generation")):
                        text = await probe.command(command, f"{command} {name} {actual_suffix}")
                    assert balances(probe) == before and backpack(probe) == stock, (text, before, balances(probe))
                    assert any(token in text for token in ("数量", "格式", "1-100")), text
                    probe.evidence.update(item_id=item_id, assets_unchanged=True, inventory_unchanged=True)
                return run
            case("bag-" + command + "-reject-" + label, command, "物品名 " + label,
                 "有库存真实特殊道具，避免缺库存遮挡参数分支", "参数拒绝，不消费道具或变更灵石",
                 group="bag-parameters", outcome="rejection")(build(command, suffix))

    @case("bag-package-huge-before-resolver", "使用", "真实礼包 超大数量", "合法礼包但仅5份库存；奖励解析器禁止进入",
          "数量或库存拒绝发生在奖励生成循环之前", group="bag-parameters", outcome="rejection")
    async def package_huge(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back as module
        definitions = module.items.get_data_by_item_type("礼包")
        item_id = int(next(iter(definitions)))
        name = probe.grant_item(item_id)
        stock, before = backpack(probe), balances(probe)
        with patch.object(module.PackageRewardResolver, "resolve_once", side_effect=AssertionError("invalid quantity reached reward generation")):
            text = await probe.command("使用", f"使用 {name} " + "9" * 350)
        assert stock == backpack(probe) and before == balances(probe), text
        probe.evidence.update(item_id=item_id, reward_loop_not_entered=True, assets_unchanged=True)
