"""Actual content names exercise inventory consumers and multi-step confirmation."""
from unittest.mock import patch

from tests.parameter_cases_assets import balances, backpack


def module():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back
    return xiuxian_back


def raise_level(probe):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.data_source import jsondata
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import OtherSet
    levels = jsondata.level_data()
    probe.player(level=max(levels, key=lambda name: int(OtherSet().set_closing_type(name))))
    if not probe.managers[0].get_user_buff_info("900001"):
        probe.managers[0].initialize_user_buff_info("900001")


def register(case):
    @case("bag-package-fixed-rewards", "使用", "实际固定礼包名 数量2", "5个真实感谢大礼包、奖励库存有空间",
          "消耗2个礼包并按内容增加每件奖励，事件重放不再消费", group="bag-consumers")
    async def fixed_package(probe):
        name = probe.grant_item(18050)
        definition = module().items.get_data_by_item_id(18050)
        text = await probe.command("使用", f"使用 {name} 2", number=970001)
        assert probe.managers[0].goods_num("900001", 18050) == 3, text
        index = 1
        while "buff_" + str(index) in definition:
            assert probe.managers[0].goods_num("900001", definition["buff_" + str(index)]) == 2 * definition["amount_" + str(index)], text
            index += 1
        saved, wallet = backpack(probe), balances(probe)
        await probe.command("使用", f"使用 {name} 2", number=970001)
        assert saved == backpack(probe) and wallet == balances(probe)
        probe.evidence.update(package_id=18050, consumed=2, replay_unchanged=True)

    @case("bag-package-random-replay", "使用", "真实随机礼包 数量1", "真实随机池至少两个不同奖励，足够礼包",
          "首次实际奖励落库，随机源改变后的事件重放仍回放冻结奖励", group="bag-consumers")
    async def random_package(probe):
        m = module()
        candidates = m.items.get_data_by_item_type("礼包")
        item_id, definition = next((int(key), value) for key, value in candidates.items()
                                   if value.get("roll") == 1 and len(value.get("roll_pool", [])) > 1
                                   and all(row.get("type") != "饰品" for row in value["roll_pool"]))
        name = probe.grant_item(item_id)
        with patch.object(m.random, "choice", side_effect=lambda pool: pool[0]):
            text = await probe.command("使用", f"使用 {name} 1", number=970002)
        assert probe.managers[0].goods_num("900001", item_id) == 4, text
        saved, wallet = backpack(probe), balances(probe)
        with patch.object(m.random, "choice", side_effect=lambda pool: pool[-1]):
            await probe.command("使用", f"使用 {name} 1", number=970002)
        assert saved == backpack(probe) and wallet == balances(probe)
        probe.evidence.update(package_id=item_id, random_changed=True, replay_unchanged=True)

    @case("bag-equip-unequip-name", "使用", "真实装备名 → 换装 真实装备名", "真实精铁符剑、足够境界",
          "装备状态和玩家法器字段实际变化，再卸载恢复", group="bag-consumers")
    async def equipment(probe):
        raise_level(probe)
        name = probe.grant_item(7001)
        text = await probe.command("使用", "使用 " + name)
        assert "成功装备" in text, text
        assert probe.rows(probe.context.paths.game_db, "SELECT state FROM back WHERE user_id='900001' AND goods_id=7001") == [(1,)]
        text = await probe.command("换装", "换装 " + name)
        assert "成功卸载" in text, text
        assert probe.rows(probe.context.paths.game_db, "SELECT state FROM back WHERE user_id='900001' AND goods_id=7001") == [(0,)]
        probe.evidence.update(equipment_id=7001, equipped_and_removed=True)

    @case("bag-skill-confirmation", "使用", "真实功法名 → 确认使用", "充足境界、未学习的真实功法",
          "确认前不消费，确认后学会目标功法且消耗1本", group="bag-consumers")
    async def skill(probe):
        raise_level(probe)
        m = module()
        item_id = int(next(iter(m.items.get_data_by_item_type("功法"))))
        name = probe.grant_item(item_id)
        with patch.object(m, "_ensure_confirm_use_cache_expiry_task"):
            text = await probe.command("使用", "使用 " + name)
        assert "确定要学习" in text and probe.managers[0].goods_num("900001", item_id) == 5, text
        text = await probe.command("确认使用")
        assert "成功学会" in text and probe.managers[0].goods_num("900001", item_id) == 4, text
        assert int(m.UserBuffDate("900001").BuffInfo["main_buff"]) == item_id
        probe.evidence.update(skill_id=item_id, confirmed=True)

    for prefix in ("", "/"):
        def build(prefix):
            async def run(probe):
                charm = probe.grant_item(20019)
                target = probe.grant_item(7001)
                probe.seed(probe.context.paths.game_db, "UPDATE back SET bind_num=3 WHERE user_id='900001' AND goods_id=7001")
                text = await probe.command("道具使用", f"{prefix}道具使用 {charm} 2 {target}", number=970003)
                assert probe.rows(probe.context.paths.game_db, "SELECT bind_num FROM back WHERE user_id='900001' AND goods_id=7001") == [(1,)], text
                assert probe.managers[0].goods_num("900001", 20019) == 3
                saved = backpack(probe)
                await probe.command("道具使用", f"{prefix}道具使用 {charm} 2 {target}", number=970003)
                assert saved == backpack(probe)
                probe.evidence.update(unbound=2, replay_unchanged=True, prefix=prefix)
            return run
        case("bag-unbind-" + ("slash" if prefix else "plain"), "道具使用", "解绑符 数量2 实际装备名",
             "3个绑定装备、5张解绑符", "只解除2个绑定并消耗2符，重放不重复", group="bag-consumers")(build(prefix))

    @case("alchemy-name-count-success", "炼金", "实际装备名 数量2", "5件未装备真实物品，合法回收价格",
          "实际消耗2件、钱包增加准确回收额，重放不重复", group="bag-consumers")
    async def alchemy(probe):
        name = probe.grant_item(7001)
        before = balances(probe)
        price = module().get_recover(7001, 2)
        text = await probe.command("炼金", f"炼金 {name} 2", number=970004)
        assert balances(probe)[0][1] - before[0][1] == price and probe.managers[0].goods_num("900001", 7001) == 3, text
        saved, wallet = backpack(probe), balances(probe)
        await probe.command("炼金", f"炼金 {name} 2", number=970004)
        assert saved == backpack(probe) and wallet == balances(probe)
        probe.evidence.update(consumed=2, revenue=price, replay_unchanged=True)

    for label, value in (("bad", "abc"), ("extra", "1 extra"), ("zero", "0"), ("negative", "-2"), ("huge", "9" * 350)):
        def build(value):
            async def run(probe):
                name = probe.grant_item(7001)
                before, stock = balances(probe), backpack(probe)
                text = await probe.command("炼金", f"炼金 {name} {value}")
                assert before == balances(probe) and stock == backpack(probe), text
                probe.evidence.update(wallet_unchanged=True, stock_unchanged=True)
            return run
        case("alchemy-reject-" + label, "炼金", "合法装备名 " + label, "真实可回收库存",
             "参数/库存拒绝，不回收也不增加钱包", group="bag-consumers", outcome="rejection")(build(value))
