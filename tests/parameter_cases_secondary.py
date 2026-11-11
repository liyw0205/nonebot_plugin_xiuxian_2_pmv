"""Secondary registered commands with real content arguments."""

from tests.parameter_cases_assets import balances, backpack


def register(case):
    @case("inventory-item-name-read", "查看修仙界物品", "随包真实物品名", "真实物品目录",
          "按实际名称返回物品详情且资产不变", group="secondary-parameters")
    async def item_read(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_back as module
        item_id = next(iter(module.items.get_data_by_item_type("丹药")))
        name = module.items.get_data_by_item_id(item_id)["name"]
        before = balances(probe)
        text = await probe.command("查看修仙界物品", "查看修仙界物品 丹药 " + name)
        assert name in text and balances(probe) == before, text
        probe.evidence.update(item_id=item_id, item_name=name, readonly=True)

    for command, query in (("我的背包", "1"), ("丹药背包", "1"), ("药材背包详细", "1")):
        def build(command, query):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, command + " " + query)
                assert text and balances(probe) == before and backpack(probe) == stock, text
                probe.evidence.update(page=1, readonly=True)
            return run
        case("inventory-read-" + command, command, "页码1", "真实注册玩家和背包迁移",
             "指定页返回且不改变资产", group="secondary-parameters")(build(command, query))

    @case("title-info-real-name", "称号详情", "随包真实称号名", "称号配置实际存在",
          "返回实际称号详情和获取条件，不写玩家状态", group="secondary-parameters")
    async def title_info(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
        title = next(iter(module.get_all_titles().values()))
        name = title["name"] if isinstance(title, dict) else title.name
        text = await probe.command("称号详情", "称号详情 " + name)
        assert name in text and "获取条件" in text, text
        probe.evidence.update(title_name=name, readonly=True)

    @case("title-equip-unknown-reject", "装备称号", "不存在称号名", "真实称号注册入口",
          "具体称号不存在拒绝且玩家状态不变", group="secondary-parameters", outcome="rejection")
    async def title_invalid(probe):
        before = balances(probe)
        text = await probe.command("装备称号", "装备称号 不存在的参数称号")
        assert "不存在" in text and balances(probe) == before, text
        probe.evidence.update(assets_unchanged=True, invalid_title=True)

    @case("arena-quantity-format-reject", "竞技场兑换", "商品编号 多余参数", "竞技场入口注册且无需访问真实网络",
          "严格拒绝多余参数，不扣荣誉或库存", group="secondary-parameters", outcome="rejection")
    async def arena_invalid(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("竞技场兑换", "竞技场兑换 1 2 extra")
        assert "格式" in text and before == balances(probe) and stock == backpack(probe), text
        probe.evidence.update(assets_unchanged=True, format_rejected=True)

    @case("boss-store-unknown-reject", "世界BOSS兑换", "不存在商品编号", "世界BOSS商店入口注册",
          "具体商品不存在拒绝，不扣积分或发物品", group="secondary-parameters", outcome="rejection")
    async def boss_invalid(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("世界BOSS兑换", "世界BOSS兑换 999999 1")
        assert before == balances(probe) and stock == backpack(probe), text
        assert any(token in text for token in ("没有", "商品", "编号", "不存在")), text
        probe.evidence.update(assets_unchanged=True, invalid_item=True)

    for command, query, marker in (
        ("赛季榜", "赛季榜 周榜 战力", "赛季榜"),
        ("我的赛季", "我的赛季 月榜", "我的赛季"),
    ):
        def build(command, query, marker):
            async def run(probe):
                before = balances(probe)
                text = await probe.command(command, query)
                assert marker in text and balances(probe) == before, text
                probe.evidence.update(query=query, readonly=True)
            return run
        case("season-read-" + command, command, "真实周期/榜单参数", "已注册玩家与真实赛季读取器",
             "读取实际赛季视图且不改变资产", group="secondary-parameters")(build(command, query, marker))

    @case("title-list-page-real", "我的称号", "页码1", "称号数据已加载、注册玩家",
          "按真实页码读取称号状态且不写玩家资产", group="secondary-parameters")
    async def title_list_page(probe):
        before = balances(probe)
        text = await probe.command("我的称号", "我的称号 1")
        assert "称号" in text and balances(probe) == before, text
        probe.evidence.update(page=1, readonly=True)

    @case("achievement-filter-real", "我的成就", "真实筛选词未完成", "称号条件配置已加载、注册玩家",
          "筛选实际成就记录且不改变玩家状态", group="secondary-parameters")
    async def achievement_filter(probe):
        before = balances(probe)
        text = await probe.command("我的成就", "我的成就 未完成")
        assert "成就" in text and balances(probe) == before, text
        probe.evidence.update(filter="未完成", readonly=True)

    @case("impart-card-image-real-name", "传承卡图", "随包真实卡名", "传承卡图目录已加载",
          "按真实卡名返回图片或降级属性，资产不变", group="secondary-parameters")
    async def impart_card_image(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart as module
        card_name = next(iter(module.impart_application.card_definitions()))
        before = balances(probe)
        text = await probe.command("传承卡图", "传承卡图 " + card_name)
        assert card_name in text or "传承卡图" in text, text
        assert balances(probe) == before
        probe.evidence.update(card_name=card_name, readonly=True, external_image="mock transport")

    @case("impart-card-image-invalid-reject", "传承卡图", "不存在卡名", "传承卡图入口已注册",
          "具体卡名不存在拒绝，不改变资产", group="secondary-parameters", outcome="rejection")
    async def impart_card_invalid(probe):
        before = balances(probe)
        text = await probe.command("传承卡图", "传承卡图 不存在的传承卡")
        assert "没有找到" in text and balances(probe) == before, text
        probe.evidence.update(assets_unchanged=True, invalid_card=True)
