"""Focused semantic coverage for ordinary parameterized commands not in core groups."""

from tests.parameter_cases_assets import balances, backpack


def register(case):
    @case(
        "accessory-bag-page-real",
        "饰品背包",
        "页码1",
        "注册玩家和启动迁移",
        "按指定页读取饰品背包，不改变资产",
        group="remaining-player-parameters",
    )
    async def accessory_bag_page(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("饰品背包", "饰品背包 1")
        assert "饰品背包" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(page=1, readonly=True)

    @case(
        "accessory-collection-real-set",
        "饰品图鉴",
        "真实套装名",
        "随包饰品套装配置",
        "按真实套装筛选图鉴，不改变资产",
        group="remaining-player-parameters",
    )
    async def accessory_collection(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("饰品图鉴", "饰品图鉴 烈阳")
        assert "套装图鉴" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(set_name="烈阳", readonly=True)

    for command, query, marker in (
        ("查看饰品", "acc_missing_parameter", "未找到"),
        ("装备饰品", "acc_missing_parameter", "未找到"),
        ("饰品锁定", "acc_missing_parameter 1", "未找到"),
        ("饰品解锁", "acc_missing_parameter 1", "未找到"),
        ("饰品洗练", "acc_missing_parameter", "未找到"),
        ("饰品分解", "acc_missing_parameter", "未在"),
    ):
        def build(command, query, marker):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, command + " " + query)
                after_stock = backpack(probe)
                after = balances(probe)
                assert marker in text and after == before and after_stock == stock, text
                probe.evidence.update(
                    invalid_uid=query.split()[0],
                    assets_unchanged=True,
                )

            return run

        case(
            "accessory-invalid-" + command,
            command,
            "不存在饰品 UID",
            "真实饰品入口和隔离玩家",
            "具体 UID 拒绝且资产不变",
            group="remaining-player-parameters",
            outcome="rejection",
        )(build(command, query, marker))

    @case(
        "partner-protection-status",
        "双修保护",
        "状态",
        "注册玩家，关系服务已迁移",
        "读取并返回双修保护状态，不改变资产",
        group="remaining-player-parameters",
    )
    async def partner_protection_status(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("双修保护", "双修保护 状态")
        assert "双修保护状态" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(status_read=True, readonly=True)

    @case(
        "partner-protection-enable",
        "双修保护",
        "开启",
        "注册玩家，关系服务已迁移",
        "持久化开启双修保护并返回成功回执",
        group="remaining-player-parameters",
    )
    async def partner_protection_enable(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        text = await probe.command("双修保护", "双修保护 开启", number=970001)
        assert "开启" in text and partner.load_player_user("900001") == "on", text
        probe.evidence.update(status="on", persisted=True)

    @case(
        "dungeon-team-create-replay",
        "创建队伍",
        "自定义队伍名",
        "注册玩家、真实组队迁移和群聊上下文",
        "创建队伍写入真实队伍状态，重复事件只返回原结果",
        group="remaining-player-parameters",
    )
    async def dungeon_team_create(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        text = await probe.command("创建队伍", "创建队伍 参数队伍", number=970002)
        team_id = module.dungeon_team_application.team_id_for_user("900001")
        assert team_id and "队伍" in text, text
        snapshot = module.dungeon_team_application.snapshot(team_id)
        await probe.command("创建队伍", "创建队伍 参数队伍", number=970002)
        assert module.dungeon_team_application.snapshot(team_id) == snapshot
        probe.evidence.update(team_id=team_id, replay_unchanged=True)

    @case(
        "arena-shop-page-real",
        "竞技场商店",
        "页码1",
        "竞技场配置和注册玩家",
        "读取竞技场商店指定页，不改变资产",
        group="remaining-player-parameters",
    )
    async def arena_shop_page(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("竞技场商店", "竞技场商店 1")
        assert "竞技场商店" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(page=1, readonly=True)

    @case(
        "fusion-catalog-page-real",
        "查看可合成物品",
        "页码1",
        "随包合成配置",
        "读取真实可合成物品目录指定页，不改变资产",
        group="remaining-player-parameters",
    )
    async def fusion_catalog_page(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("查看可合成物品", "查看可合成物品 1")
        assert text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(page=1, readonly=True)

    @case(
        "dungeon-purchase-invalid-reject",
        "副本兑换",
        "不存在商品ID 数量1",
        "副本商店入口和注册玩家",
        "具体商品不存在拒绝且资产不变",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def dungeon_purchase_invalid(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("副本兑换", "副本兑换 999999 1")
        assert before == balances(probe) and stock == backpack(probe), text
        assert any(token in text for token in ("不存在", "商品", "无效", "兑换")), text
        probe.evidence.update(invalid_item_id=999999, assets_unchanged=True)
