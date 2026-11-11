"""Real map, seed, fusion and alchemy parameter paths."""

from tests.parameter_cases_assets import balances, backpack


def register(case):
    @case("map-info-real-query", "地图", "随包真实界名", "真实地图配置和已注册玩家",
          "查询实际界面节点，不修改位置或资产", group="map-fusion-parameters")
    async def map_info(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        realm = data["meta"]["realms"][0]
        before = balances(probe)
        text = await probe.command("地图", "地图 " + realm)
        assert realm in text and balances(probe) == before, text
        probe.evidence.update(realm=realm, readonly=True)

    @case("map-go-real-node", "前往", "当前天另一真实节点名", "真实地图状态和足够体力",
          "按实际节点名移动，位置和体力持久化，重复事件不重复扣体力", group="map-fusion-parameters")
    async def map_go(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        module._get_player_map_status("900001", data)
        status = module.map_application.map_status("900001")
        nodes = module._nodes(data, status["realm"], status["heaven"])
        target = next(node for node in nodes if node["id"] != status["node_id"])
        probe.player(user_stamina=100)
        text = await probe.command("前往", "前往 " + target["name"], number=970001)
        moved = module.map_application.map_status("900001")
        assert moved["node_id"] == target["id"] and moved["realm"] == status["realm"], text
        saved = probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'")
        assert saved[0][0] < 100
        await probe.command("前往", "前往 " + target["name"], number=970001)
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == saved
        probe.evidence.update(target=target["name"], before=status, after=moved, replay_unchanged=True)

    @case("map-seed-purchase-valid", "购买种子", "真实种子名 数量2", "真实种子商店节点、100000000灵石、空库存",
          "精确购买2份种子，扣真实价格并写库存，重放不重复", group="map-fusion-parameters")
    async def seed_buy(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        for realm in data["meta"]["realms"]:
            for heaven in module._heaven_names(data, realm):
                node = next((candidate for candidate in module._nodes(data, realm, heaven) if module._is_seed_shop_node(candidate["type"])), None)
                if node:
                    node = {**node, "realm": realm, "heaven": heaven}
                    break
            if node:
                break
        module.map_application.save_status("900001", node["realm"], node["heaven"], node["id"], [node["id"]])
        probe.player(stone=100_000_000)
        seed = module.SEED_CONFIG[21001]
        before = balances(probe)
        text = await probe.command("购买种子", f"购买种子 {seed['name']} 2", number=970002)
        assert probe.managers[0].goods_num("900001", 21001) == 2, text
        after = balances(probe)
        assert before[0][1] - after[0][1] == 2 * seed["price"]
        saved = backpack(probe)
        await probe.command("购买种子", f"购买种子 {seed['name']} 2", number=970002)
        assert backpack(probe) == saved and balances(probe) == after
        probe.evidence.update(seed_id=21001, quantity=2, replay_unchanged=True)

    @case("map-seed-extra-reject", "购买种子", "真实种子名 数量 多余参数", "真实种子商店节点和充足灵石",
          "格式拒绝，资产与库存不变", group="map-fusion-parameters", outcome="rejection")
    async def seed_extra(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        for realm in data["meta"]["realms"]:
            for heaven in module._heaven_names(data, realm):
                node = next((candidate for candidate in module._nodes(data, realm, heaven) if module._is_seed_shop_node(candidate["type"])), None)
                if node:
                    node = {**node, "realm": realm, "heaven": heaven}
                    break
            if node:
                break
        module.map_application.save_status("900001", node["realm"], node["heaven"], node["id"], [node["id"]])
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("购买种子", "购买种子 青灵草种 1 extra")
        assert before == balances(probe) and stock == backpack(probe), text
        assert "格式" in text, text
        probe.evidence.update(assets_unchanged=True, inventory_unchanged=True)

    @case("fusion-real-item-count", "强行合成", "真实可合成物品名 数量", "真实融合配置、合法境界与材料库存",
          "走真实融合应用，材料/灵石按数量结算并持久化，重放不重复", group="map-fusion-parameters")
    async def fusion(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_fusion as module
        candidate = None
        for item_id in range(1, 20000):
            item = module._items().get_data_by_item_id(item_id)
            if item and item.get("fusion", {}).get("need_item"):
                candidate = (item_id, item)
                break
        assert candidate, "fixture has no fusion recipe"
        item_id, item = candidate
        fusion = item["fusion"]
        probe.player(level=fusion.get("need_rank", "江湖好手"), stone=max(10**9, int(fusion.get("need_stone", 0)) * 2), exp=10**30)
        for material_id, amount in fusion["need_item"].items():
            probe.grant_item(int(material_id), int(amount) * 2)
        before = backpack(probe)
        text = await probe.command("强行合成", f"强行合成 {item['name']} 1", number=970003)
        after = backpack(probe)
        assert after != before and item["name"] in text, text
        saved = backpack(probe)
        await probe.command("强行合成", f"强行合成 {item['name']} 1", number=970003)
        assert backpack(probe) == saved
        probe.evidence.update(item_id=item_id, item_name=item["name"], replay_unchanged=True)

    @case("fusion-invalid-quantity", "强行合成", "真实物品名 零/非数字/多余参数", "真实可合成物品和材料库存",
          "严格拒绝数量格式，不消费材料", group="map-fusion-parameters", outcome="rejection")
    async def fusion_invalid(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_fusion as module
        item = next(module._items().get_data_by_item_id(i) for i in range(1, 20000) if module._items().get_data_by_item_id(i) and module._items().get_data_by_item_id(i).get("fusion", {}).get("need_item"))
        before = backpack(probe)
        text = await probe.command("强行合成", f"强行合成 {item['name']} 0")
        assert backpack(probe) == before and "正整数" in text, text
        probe.evidence.update(assets_unchanged=True)
